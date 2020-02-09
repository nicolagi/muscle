package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// Valid prefix byte in the propagation log lines. A pending item is only in the
// fast store, that needs to copied to the slow store. A done item is in the slow
// store and may or may not be in the fast store (might have been evicted). A
// missing item is one that was to be propagated from fast to slow store, but
// was not found in the fast store.
const (
	itemPending = 'p'
	itemMissing = 'm'
	itemDone    = 'd'
)

// The log consists of lines of known length (a byte, a key, a newline).
const logLineLength = 66

type propagationLog struct {
	pollInterval time.Duration
	readOffset   int64

	mu   sync.Mutex
	file *os.File
}

// newLog reads the log at pathname (creating it if necessary), compacts it, and time stamps the previous version.
func newLog(pathname string) (*propagationLog, error) {
	curr, err := os.OpenFile(pathname, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open %q read-only: %w", pathname, err)
	}
	next, err := os.OpenFile(pathname+".new", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("open %q write-only: %w", pathname+".new", err)
	}
	s := bufio.NewScanner(curr)
	for s.Scan() {
		line := s.Text()
		switch state := line[0]; state {
		case itemPending, itemMissing:
			if _, err := fmt.Fprintln(next, line); err != nil {
				return nil, fmt.Errorf("copying line from %q to %q: %w", curr.Name(), next.Name(), err)
			}
		case itemDone:
		default:
			log.Errorf("Skipping unrecognized item state: %d", state)
		}
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("scan %q: %w", curr.Name(), err)
	}
	if err := curr.Close(); err != nil {
		return nil, fmt.Errorf("close %q: %w", curr.Name(), err)
	}
	if err := next.Close(); err != nil {
		return nil, fmt.Errorf("close %q: %w", next.Name(), err)
	}
	if err := os.Rename(curr.Name(), fmt.Sprintf("%s.%d", curr.Name(), time.Now().Unix())); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.Rename(next.Name(), curr.Name()); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	curr, err = os.OpenFile(pathname, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open %q read-write: %w", pathname, err)
	}
	// Seek to end for writes. (Reads will use ReadAt instead.)
	if _, err := curr.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek %q to EOF: %w", curr.Name(), err)
	}
	return &propagationLog{file: curr, pollInterval: 5 * time.Second}, nil
}

func (pl *propagationLog) add(key Key) error {
	pl.mu.Lock()
	n, err := fmt.Fprintf(pl.file, "%c%s\n", itemPending, key)
	pl.mu.Unlock()
	if n != logLineLength {
		return fmt.Errorf("written only %d of %d bytes", n, logLineLength)
	}
	return err
}

func (pl *propagationLog) next(p []byte) {
	for {
		pl.mu.Lock()
		n, err := pl.file.ReadAt(p, pl.readOffset)
		pl.mu.Unlock()
		if n == logLineLength && err == nil {
			break
		}
		time.Sleep(pl.pollInterval)
	}
}

func (pl *propagationLog) mark(state byte) error {
	pl.mu.Lock()
	n, err := pl.file.WriteAt([]byte{state}, pl.readOffset)
	pl.mu.Unlock()
	pl.readOffset += logLineLength // Advance to next line.
	if n != 1 {
		return fmt.Errorf("wrote %d bytes instead of 1", n)
	}
	return err
}

func (pl *propagationLog) skip() {
	pl.readOffset += logLineLength
}

func (pl *propagationLog) close() {
	pl.mu.Lock()
	_ = pl.file.Close()
	pl.file = nil // panic if somebody tries to use the log after this.
	pl.mu.Unlock()
}

// Paired is a store implementation that is meant to provide the benefits of a fast local store and long term
// persistence and accessibility of cloud storage. Paired writes to the fast store and queues async writes to the slow
// store. It reads from the fast store if possible. If not, reads from the slow store and copies content to the fast
// store for next time. It deletes from the slow store first and then from the fast store.
type Paired struct {
	retryInterval time.Duration

	fast Store
	slow Store

	// To start the background goroutine from Put operations.
	once sync.Once

	log *propagationLog
}

// NewPaired creates a write-back cache from fast to slow.
// If the log path is empty, the cache is read-only and puts will fail.
func NewPaired(fast, slow Store, logPath string) (p *Paired, err error) {
	p = new(Paired)
	p.retryInterval = 5 * time.Second
	p.fast = fast
	p.slow = slow
	if logPath != "" {
		p.log, err = newLog(logPath)
		if err != nil {
			return
		}
	}
	return p, err
}

func (p *Paired) Get(k Key) (v Value, err error) {
	v, err = p.fast.Get(k)
	if errors.Is(err, ErrNotFound) {
		v, err = p.slow.Get(k)
		if err == nil {
			if e := p.fast.Put(k, v); e != nil {
				log.WithFields(log.Fields{
					"key":   k,
					"cause": e.Error(),
				}).Warning("Could not write item to the fast store")
			}
		}
	}
	return
}

var ErrReadOnly = errors.New("read-only store")

// Put writes an item to the fast store and enqueues it to be written
// to the slow store asynchronously. Might block if the async write
// queue is full. Since this operation is used by the code creating a
// new revision, triggered by "echo snapshot > ctl", this in turn might
// block the fileserver entirely for the duration of the snapshot. This
// hasn't happened to me in practice.  Could probably use the underlying
// filesystem as queue (e.g., hard or symbolic links) but that leads to
// assuming a disk store implementation and I don't want to.
func (p *Paired) Put(k Key, v Value) error {
	if p.log == nil {
		return ErrReadOnly
	}
	p.EnsureBackgroundPuts()
	if err := p.fast.Put(k, v); err != nil {
		return err
	}
	if err := p.log.add(k); err != nil {
		return err
	}
	return nil
}

func (p *Paired) EnsureBackgroundPuts() {
	p.once.Do(func() {
		if p.log != nil {
			go p.propagate()
		}
	})
}

func (p *Paired) propagate() {
	line := make([]byte, logLineLength)
	for {
		p.log.next(line)
		if state := line[0]; state != itemPending && state != itemMissing {
			log.Warnf("skipping item with unexpected state: %d", state)
			p.log.skip()
			continue
		}
		key := Key(line[1:65])
		value, err := p.fast.Get(key)
		if err != nil {
			p.log.mark(itemMissing)
			continue
		}
		for {
			if err = p.slow.Put(key, value); err == nil {
				break
			}
			log.Warnf("failure to put %q to slow store (will retry): %v", key, err)
			time.Sleep(p.retryInterval)
		}
		p.log.mark(itemDone)
	}
}

// Delete deletes an item from the slow store first, then from the fast store second. Note that if done in the other
// order, a concurrent Get could replenish the fast store from the slow store after the deletion, e.g., (1) delete from
// fast, (2) get from slow, (3) replenish fast, (4) delete from slow. Steps (1) and (4) belong to this method while (2)
// and (3) belong to Get.
func (p *Paired) Delete(k Key) error {
	if err := p.slow.Delete(k); err != nil {
		return err
	}
	return p.fast.Delete(k)
}
