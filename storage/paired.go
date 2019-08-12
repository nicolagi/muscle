package storage

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const defaultPropagationBacklogSize = 4096

type propagationLog struct {
	mu sync.Mutex
	f  *os.File
}

func readPropagationLog(pathname string) (keys []Key, err error) {
	var f *os.File
	f, err = os.OpenFile(pathname, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("could not open read-only %q: %v", pathname, err)
	}
	m := make(map[string]struct{})
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		switch fields[1] {
		case "todo":
			m[fields[2]] = struct{}{}
		case "done":
			delete(m, fields[2])
		default:
			return nil, fmt.Errorf("unknown verb %q in propagation log", fields[1])
		}
	}
	err = f.Close()
	if err != nil {
		return nil, fmt.Errorf("could not close %q: %v", pathname, err)
	}
	for key := range m {
		keys = append(keys, Key(key))
	}
	return
}

func archivePropagationLog(pathname string) error {
	wrapErr := func(err error) error {
		return fmt.Errorf("when archiving %q: %v", pathname, err)
	}
	f, err := os.Open(pathname)
	if err != nil {
		return wrapErr(err)
	}
	g, err := os.Create(fmt.Sprintf("%s.%d", pathname, time.Now().Unix()))
	if err != nil {
		return wrapErr(err)
	}
	z := gzip.NewWriter(g)
	_, err = io.Copy(z, f)
	if err != nil {
		return wrapErr(err)
	}
	err = f.Close()
	if err != nil {
		return wrapErr(err)
	}
	err = z.Close()
	if err != nil {
		return wrapErr(err)
	}
	err = g.Close()
	if err != nil {
		return wrapErr(err)
	}
	return nil
}

func writePropagationLog(pathname string, keys []Key) error {
	f, err := ioutil.TempFile(filepath.Dir(pathname), "propagation.*.log")
	if err != nil {
		return fmt.Errorf("could not create temporary file to compact propagation log: %v", err)
	}
	ts := time.Now().Format(time.RFC3339)
	for _, k := range keys {
		if _, err = fmt.Fprintf(f, "%s todo %s\n", ts, k); err != nil {
			return fmt.Errorf("could not write to do item to temporary file %q: %v", f.Name(), err)
		}
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("could not close temporary file %q: %v", f.Name(), err)
	}
	if err = os.Rename(f.Name(), pathname); err != nil {
		return fmt.Errorf("could not rename %q to %q: %v", f.Name(), pathname, err)
	}
	return nil
}

func newPropagationLog(pathname string) (*propagationLog, []Key, error) {
	keys, err := readPropagationLog(pathname)
	if err != nil {
		return nil, nil, err
	}
	log.WithField("count", len(keys)).Info("Found keys to propagate in the propagation log")
	fi, err := os.Stat(pathname)
	if err != nil {
		return nil, nil, err
	}
	if fi.Size() > 1024*1024 {
		if err = archivePropagationLog(pathname); err != nil {
			return nil, nil, err
		}
		if err = writePropagationLog(pathname, keys); err != nil {
			return nil, nil, err
		}
	}
	f, err := os.OpenFile(pathname, os.O_WRONLY|os.O_APPEND, 0644)
	return &propagationLog{f: f}, keys, err
}

func (l *propagationLog) todo(key Key) error {
	l.mu.Lock()
	_, err := fmt.Fprintf(l.f, "%s todo %s\n", time.Now().Format(time.RFC3339), key)
	l.mu.Unlock()
	return err
}

func (l *propagationLog) done(k Key) {
	l.mu.Lock()
	_, err := fmt.Fprintf(l.f, "%s done %s\n", time.Now().Format(time.RFC3339), k)
	l.mu.Unlock()
	if err != nil {
		// Logging as warning only, and otherwise ignoring the error. If this is called, the item has been propagated
		// from the fast to the slow store, and the only risk here is redoing that.
		log.WithFields(log.Fields{
			"op":    "writeback",
			"key":   k,
			"cause": err,
		}).Warning("Could not mark done")
	}
}

type asyncWrite struct {
	key      Key
	contents Value
}

// Paired is a store implementation that is meant to provide the benefits of a fast local store and long term
// persistence and accessibility of cloud storage. Paired writes to the fast store and queues async writes to the slow
// store. It reads from the fast store if possible. If not, reads from the slow store and copies content to the fast
// store for next time. It deletes from the slow store first and then from the fast store.
type Paired struct {
	fast Store
	slow Store

	// To start the background goroutine from Put operations.
	once sync.Once

	queue chan asyncWrite
	log   *propagationLog
}

func NewPaired(fast, slow Store, logPath string) (p *Paired, err error) {
	p = new(Paired)
	p.fast = fast
	p.slow = slow
	var keys []Key
	p.log, keys, err = newPropagationLog(logPath)
	if err != nil {
		return
	}
	sz := defaultPropagationBacklogSize
	if sz < len(keys) {
		// Make sure the paired store doesn't block on start!
		sz = len(keys)
	}
	p.queue = make(chan asyncWrite, sz)
	for _, key := range keys {
		contents, e := p.fast.Get(key)
		if e != nil {
			return nil, fmt.Errorf("can't get key %q from the fast store, won't be able to copy it to the slow store: %v", key, err)
		}
		p.queue <- asyncWrite{key, contents}
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

// Put writes an item to the fast store and enqueues it to be written
// to the slow store asynchronously. Might block if the async write
// queue is full. Since this operation is used by the code creating a
// new revision, triggered by "echo snapshot > ctl", this in turn might
// block the fileserver entirely for the duration of the snapshot. This
// hasn't happened to me in practice.  Could probably use the underlying
// filesystem as queue (e.g., hard or symbolic links) but that leads to
// assuming a disk store implementation and I don't want to.
func (p *Paired) Put(k Key, v Value) error {
	p.EnsureBackgroundPuts()
	if err := p.fast.Put(k, v); err != nil {
		return err
	}
	if err := p.log.todo(k); err != nil {
		return err
	}
	p.queue <- asyncWrite{k, v}
	return nil
}

func (p *Paired) EnsureBackgroundPuts() {
	p.once.Do(func() {
		go func() {
			for w := range p.queue {
				for {
					err := p.slow.Put(w.key, w.contents)
					if err == nil {
						p.log.done(w.key)
						break
					}
					log.WithFields(log.Fields{
						"key":   w.key,
						"cause": err.Error(),
					}).Warn("Could propagate to permanent storage, will retry")
					// TODO: We need some sort of exponential backoff here.
					time.Sleep(500 * time.Millisecond)
				}
			}
		}()
	})
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
