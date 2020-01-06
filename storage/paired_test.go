package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// This is fairly limited, examines only one interleaving of the events that happen concurrently.
// Any sequence of add(), next(), mark() can happen.
func TestPropagationLogPreservesStateAcrossRestarts(t *testing.T) {
	f := func(byteKeys [][32]byte, restart int) bool {
		logFile, err := ioutil.TempFile("", "")
		require.Nil(t, err)
		defer func() {
			_ = os.Remove(logFile.Name())
			matches, _ := filepath.Glob(logFile.Name() + ".*")
			for _, archived := range matches {
				_ = os.Remove(archived)
			}
		}()
		_ = logFile.Close()
		log, err := newLog(logFile.Name())
		require.Nil(t, err)

		keys := make([]Key, len(byteKeys))
		for i, raw := range byteKeys {
			k := Key(fmt.Sprintf("%x", raw))
			keys[i] = k
			require.Nil(t, log.add(k))
		}
		p := make([]byte, logLineLength)
		i := 0
		stop := 0
		if len(byteKeys) > 0 {
			stop = restart % len(byteKeys)
		}
		for ; i < stop; i++ {
			log.next(p)
			if strings.IndexByte("pmd", p[0]) == -1 {
				t.Errorf("unknown state %d", p[0])
				return false
			}
			if nextKey := Key(p[1:65]); nextKey != keys[i] {
				t.Errorf("key mismatch, got %q, want %q", nextKey, keys[i])
				return false
			}
			require.Nil(t, log.mark(itemDone))
		}
		// Shutdown.
		log.close()

		// Restart and process the rest.
		log, err = newLog(logFile.Name())
		require.Nil(t, err)
		for ; i < len(byteKeys); i++ {
			log.next(p)
			if strings.IndexByte("pmd", p[0]) == -1 {
				t.Errorf("unknown state %d", p[0])
				return false
			}
			if nextKey := Key(p[1:65]); nextKey != keys[i] {
				t.Errorf("key mismatch, got %q, want %q", nextKey, keys[i])
				return false
			}
			require.Nil(t, log.mark(itemDone))
		}

		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestPaired(t *testing.T) {

	t.Run("Successful put and get from fast store", func(t *testing.T) {
		fast := NewInMemory()
		logFilePath, cleanupLog := disposablePathName(t)
		defer cleanupLog()
		paired, err := NewPaired(fast, NullStore{}, logFilePath)
		require.Nil(t, err)
		k, v := RandomPair()
		assert.Nil(t, paired.Put(k, v))
		after, err := paired.Get(k)
		assert.Nil(t, err)
		assert.Equal(t, v, after)
	})

	t.Run("Get when fast store does not have key and slow store breaks", func(t *testing.T) {
		fast := NewInMemory()

		pathname, cleanupLog := disposablePathName(t)
		defer cleanupLog()

		slow := new(StoreMock)
		cannedErr := errors.New("failed")
		slow.On("Get", mock.Anything).Return(nil, cannedErr)

		store, err := NewPaired(fast, slow, pathname)
		require.Nil(t, err)

		after, err := store.Get(RandomKey())
		assert.Nil(t, after)
		assert.Equal(t, cannedErr, err)
	})

	t.Run("Get propagates from slow to fast", func(t *testing.T) {
		fast := NewInMemory()
		slow := NewInMemory()

		k, v := RandomPair()
		assert.Nil(t, slow.Put(k, v))
		pathname, cleanupLog := disposablePathName(t)
		defer cleanupLog()
		store, err := NewPaired(fast, slow, pathname)
		require.Nil(t, err)

		after1, err1 := store.Get(k)
		after2, err2 := fast.Get(k)
		assert.Equal(t, v, after1)
		assert.Equal(t, v, after2)
		assert.Nil(t, err1)
		assert.Nil(t, err2)
	})

	t.Run("Get succeeds even if propagation to fast store fails", func(t *testing.T) {
		fast := new(StoreMock)
		fast.On("Get", mock.Anything).Return(nil, ErrNotFound)
		fast.On("Put", mock.Anything, mock.Anything).Return(errors.New("failed"))

		slow := NewInMemory()

		k, v := RandomPair()
		assert.Nil(t, slow.Put(k, v))
		pathname, cleanupLog := disposablePathName(t)
		defer cleanupLog()
		store, err := NewPaired(fast, slow, pathname)
		require.Nil(t, err)
		after, err := store.Get(k)
		assert.Equal(t, v, after)
		assert.Nil(t, err)
	})

	t.Run("Put propagates asynchronously from fast to slow", func(t *testing.T) {
		fast := NewInMemory()
		slow := NewInMemory()

		k, v := RandomPair()
		pathname, cleanupLog := disposablePathName(t)
		defer cleanupLog()
		store, err := NewPaired(fast, slow, pathname)
		require.Nil(t, err)
		store.log.pollInterval = 5 * time.Millisecond
		_ = store.Put(k, v)
		contents, err := fast.Get(k)
		assert.Equal(t, Value(v), contents)
		assert.Nil(t, err)

		done := make(chan struct{})
		go func() {
			for {
				after, err := slow.Get(k)
				if err == nil {
					assert.Equal(t, v, after)
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Errorf("timed out waiting for item to be in slow store")
		}
	})
}

func disposablePathName(t *testing.T) (pathname string, cleanup func()) {
	f, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	require.Nil(t, f.Close())
	return f.Name(), func() {
		assert.Nil(t, os.Remove(f.Name()))
	}
}
