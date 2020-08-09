package storage

import (
	"bytes"
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

	t.Run("Successful put and get from fast store regardless of slow store", func(t *testing.T) {
		fast := &InMemory{}
		logFilePath, cleanupLog := disposablePathName(t)
		defer cleanupLog()
		paired, err := NewPaired(fast, NullStore{}, logFilePath)
		require.Nil(t, err)
		f := func(key [32]byte, v []byte) bool {
			k := Key(fmt.Sprintf("%x", key))
			if err := paired.Put(k, v); err != nil {
				t.Log(err)
				return false
			}
			after, err := paired.Get(k)
			if err != nil {
				t.Log(err)
				return false
			}
			return bytes.Equal(v, after)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("Get when fast store does not have key and slow store breaks", func(t *testing.T) {
		fast := &InMemory{}

		pathname, cleanupLog := disposablePathName(t)
		defer cleanupLog()

		cannedErr := errors.New("failed")
		slow := storeFuncs{get: func(Key) (Value, error) { return nil, cannedErr }}

		store, err := NewPaired(fast, slow, pathname)
		require.Nil(t, err)

		k, _ := RandomKey(32)
		after, err := store.Get(k)
		assert.Nil(t, after)
		assert.Equal(t, cannedErr, err)
	})

	t.Run("Get propagates from slow to fast", func(t *testing.T) {
		pathname, cleanup := disposablePathName(t)
		defer cleanup()

		fast := &InMemory{}
		slow := &InMemory{}
		store, err := NewPaired(fast, slow, pathname)
		if err != nil {
			t.Fatal(err)
		}

		f := func(key [32]byte, v []byte) bool {
			k := Key(fmt.Sprintf("%x", key))
			if err := slow.Put(k, v); err != nil {
				t.Log(err)
				return false
			}
			after1, err := store.Get(k)
			if err != nil {
				t.Log(err)
				return false
			}
			after2, err := fast.Get(k)
			if err != nil {
				t.Log(err)
				return false
			}
			return bytes.Equal(v, after1) && bytes.Equal(v, after2)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("Get succeeds even if propagation to fast store fails", func(t *testing.T) {
		pathname, cleanupLog := disposablePathName(t)
		defer cleanupLog()

		fast := storeFuncs{
			get: func(Key) (Value, error) { return nil, ErrNotFound },
			put: func(Key, Value) error { return errors.New("failed") },
		}

		slow := &InMemory{}

		store, err := NewPaired(fast, slow, pathname)
		require.Nil(t, err)

		f := func(key [32]byte, v []byte) bool {
			k := Key(fmt.Sprintf("%x", key))
			if err := slow.Put(k, v); err != nil {
				t.Log(err)
				return false
			}
			if after, err := store.Get(k); err != nil {
				t.Log(err)
				return false
			} else {
				return bytes.Equal(v, after)
			}
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("Put propagates asynchronously from fast to slow, retrying as necessary", func(t *testing.T) {
		fast := &InMemory{}
		slow1 := &InMemory{}
		putErrs := make(map[Key]int)
		slow := storeFuncs{
			get: slow1.Get,
			put: func(k Key, v Value) error {
				if count := putErrs[k]; count < 5 {
					putErrs[k] = count + 1
					return fmt.Errorf("error %d on put of %v", 1+count, k)
				}
				putErrs[k] = 0
				return slow1.Put(k, v)
			},
		}

		k, err := RandomKey(32)
		require.Nil(t, err)
		value, err := RandomKey(64)
		require.Nil(t, err)
		v := []byte(value)
		pathname, cleanupLog := disposablePathName(t)
		defer cleanupLog()
		store, err := NewPaired(fast, slow, pathname)
		require.Nil(t, err)
		store.retryInterval = time.Millisecond
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
					assert.EqualValues(t, v, after)
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
