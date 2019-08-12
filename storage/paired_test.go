package storage

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPropagationLogPreservesStateAcrossRestarts(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run("", func(t *testing.T) {
			todoCount := rand.Intn(10)
			doneCount := rand.Intn(10)
			if doneCount > todoCount {
				doneCount = todoCount
			}

			pathname, cleanup := disposablePathName(t)
			defer cleanup()

			log, keys, err := newPropagationLog(pathname)
			require.Nil(t, err)
			require.Len(t, keys, 0)

			var before []Key
			for i := 0; i < todoCount; i++ {
				pointer := RandomPointer()
				require.Nil(t, log.todo(pointer.Key()))
				before = append(before, pointer.Key())
			}

			// Mark the required number of keys as done in the log - order should not matter.
			rand.Shuffle(len(before), func(i, j int) {
				before[i], before[j] = before[j], before[i]
			})
			for i := 0; i < doneCount; i++ {
				log.done(before[i])
			}

			require.Nil(t, log.f.Close())

			_, after, err := newPropagationLog(pathname)
			require.Nil(t, err)
			assert.ElementsMatch(t, before[doneCount:], after)
		})
	}
}

func TestPaired(t *testing.T) {

	t.Run("Successful put and get from fast store", func(t *testing.T) {
		fast, cleanupStore := disposableDiskStore(t)
		defer cleanupStore()
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
		fast, cleanupStore := disposableDiskStore(t)
		defer cleanupStore()
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
		fast, cleanupFast := disposableDiskStore(t)
		defer cleanupFast()
		slow, cleanupSlow := disposableDiskStore(t)
		defer cleanupSlow()

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

		slow, cleanupStore := disposableDiskStore(t)
		defer cleanupStore()

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
		fast, cleanupFast := disposableDiskStore(t)
		defer cleanupFast()
		slow, cleanupSlow := disposableDiskStore(t)
		defer cleanupSlow()

		k, v := RandomPair()
		pathname, cleanupLog := disposablePathName(t)
		defer cleanupLog()
		store, err := NewPaired(fast, slow, pathname)
		require.Nil(t, err)
		_ = store.Put(k, v)
		contents, err := fast.Get(k)
		assert.Equal(t, Value(v), contents)
		assert.Nil(t, err)

		for {
			if len(store.queue) == 0 {
				after, err := slow.Get(k)
				assert.Equal(t, v, after)
				assert.Nil(t, err)
				break
			}
			time.Sleep(time.Millisecond)
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
