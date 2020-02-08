package storage // import "github.com/nicolagi/muscle/storage"

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskStore_Get(t *testing.T) {
	store, clean := disposableDiskStore(t)
	defer clean()
	key := RandomPointer().Key()
	value := Value("some value")
	err := store.Put(key, value)
	require.Nil(t, err)
	actual, err := store.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, actual)
}

func TestDiskStore_Delete(t *testing.T) {
	store, clean := disposableDiskStore(t)
	defer clean()
	key := RandomPointer()
	err := store.Put(key.Key(), key.Bytes())
	require.Nil(t, err)
	err = store.Delete(key.Key())
	require.Nil(t, err)
	value, err := store.Get(key.Key())
	assert.Nil(t, value)
	assert.NotNil(t, err)
}

func TestDiskStore_ForEach(t *testing.T) {
	store, clean := disposableDiskStore(t)
	defer clean()
	value := []byte("irrelevant contents")
	for i := 0; i < 20; i++ {
		require.Nil(t, store.Put(RandomPointer().Key(), value))
	}
	deleteHalf(t, store, 20)
	deleteHalf(t, store, 10)
	deleteHalf(t, store, 5)
}

func TestDiskStore_Contains(t *testing.T) {
	store, clean := disposableDiskStore(t)
	defer clean()
	f := func(key [32]byte, value Value) bool {
		k := Key(fmt.Sprintf("%x", key))
		contains, err := store.Contains(k)
		if err != nil {
			t.Log(err)
			return false
		}
		if contains {
			return false
		}
		err = store.Put(k, value)
		if err != nil {
			t.Log(err)
			return false
		}
		contains, err = store.Contains(k)
		if err != nil {
			t.Log(err)
			return false
		}
		return contains
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

// Using PBT to catch a %02x vs %x bug.
// Could easily do it with an oracle test but I wanted to do this exercise.
func TestDiskStore_PathFor(t *testing.T) {
	store := NewDiskStore("dir")
	for i := 0; i < 1000; i++ {
		// 3 + 1 (slash) + 2 (first byte) + 1 (slash) + 64 (all bytes)
		assert.Len(t, store.pathFor(RandomPointer().Key()), 71)
	}
}

func deleteHalf(t *testing.T, store *DiskStore, expectedKeysCount int) {
	actualKeysCount := 0
	require.Nil(t, store.ForEach(func(k Key) error {
		if actualKeysCount%2 == 0 {
			require.Nil(t, store.Delete(k))
		}
		actualKeysCount++
		return nil
	}))
	assert.Equal(t, expectedKeysCount, actualKeysCount)
}

func disposableDiskStore(t *testing.T) (store *DiskStore, cleanup func()) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	return NewDiskStore(dir), func() {
		assert.Nil(t, os.RemoveAll(store.dir))
	}
}
