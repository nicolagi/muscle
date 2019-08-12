package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMartino_Put(t *testing.T) {
	internalErr := errors.New("internal error of some kind")
	t.Run("Put fails if putting to staging area fails", func(t *testing.T) {
		t.Parallel()
		m := newMartinoWithMocks()
		m.staging.(*StoreMock).On("Put", mock.Anything, mock.Anything).Return(internalErr)
		_, err := m.Put(RandomValue())
		assert.Equal(t, internalErr, err)
	})
	t.Run("Puts only to staging area", func(t *testing.T) {
		t.Parallel()
		withDisposableMartino(t, func(t *testing.T, m *Martino) {
			value := RandomPointer().Bytes()
			k, err := m.Put(value)
			assert.Nil(t, err)

			actual, err := m.staging.Get(k.Key())
			assert.Equal(t, Value(value), actual)
			assert.Nil(t, err)

			actual, err = m.store.Get(k.Key())
			assert.Nil(t, actual)
			assert.True(t, errors.Is(err, ErrNotFound))
		})
	})
	t.Run("Returned key depends on contents", func(t *testing.T) {
		t.Parallel()
		m := newMartinoWithMocks()
		m.staging.(*StoreMock).On("Put", mock.Anything, mock.Anything).Return(nil)
		k1, err1 := m.Put(RandomValue())
		k2, err2 := m.Put(RandomValue())
		assert.Nil(t, err1)
		assert.Nil(t, err2)
		assert.NotEqual(t, k1.Hex(), k2.Hex())
	})
}

func TestMartino_Get(t *testing.T) {
	internalErr := errors.New("internal error of some kind")
	t.Run("Happy paths with real stores", func(t *testing.T) {
		t.Parallel()
		withDisposableMartino(t, func(t *testing.T, m *Martino) {
			// Missing key case
			actual, err := m.Get(RandomPointer())
			assert.Nil(t, actual)
			assert.True(t, errors.Is(err, ErrNotFound))

			// Key only in the store.
			// Can find value, but not copy to staging.
			k := RandomPointer()
			v := RandomPointer().Bytes()
			assert.Nil(t, m.store.Put(k.Key(), v))
			actual, err = m.Get(k)
			assert.Equal(t, v, actual)
			assert.Nil(t, err)
			actual, err = m.staging.Get(k.Key())
			assert.Nil(t, actual)
			assert.True(t, errors.Is(err, ErrNotFound))

			// Key only in staging. Should find it, without going to store.
			k = RandomPointer()
			v = RandomPointer().Bytes()
			assert.Nil(t, m.staging.Put(k.Key(), v))
			actual, err = m.Get(k)
			assert.Equal(t, v, actual)
			assert.Nil(t, err)
		})
	})
	t.Run("Get fails if staging area breaks", func(t *testing.T) {
		t.Parallel()
		m := newMartinoWithMocks()
		k := RandomPointer()
		m.staging.(*StoreMock).On("Get", k.Key()).Return(nil, internalErr)
		v, err := m.Get(k)
		assert.Nil(t, v)
		assert.Equal(t, internalErr, err)
	})
	t.Run("Get fails if store breaks", func(t *testing.T) {
		t.Parallel()
		m := newMartinoWithMocks()
		k := RandomPointer()
		m.staging.(*StoreMock).On("Get", k.Key()).Return(nil, ErrNotFound)
		m.store.(*Paired).fast.(*StoreMock).On("Get", k.Key()).Return(nil, internalErr)
		v, err := m.Get(k)
		assert.Nil(t, v)
		assert.Equal(t, internalErr, err)
	})
}

func TestMartino_Commit(t *testing.T) {
	t.Run("Iterates only keys in the staging area", func(t *testing.T) {
		t.Parallel()
		withDisposableMartino(t, func(t *testing.T, m *Martino) {
			k := RandomPointer()
			require.Nil(t, m.staging.Put(k.Key(), RandomValue()))
			require.Nil(t, m.store.Put(RandomPointer().Key(), RandomValue()))
			var keys []Key
			err := m.Commit(func(k Key) bool {
				keys = append(keys, k)
				return false
			})
			assert.Nil(t, err)
			assert.Len(t, keys, 1)
			assert.Equal(t, k.Key(), keys[0])
		})
	})
	t.Run("Persists only keys according to the test function provided", func(t *testing.T) {
		t.Parallel()
		withDisposableMartino(t, func(t *testing.T, m *Martino) {
			valueToPersist := RandomValue()
			keyToPersist, _ := m.Put(valueToPersist)
			keyToIgnore, _ := m.Put(RandomValue())
			err := m.Commit(func(k Key) bool {
				return k == keyToPersist.Key()
			})
			assert.Nil(t, err)

			// Vanishes keys from the staging area.
			actual, err := m.staging.Get(keyToPersist.Key())
			assert.Nil(t, actual)
			assert.True(t, errors.Is(err, ErrNotFound))
			actual, err = m.staging.Get(keyToIgnore.Key())
			assert.Nil(t, actual)
			assert.True(t, errors.Is(err, ErrNotFound))

			// Copies keys to the cache.
			actual, err = m.store.Get(keyToPersist.Key())
			assert.Equal(t, Value(valueToPersist), actual)
			assert.Nil(t, err)
			actual, err = m.store.Get(keyToIgnore.Key())
			assert.Nil(t, actual)
			assert.True(t, errors.Is(err, ErrNotFound))
		})
	})
	internalErr := errors.New("internal error of some kind")
	t.Run("Succeeds even if deletions and cache put fail", func(t *testing.T) {
		t.Parallel()
		t.Skip("To implement")
	})
	t.Run("Fails if store put fails", func(t *testing.T) {
		t.Parallel()
		withDisposableMartino(t, func(t *testing.T, m *Martino) {
			v1 := RandomValue()
			v2 := RandomValue()
			v3 := RandomValue()
			k1, _ := m.Put(v1)
			k2, _ := m.Put(v2)
			k3, _ := m.Put(v3)

			// Override only the remote store with a
			// mock, so it fails to put.
			remoteStore := &StoreMock{}
			remoteStore.On("Put", k1.Key(), mock.Anything).Return(nil)
			remoteStore.On("Put", k3.Key(), mock.Anything).Return(internalErr)
			m.store = remoteStore

			err := m.Commit(func(k Key) bool {
				return k != k2.Key()
			})
			assert.Equal(t, internalErr, err)

			// Crucial point. Since not all keys in the
			// staging area have been persisted to the
			// remote, we should not have deleted any.
			// That's too risky, because if we deleted
			// them as they are persisted, it could
			// break the algorithm that examines the
			// tree for legitimate blocks to persist
			// next time around. So we must check the
			// staging area still contains all items.
			v1after, err1 := m.staging.Get(k1.Key())
			v2after, err2 := m.staging.Get(k2.Key())
			v3after, err3 := m.staging.Get(k3.Key())
			assert.Nil(t, err1)
			assert.Nil(t, err2)
			assert.Nil(t, err3)
			assert.Equal(t, Value(v1), v1after)
			assert.Equal(t, Value(v2), v2after)
			assert.Equal(t, Value(v3), v3after)
		})
	})
	t.Run("Fails if staging get fails", func(t *testing.T) {
		t.Parallel()
		t.Skip("To implement")
	})
}

func newMartinoWithMocks() *Martino {
	p, err := NewPaired(new(StoreMock), new(StoreMock), "/tmp/martino.paired.log")
	if err != nil {
		panic(err)
	}
	return NewMartino(new(StoreMock), p)
}

func withDisposableMartino(t *testing.T, f func(*testing.T, *Martino)) {
	pathname, cleanupLog := disposablePathName(t)
	defer cleanupLog()
	staging, cleanupStaging := disposableDiskStore(t)
	defer cleanupStaging()
	fast, cleanupFast := disposableDiskStore(t)
	defer cleanupFast()
	slow, cleanupSlow := disposableDiskStore(t)
	defer cleanupSlow()
	paired, err := NewPaired(fast, slow, pathname)
	require.Nil(t, err)
	f(t, NewMartino(staging, paired))
}
