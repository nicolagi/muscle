package tree_test

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreFork(t *testing.T) {
	remoteStore, cleanup := newDisposableStore(t)
	defer cleanup()
	instance := "source"
	key := storage.RandomPointer().Bytes()
	blockFactory, err := block.NewFactory(remoteStore, remoteStore, key)
	require.Nil(t, err)
	store, err := tree.NewStore(
		blockFactory,
		remoteStore,
		"",
		tree.RemoteRootKeyPrefix+instance,
		storage.RandomPointer().Bytes(),
	)
	require.Nil(t, err)

	// Create a root node that will be used by all tests.
	var root tree.Node
	require.Nil(t, store.StoreNode(&root))

	t.Run("source instance does not exist", func(t *testing.T) {
		err := store.Fork("laptop", "desktop")
		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, storage.ErrNotFound))
	})

	t.Run("source exists, target does not (happy path)", func(t *testing.T) {
		sr := tree.NewRevision(instance, root.Key(), nil)
		err := store.StoreRevision(sr)
		require.Nil(t, err)
		require.Nil(t, store.UpdateRemoteRevision(sr))
		require.Nil(t, store.Fork(instance, "desktop"))

		tkey, err := store.RemoteRevisionKey("desktop")
		require.Nil(t, err)
		rev, err := store.LoadRevisionByKey(tkey)
		require.Nil(t, err)
		srevs, err := store.History(1, sr)
		require.Nil(t, err)
		trevs, err := store.History(3, rev)
		require.Nil(t, err)
		require.Len(t, trevs, 2)
		assert.Equal(t, trevs[1], srevs[0])

		t.Run("target instance already exists", func(t *testing.T) {
			require.NotNil(t, store.Fork(instance, "desktop"))
			tkey2, err := store.RemoteRevisionKey("desktop")
			require.Nil(t, err)
			rev, err := store.LoadRevisionByKey(tkey2)
			require.Nil(t, err)
			assert.Equal(t, tkey, tkey2)
			trevs2, err := store.History(3, rev)
			require.Nil(t, err)
			assert.Equal(t, trevs, trevs2)
		})
	})
}

func newDisposableStore(t *testing.T) (store *storage.DiskStore, cleanup func()) {
	d, err := ioutil.TempDir("", "test-muscle-tree-")
	require.Nil(t, err)
	return storage.NewDiskStore(d), func() {
		_ = os.RemoveAll(d)
	}
}
