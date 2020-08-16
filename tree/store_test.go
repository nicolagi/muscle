package tree

import (
	"math/rand"
	"testing"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
)

func TestStoreLoadNode(t *testing.T) {
	t.Run("loaded node's owner and group are inherited from the process", func(t *testing.T) {
		t.Logf("nodeUID=%s nodeGID=%s", nodeUID, nodeGID)
		if nodeUID == "" || nodeGID == "" {
			t.Fatal("nodeUID or nodeGID not set")
		}
		s := newTestStore(t)
		node := &Node{}
		if err := s.StoreNode(node); err != nil {
			t.Fatal(err)
		}
		if err := s.LoadNode(node); err != nil {
			t.Error(err)
		}
		if got, want := node.D.Uid, nodeUID; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func newTestBlockFactory(t *testing.T) *block.Factory {
	t.Helper()
	key := make([]byte, 16)
	rand.Read(key)
	index := &storage.InMemory{}
	bf, err := block.NewFactory(index, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	return bf
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	baseDir := t.TempDir()
	treeStore, err := NewStore(newTestBlockFactory(t), nil, baseDir)
	if err != nil {
		t.Fatal(err)
	}
	return treeStore
}
