package tree

import (
	"math/rand"
	"testing"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
)

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
