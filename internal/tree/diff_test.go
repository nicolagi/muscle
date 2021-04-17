package tree

import (
	"math/rand"
	"testing"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/storage"
	"github.com/stretchr/testify/assert"
)

type brokenStore struct {
	err error
}

func (b brokenStore) Get(storage.Key) (storage.Value, error) {
	return nil, b.err
}

func (b brokenStore) Put(storage.Key, storage.Value) error {
	return b.err
}

func (b brokenStore) Delete(storage.Key) error {
	return b.err
}

func (b brokenStore) Contains(storage.Key) (bool, error) {
	return false, b.err
}

func (b brokenStore) ForEach(func(storage.Key) error) error {
	return b.err
}

func assertSame(t *testing.T, a, b *Node) {
	t.Helper()
	assertComparison(t, a, b, true)
	assertComparison(t, b, a, true)
}

func assertNotSame(t *testing.T, a, b *Node) {
	t.Helper()
	assertComparison(t, a, b, false)
	assertComparison(t, b, a, false)
}

func assertComparison(t *testing.T, a, b *Node, want bool) {
	t.Helper()
	got := metaDiff(a, b) == ""
	if got != want {
		t.Errorf("got %t, want %t", got, want)
	}
}

func TestNodeMetaSameAs(t *testing.T) {
	t.Run("comparisons when one node is nil or both are", func(t *testing.T) {
		var a Node
		assertNotSame(t, &a, nil)
		assertSame(t, nil, nil)
	})
	t.Run("comparisons based on nodes with same checksum", func(t *testing.T) {
		common := storage.RandomPointer()
		a := &Node{pointer: common}
		b := &Node{pointer: common}
		a.info.Name = "a name"
		b.info.Name = "b name"
		assertSame(t, a, b)
		assertSame(t, a, a)
		assertSame(t, b, b)
	})
	t.Run("comparison based on nodes with different checksum", func(t *testing.T) {
		ap := storage.RandomPointer()
		bp := storage.RandomPointer()
		a := &Node{pointer: ap}
		b := &Node{pointer: bp}
		assertNotSame(t, a, b)
		assertSame(t, a, a)
		assertSame(t, b, b)
	})
}

func TestNodeMetaContent(t *testing.T) {
	bf := blockFactory(t, nil)
	t.Run("zero node against nil node", func(t *testing.T) {
		a := &Node{}
		assert.Equal(t, `-Key ""
-Dir.Qid.Version 0
-Dir.Qid.Path 0
-Dir.Mode 0
-Dir.Mtime 1970-01-01T00:00:00Z
-Dir.Length 0
-Dir.Name ""
-Blocks 
`, metaDiff(a, nil))
		assert.Equal(t, `+Key ""
+Dir.Qid.Version 0
+Dir.Qid.Path 0
+Dir.Mode 0
+Dir.Mtime 1970-01-01T00:00:00Z
+Dir.Length 0
+Dir.Name ""
+Blocks 
`, metaDiff(nil, a))
	})
	t.Run("comparison between non-nil nodes", func(t *testing.T) {
		var a, b Node
		a.pointer, _ = storage.NewPointerFromHex("f00df00df00df00df00df00df00df00df00df00df00df00df00df00df00df00d")
		a.info.Version = 5
		a.info.ID = 6
		a.info.Mode = 7
		a.info.Modified = 9
		a.info.Size = 10
		a.info.Name = "Carl"
		ref1, _ := block.NewRef([]byte{222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13})
		ref2, _ := block.NewRef([]byte{139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239})
		b1 := newBlock(t, bf, ref1)
		b2 := newBlock(t, bf, ref2)
		a.blocks = append(a.blocks, b1, b2)
		b = a
		b.pointer = storage.RandomPointer()
		b.info.Version++
		b.info.Name = "Rupert"
		assert.Equal(t, `-Key f00df00df00df00df00df00df00df00df00df00df00df00df00df00df00df00d
+Key 680b4e7c8b763a1b1d49d4955c8486216325253fec738dd7a9e28bf921119c16
-Dir.Qid.Version 5
+Dir.Qid.Version 6
 Dir.Qid.Path 6
 Dir.Mode 7
 Dir.Mtime "1970-01-01T00:00:09Z"
 Dir.Length 10
-Dir.Name "Carl"
+Dir.Name "Rupert"
 Blocks "deadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00d" "8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef"
`, metaDiff(&a, &b))
	})
}

func blockFactory(t *testing.T, storeErr error) *block.Factory {
	t.Helper()
	key := make([]byte, 16)
	rand.Read(key)
	index := brokenStore{err: storeErr}
	repository := brokenStore{err: storeErr}
	f, err := block.NewFactory(index, repository, key)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func newBlock(t *testing.T, f *block.Factory, ref block.Ref) *block.Block {
	t.Helper()
	b, err := f.New(ref, 8192)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
