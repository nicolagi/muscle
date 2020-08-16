package tree

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/nicolagi/muscle/diff"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func assertSame(t *testing.T, a, b diff.Node) {
	t.Helper()
	assertComparison(t, a, b, true)
	assertComparison(t, b, a, true)
}

func assertNotSame(t *testing.T, a, b diff.Node) {
	t.Helper()
	assertComparison(t, a, b, false)
	assertComparison(t, b, a, false)
}

func assertComparison(t *testing.T, a, b diff.Node, want bool) {
	t.Helper()
	got, err := a.SameAs(b)
	if err != nil {
		t.Error(err)
	}
	if got != want {
		t.Errorf("got %t, want %t", got, want)
	}
}

func TestNodeMetaSameAs(t *testing.T) {
	t.Run("meta node never equals a nil node", func(t *testing.T) {
		var a nodeMeta
		assertComparison(t, a, (*nodeMeta)(nil), false)
		assertComparison(t, a, (*diff.StringNode)(nil), false)
		assertComparison(t, a, nil, false)
	})
	t.Run("meta node with inner nil node only equals a meta node with inner nil node", func(t *testing.T) {
		a := nodeMeta{}
		b := nodeMeta{}
		c := nodeMeta{n: &Node{pointer: storage.RandomPointer()}}
		assertSame(t, a, b)
		assertNotSame(t, a, c)
	})
	t.Run("comparisons based on nodes with same checksum", func(t *testing.T) {
		common := storage.RandomPointer()
		a := nodeMeta{n: &Node{pointer: common}}
		b := nodeMeta{n: &Node{pointer: common}}
		assertSame(t, a, b)
		assertSame(t, a, a)
		assertSame(t, b, b)
	})
	t.Run("comparison based on nodes with different checksum", func(t *testing.T) {
		ap := storage.RandomPointer()
		bp := storage.RandomPointer()
		a := nodeMeta{n: &Node{pointer: ap}}
		b := nodeMeta{n: &Node{pointer: bp}}
		assertNotSame(t, a, b)
		assertSame(t, a, a)
		assertSame(t, b, b)
	})
}

func TestNodeMetaContent(t *testing.T) {
	bf := blockFactory(t, nil)
	t.Run("meta node with zero node", func(t *testing.T) {
		a := nodeMeta{n: &Node{}}
		content, err := a.Content()
		assert.Nil(t, err)
		assert.Equal(t, `Key ""
Dir.Size 0
Dir.Type 0
Dir.Dev 0
Dir.Qid.Version 0
Dir.Qid.Path 0
Dir.Mode 0
Dir.Atime 1970-01-01T00:00:00Z
Dir.Mtime 1970-01-01T00:00:00Z
Dir.Length 0
Dir.Name ""
Dir.Uid ""
Dir.Gid ""
Dir.Muid ""
blocks:
`, content)
	})
	t.Run("meta node with non-zero node", func(t *testing.T) {
		a := nodeMeta{n: &Node{}}
		a.n.pointer, _ = storage.NewPointerFromHex("f00df00df00df00df00df00df00df00df00df00df00df00df00df00df00df00d")
		a.n.D.Size = 1
		a.n.D.Type = 2
		a.n.D.Dev = 3
		a.n.D.Qid.Version = 5
		a.n.D.Qid.Path = 6
		a.n.D.Mode = 7
		a.n.D.Atime = 8
		a.n.D.Mtime = 9
		a.n.D.Length = 10
		a.n.D.Name = "carl"
		a.n.D.Uid = "perkins"
		a.n.D.Gid = "eddie"
		a.n.D.Muid = "cochran"
		ref1, _ := block.NewRef([]byte{222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13})
		ref2, _ := block.NewRef([]byte{139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239, 139, 173, 240, 13, 222, 173, 190, 239})
		b1 := newBlock(t, bf, ref1)
		b2 := newBlock(t, bf, ref2)
		a.n.blocks = append(a.n.blocks, b1, b2)
		content, err := a.Content()
		assert.Nil(t, err)
		assert.Equal(t, `Key "f00df00df00df00df00df00df00df00df00df00df00df00df00df00df00df00d"
Dir.Size 1
Dir.Type 2
Dir.Dev 3
Dir.Qid.Version 5
Dir.Qid.Path 6
Dir.Mode 7
Dir.Atime 1970-01-01T00:00:08Z
Dir.Mtime 1970-01-01T00:00:09Z
Dir.Length 10
Dir.Name "carl"
Dir.Uid "perkins"
Dir.Gid "eddie"
Dir.Muid "cochran"
blocks:
	deadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00d
	8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef
`, content)
	})
	t.Run("meta node with nil node", func(t *testing.T) {
		a := nodeMeta{n: nil}
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.Nil(t, err)
	})
}

func TestTreeNodeSameAs(t *testing.T) {
	bf := blockFactory(t, nil)
	t.Run("tree node never equals a nil node", func(t *testing.T) {
		var a treeNode
		assertComparison(t, a, (*treeNode)(nil), false)
		assertComparison(t, a, (*diff.StringNode)(nil), false)
		assertComparison(t, a, nil, false)
	})
	t.Run("tree node with inner nil node only equals a tree node with inner nil node", func(t *testing.T) {
		a := treeNode{}
		b := treeNode{}
		c := treeNode{n: &Node{pointer: storage.RandomPointer()}}
		assertSame(t, a, b)
		assertNotSame(t, a, c)
	})
	t.Run("comparisons based on nodes with same checksum", func(t *testing.T) {
		common := dirtyBlock(t, bf, "some content")
		a := treeNode{n: &Node{blocks: []*block.Block{common}}}
		b := treeNode{n: &Node{blocks: []*block.Block{common}}}
		assertSame(t, a, b)
		assertSame(t, a, a)
		assertSame(t, b, b)
	})
	t.Run("comparison based on nodes with different checksum", func(t *testing.T) {
		ab := dirtyBlock(t, bf, "some content")
		bb := dirtyBlock(t, bf, "some other content")
		a := treeNode{n: &Node{blocks: []*block.Block{ab}}}
		b := treeNode{n: &Node{blocks: []*block.Block{bb}}}
		assertNotSame(t, a, b)
		assertSame(t, a, a)
		assertSame(t, b, b)
	})
}

func TestTreeNodeContent(t *testing.T) {
	bsize := uint32(8192)
	innerErr := errors.New("some error")
	bf := blockFactory(t, innerErr)
	t.Run("get contents for nil node", func(t *testing.T) {
		a := &treeNode{}
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.Nil(t, err)
	})
	t.Run("get contents for small node", func(t *testing.T) {
		a := &treeNode{t: &Tree{}, n: &Node{
			blockFactory: bf,
			bsize:        bsize,
		}, maxSize: 1024}
		require.Nil(t, a.n.WriteAt([]byte("some text"), 0))
		content, err := a.Content()
		assert.Equal(t, "some text", content)
		assert.Nil(t, err)
	})
	t.Run("no error but not all node contents", func(t *testing.T) {
		a := &treeNode{maxSize: 1024}
		a.t = &Tree{}
		a.n = &Node{bsize: bsize}
		a.n.D.Length = 42
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, errTreeNodeTruncated))
	})
	t.Run("get contents for too large a node", func(t *testing.T) {
		a := &treeNode{n: &Node{blockFactory: bf, bsize: bsize}, maxSize: 1024}
		a.n.D.Length = 1025
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, errTreeNodeLarge))
	})
	t.Run("read error bubbles up", func(t *testing.T) {
		a := &treeNode{t: &Tree{}, n: &Node{blockFactory: bf, bsize: bsize}, maxSize: 1024}

		// Pretend the node has some content to load from the store.
		a.n.D.Length = 1
		ref, err := block.NewRef(nil)
		if err != nil {
			t.Fatal(err)
		}
		b := newBlock(t, bf, ref)
		a.n.blocks = append(a.n.blocks, b)

		// Now we expect a failure to read (ultimately because of the store errors.
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, innerErr))
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

func dirtyBlock(t *testing.T, f *block.Factory, content string) *block.Block {
	t.Helper()
	b, err := f.New(nil, 8192)
	if err != nil {
		t.Fatal(err)
	}
	n, increase, err := b.Write([]byte(content), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(content) {
		t.Fatalf("got %d, want %d written bytes", n, len(content))
	}
	if increase != len(content) {
		t.Fatalf("got %d, want %d written bytes", increase, len(content))
	}
	return b
}

func newBlock(t *testing.T, f *block.Factory, ref block.Ref) *block.Block {
	t.Helper()
	b, err := f.New(ref, 8192)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
