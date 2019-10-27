package tree

import (
	"errors"
	"testing"

	"github.com/nicolagi/muscle/diff"
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
	assert.True(t, a.SameAs(b))
	assert.True(t, b.SameAs(a))
}

func assertNotSame(t *testing.T, a, b diff.Node) {
	assert.False(t, a.SameAs(b))
	assert.False(t, b.SameAs(a))
}

func TestNodeMetaSameAs(t *testing.T) {
	t.Run("meta node never equals a nil node", func(t *testing.T) {
		var a nodeMeta
		assert.False(t, a.SameAs((*nodeMeta)(nil)))
		assert.False(t, a.SameAs((*diff.StringNode)(nil)))
		assert.False(t, a.SameAs(nil))
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
		assert.True(t, a.SameAs(a))
		assert.True(t, b.SameAs(b))
	})
	t.Run("comparison based on nodes with different checksum", func(t *testing.T) {
		ap := storage.RandomPointer()
		bp := storage.RandomPointer()
		a := nodeMeta{n: &Node{pointer: ap}}
		b := nodeMeta{n: &Node{pointer: bp}}
		assertNotSame(t, a, b)
		assert.True(t, a.SameAs(a))
		assert.True(t, b.SameAs(b))
	})
}

func TestNodeMetaContent(t *testing.T) {
	t.Run("meta node with zero node", func(t *testing.T) {
		a := nodeMeta{n: &Node{}}
		content, err := a.Content()
		assert.Nil(t, err)
		assert.Equal(t, `Key 
Dir.Size 0
Dir.Type 0
Dir.Dev 0
Dir.Qid.Type 0
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
		a.n.D.Qid.Type = 4
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
		b1, _ := storage.NewPointerFromHex("deadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00d")
		b2, _ := storage.NewPointerFromHex("8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef8badf00ddeadbeef")
		a.n.blocks = append(a.n.blocks, &Block{pointer: b1}, &Block{pointer: b2})
		content, err := a.Content()
		assert.Nil(t, err)
		assert.Equal(t, `Key f00df00df00df00df00df00df00df00df00df00df00df00df00df00df00df00d
Dir.Size 1
Dir.Type 2
Dir.Dev 3
Dir.Qid.Type 4
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
	t.Run("tree node never equals a nil node", func(t *testing.T) {
		var a treeNode
		assert.False(t, a.SameAs((*treeNode)(nil)))
		assert.False(t, a.SameAs((*diff.StringNode)(nil)))
		assert.False(t, a.SameAs(nil))
	})
	t.Run("tree node with inner nil node only equals a tree node with inner nil node", func(t *testing.T) {
		a := treeNode{}
		b := treeNode{}
		c := treeNode{n: &Node{pointer: storage.RandomPointer()}}
		assertSame(t, a, b)
		assertNotSame(t, a, c)
	})
	t.Run("comparisons based on nodes with same checksum", func(t *testing.T) {
		common := []*Block{
			{
				pointer: storage.RandomPointer(),
				state:   blockNotLoaded,
			},
		}
		a := treeNode{n: &Node{blocks: common}}
		b := treeNode{n: &Node{blocks: common}}
		assertSame(t, a, b)
		assert.True(t, a.SameAs(a))
		assert.True(t, b.SameAs(b))
	})
	t.Run("comparison based on nodes with different checksum", func(t *testing.T) {
		ablock := Block{pointer: storage.RandomPointer()}
		bblock := Block{pointer: storage.RandomPointer()}
		a := treeNode{n: &Node{blocks: []*Block{&ablock}}}
		b := treeNode{n: &Node{blocks: []*Block{&bblock}}}
		assertNotSame(t, a, b)
		assert.True(t, a.SameAs(a))
		assert.True(t, b.SameAs(b))
	})
}

func TestTreeNodeContent(t *testing.T) {
	t.Run("get contents for nil node", func(t *testing.T) {
		a := &treeNode{}
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.Nil(t, err)
	})
	t.Run("get contents for small node", func(t *testing.T) {
		a := &treeNode{t: &Tree{}, n: &Node{}, maxSize: 1024}
		require.Nil(t, a.t.WriteAt(a.n, []byte("some text"), 0))
		content, err := a.Content()
		assert.Equal(t, "some text", content)
		assert.Nil(t, err)
	})
	t.Run("no error but not all node contents", func(t *testing.T) {
		a := &treeNode{maxSize: 1024}
		a.t = &Tree{}
		a.n = &Node{}
		a.n.D.Length = 42
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, errTreeNodeTruncated))
	})
	t.Run("get contents for too large a node", func(t *testing.T) {
		a := &treeNode{n: &Node{}, maxSize: 1024}
		a.n.D.Length = 1025
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, errTreeNodeLarge))
	})
	t.Run("read error bubbles up", func(t *testing.T) {
		a := &treeNode{t: &Tree{}, n: &Node{}, maxSize: 1024}

		// Set up the tree with a store that will only return errors.
		innerErr := errors.New("some error")
		layeredStore := storage.NewMartino(brokenStore{err: innerErr}, nil)
		var err error
		a.t.store, err = NewStore(layeredStore, nil, "", "", storage.RandomPointer().Bytes())
		require.Nil(t, err)

		// Pretend the node has some content to load from the store.
		a.n.D.Length = 1
		a.n.blocks = append(a.n.blocks, &Block{
			pointer:  storage.RandomPointer(),
			contents: nil,
			state:    blockNotLoaded,
		})

		// Now we expect a failure to read (ultimately because of the store errors.
		content, err := a.Content()
		assert.Equal(t, "", content)
		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, innerErr))
	})
}
