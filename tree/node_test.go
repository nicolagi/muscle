package tree

import (
	"math/rand"
	"testing"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/stretchr/testify/assert"
)

func TestNodeFlagsString(t *testing.T) {
	testCases := []struct {
		input  nodeFlags
		output string
	}{
		{0, "none"},
		{loaded, "loaded"},
		{dirty, "dirty"},
		{sealed, "sealed"},
		{loaded | dirty, "loaded,dirty"},
		{42, "dirty,extraneous"},
	}
	for _, tc := range testCases {
		if got, want := tc.input.String(), tc.output; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestNodePath(t *testing.T) {
	a := new(Node)
	b := new(Node)
	c := new(Node)

	a.D.Name = "root"
	b.D.Name = "child"
	c.D.Name = "rosemary"

	a.add(b)
	b.add(c)

	assert.Equal(t, "root", a.Path())
	assert.Equal(t, "root/child", b.Path())
	assert.Equal(t, "root/child/rosemary", c.Path())

	assert.Equal(t, "", (*Node)(nil).Path())
}

func nodeReadAll(node *Node) string {
	b := make([]byte, node.D.Length)
	node.ReadAt(b, 0)
	return string(b)
}

func blockReadAll(t *testing.T, b *block.Block) string {
	t.Helper()
	value, err := b.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return string(value)
}

func TestNodeWriting(t *testing.T) {
	pbs := DefaultBlockCapacity
	DefaultBlockCapacity = 6
	defer func() {
		DefaultBlockCapacity = pbs
	}()
	bf := blockFactory(t, nil)
	node := &Node{
		blockFactory: bf,
		pointer:      storage.RandomPointer(),
	}

	// Append first block and to first block.
	node.WriteAt([]byte("foo"), 0)
	assert.Equal(t, "foo", nodeReadAll(node))
	assert.Len(t, node.blocks, 1)

	// Cross blocks
	s := "012345012345012345"
	node.WriteAt([]byte(s), 0)
	assert.Equal(t, s, nodeReadAll(node))
	assert.Len(t, node.blocks, 3)
	for _, b := range node.blocks {
		assert.Equal(t, "012345", blockReadAll(t, b))
	}

	// Overwrite one block.
	node.WriteAt([]byte("xxxxxx"), 6)
	assert.Equal(t, "012345xxxxxx012345", nodeReadAll(node))
	assert.Len(t, node.blocks, 3)

	// Cross-block overwrite.
	node.WriteAt([]byte("yyyyyy"), 9)
	assert.Equal(t, "012345xxxyyyyyy345", nodeReadAll(node))
	assert.Len(t, node.blocks, 3)
	assert.Equal(t, "012345", blockReadAll(t, node.blocks[0]))
	assert.Equal(t, "xxxyyy", blockReadAll(t, node.blocks[1]))
	assert.Equal(t, "yyy345", blockReadAll(t, node.blocks[2]))

	t.Run("writing on block that is not loaded", func(t *testing.T) {
		key := make([]byte, 16)
		rand.Read(key)
		bf, err := block.NewFactory(storage.NewInMemory(0), storage.NewInMemory(0), key, DefaultBlockCapacity)
		if err != nil {
			t.Fatal(err)
		}

		node := &Node{
			blockFactory: bf,
			pointer:      storage.RandomPointer(),
		}
		node.WriteAt([]byte("whiteboard"), 0)
		flushed, err := node.blocks[0].Flush()
		if !flushed {
			t.Fatal("not flushed")
		}
		if err != nil {
			t.Fatal(err)
		}
		flushed, err = node.blocks[1].Flush()
		if !flushed {
			t.Fatal("not flushed")
		}
		if err != nil {
			t.Fatal(err)
		}
		ref1 := node.blocks[0].Ref()
		ref2 := node.blocks[1].Ref()
		node = nil

		node = &Node{
			blockFactory: bf,
			pointer:      storage.RandomPointer(),
		}
		node.D.Length = 10 // This would have been serialized...
		block, err := bf.New(ref1)
		if err != nil {
			t.Fatal(err)
		}
		node.blocks = append(node.blocks, block)
		block, err = bf.New(ref2)
		if err != nil {
			t.Fatal(err)
		}
		node.blocks = append(node.blocks, block)

		assert.Nil(t, node.WriteAt([]byte("black"), 0))
		if got, want := node.D.Length, uint64(10); got != want {
			t.Errorf("got %d, want a %d-byte node", got, want)
		}
		if got, want := nodeReadAll(node), "blackboard"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}
