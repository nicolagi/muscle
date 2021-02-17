package tree

import (
	"math/rand"
	"testing"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/storage"
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
		{42, "dirty,unlinked,extraneous"}, // bits 1, 3, 5
	}
	for _, tc := range testCases {
		if got, want := tc.input.String(), tc.output; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestNodePath(t *testing.T) {
	a := &Node{flags: loaded}
	b := &Node{flags: loaded}
	c := &Node{flags: loaded}

	a.info.Name = "root"
	b.info.Name = "child"
	c.info.Name = "rosemary"

	if err := a.addChild(b); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := b.addChild(c); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, "/", a.Path())
	assert.Equal(t, "/child", b.Path())
	assert.Equal(t, "/child/rosemary", c.Path())

	assert.Equal(t, "", (*Node)(nil).Path())
}

func mustRead(t *testing.T, node *Node) string {
	t.Helper()
	b := make([]byte, node.info.Size)
	_, err := node.ReadAt(b, 0)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mustWrite(t *testing.T, node *Node, p []byte, off int64) {
	t.Helper()
	if err := node.WriteAt(p, off); err != nil {
		t.Fatal(err)
	}
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
	blockCapacity := uint32(6)
	node := &Node{
		blockFactory: blockFactory(t, nil),
		pointer:      storage.RandomPointer(),
		bsize:        blockCapacity,
	}

	// Append first block and to first block.
	mustWrite(t, node, []byte("foo"), 0)
	if got, want := mustRead(t, node), "foo"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := len(node.blocks), 1; got != want {
		t.Fatalf("got %d, want %d blocks", got, want)
	}

	// Cross blocks
	s := "012345012345012345"
	mustWrite(t, node, []byte(s), 0)
	if got, want := mustRead(t, node), s; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := len(node.blocks), 3; got != want {
		t.Fatalf("got %d, want %d blocks", got, want)
	}
	for i, b := range node.blocks {
		if got, want := blockReadAll(t, b), "012345"; got != want {
			t.Fatalf("got %q, want %q at block %d", got, want, i)
		}
	}

	// Overwrite one block.
	mustWrite(t, node, []byte("xxxxxx"), 6)
	assert.Equal(t, "012345xxxxxx012345", mustRead(t, node))
	assert.Len(t, node.blocks, 3)

	// Cross-block overwrite.
	mustWrite(t, node, []byte("yyyyyy"), 9)
	assert.Equal(t, "012345xxxyyyyyy345", mustRead(t, node))
	assert.Len(t, node.blocks, 3)
	assert.Equal(t, "012345", blockReadAll(t, node.blocks[0]))
	assert.Equal(t, "xxxyyy", blockReadAll(t, node.blocks[1]))
	assert.Equal(t, "yyy345", blockReadAll(t, node.blocks[2]))

	t.Run("writing on block that is not loaded", func(t *testing.T) {
		key := make([]byte, 16)
		rand.Read(key)
		bf, err := block.NewFactory(&storage.InMemory{}, &storage.InMemory{}, key)
		if err != nil {
			t.Fatal(err)
		}

		node := &Node{
			blockFactory: bf,
			pointer:      storage.RandomPointer(),
			bsize:        blockCapacity,
		}
		mustWrite(t, node, []byte("whiteboard"), 0)
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
			bsize:        blockCapacity,
		}
		node.info.Size = 10 // This would have been serialized...
		b, err := bf.New(ref1, int(blockCapacity))
		if err != nil {
			t.Fatal(err)
		}
		node.blocks = append(node.blocks, b)
		b, err = bf.New(ref2, int(blockCapacity))
		if err != nil {
			t.Fatal(err)
		}
		node.blocks = append(node.blocks, b)

		mustWrite(t, node, []byte("black"), 0)
		if got, want := node.info.Size, uint64(10); got != want {
			t.Errorf("got %d, want a %d-byte node", got, want)
		}
		if got, want := mustRead(t, node), "blackboard"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}
