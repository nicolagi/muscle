package tree

import (
	"testing"
	"time"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
)

func TestCodecV13(t *testing.T) {
	index := &storage.InMemory{}
	blockFactory, err := block.NewFactory(index, nil, make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	// Prepare a block and ensure it's in the index, for the decode to find.
	nodeBlock, err := blockFactory.New(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if flushed, err := nodeBlock.Flush(); err != nil {
		t.Fatal(err)
	} else if !flushed {
		t.Fatal("block not flushed, was it dirty?")
	}
	t.Run("defaults for properties added in later codecs", func(t *testing.T) {
		// Zero encoding for the node, save for a block reference.
		encoded := make([]byte, 47)
		p := encoded[26:]
		p = pint32(1, p) // number of blocks
		p = pint8(16, p) // number of bytes for index block reference
		pbytes(nodeBlock.Ref().Bytes(), p)
		node := Node{
			blockFactory: blockFactory,
		}
		codec := codecV13{}
		if err := codec.decodeNode(encoded, &node); err != nil {
			t.Fatal(err)
		}
		// Nodes encoded with V13 are necessarily sealed, i.e., they belong to the repository and are read-only.
		if got, want := node.flags, sealed; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		// Value of tree.DefaultBlockCapacity at the time of writing.
		if got, want := node.bsize, uint32(1024*1024); got != want {
			t.Errorf("got %v, want %v block size", got, want)
		}
		if got, want := len(node.blocks), 1; got != want {
			t.Errorf("got %v, want %v block", got, want)
		}
		// Indirectly verify the block's capacity.
		if err := node.blocks[0].Truncate(1024 * 1024); err != nil {
			t.Fatalf("Should be able to expand to full capacity:%v", err)
		}
		if err := node.blocks[0].Truncate(1024*1024 + 1); err == nil {
			t.Fatal("Should not be able to expand further")
		}
		// Verify the QID.
		if got, want := node.D.Qid.Version, uint32(1); got != want {
			t.Errorf("got %v, want %v as the version", got, want)
		}
		if got, want := node.D.Qid.Path, uint64(time.Now().UnixNano()); time.Duration(want-got) > time.Second {
			t.Errorf("got %v, want value within 1 second of %v", got, want)
		}
	})
}
