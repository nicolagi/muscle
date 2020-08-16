package tree

import (
	"testing"
	"testing/quick"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/stretchr/testify/assert"
)

func TestLatestCodecForNodes(t *testing.T) {
	c := newMultiCodec()
	c.register(13, &codecV13{})
	c.register(14, &codecV14{})
	c.register(15, &codecV15{})
	key := make([]byte, 16)
	factory, err := block.NewFactory(nil, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("for nodes", func(t *testing.T) {
		f := func(
			name string,
			flags uint8,
			qidType uint8,
			qidPath uint64,
			qidVersion uint32,
			bsize uint32,
			mode uint32,
			mtime uint32,
			length uint64,
			children [][]byte,
			indexBlocks [][16]byte,
			repositoryBlocks [][32]byte,
		) bool {
			input := &Node{}
			input.flags = nodeFlags(flags) & ^(loaded | dirty)
			input.bsize = bsize
			input.D.Qid.Type = qidType
			input.D.Qid.Path = qidPath
			input.D.Qid.Version = qidVersion
			input.D.Name = name
			input.D.Mode = mode
			input.D.Mtime = mtime
			input.D.Length = length
			for _, b := range children {
				input.children = append(input.children, &Node{
					pointer: storage.NewPointer(b),
				})
			}
			for _, ref := range indexBlocks {
				r, err := block.NewRef(ref[:])
				if err != nil {
					t.Log(err)
					return false
				}
				b, err := factory.New(r, int(bsize))
				if err != nil {
					t.Log(err)
					return false
				}
				input.blocks = append(input.blocks, b)
			}
			for _, ref := range repositoryBlocks {
				r, err := block.NewRef(ref[:])
				if err != nil {
					t.Log(err)
					return false
				}
				b, err := factory.New(r, int(bsize))
				if err != nil {
					t.Log(err)
					return false
				}
				input.blocks = append(input.blocks, b)
			}

			// Normalize
			input.D.Atime = mtime
			for _, c := range input.children {
				c.parent = input
			}
			if input.D.Mode&DMDIR != 0 {
				input.D.Qid.Type = QTDIR
				input.D.Length = 0
			}
			// This would only be needed on the output node, but adding it here as well for comparison.
			input.blockFactory = factory

			b, err := c.encodeNode(input)
			if err != nil {
				t.Log(err)
				return false
			}
			output := Node{blockFactory: factory}
			if err := c.decodeNode(b, &output); err != nil {
				t.Log(err)
				return false
			}
			// TODO Write comparison function to avoid assert.Equal.
			return assert.Equal(t, *input, output)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	t.Run("for revisions", func(t *testing.T) {
		f := func(
			rootKey []byte,
			parentKey []byte,
			when int64,
			hostname string,
		) bool {
			input := &Revision{}
			input.rootKey = storage.NewPointer(rootKey)
			input.parent = storage.NewPointer(parentKey)
			input.when = when
			input.host = hostname
			b, err := c.encodeRevision(input)
			if err != nil {
				t.Log(err)
				return false
			}
			var output Revision
			if err := c.decodeRevision(b, &output); err != nil {
				t.Log(err)
				return false
			}
			// TODO Write comparison function to avoid assert.Equal.
			return assert.Equal(t, *input, output)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}
