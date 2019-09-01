package tree

import (
	"math/rand"
	"testing"
	"time"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/muscle/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var c *multiCodec

func init() {
	c = newMultiCodec()
	c.register(13, &codecV13{})
}

func TestEncodeThenDecodeNode(t *testing.T) {
	rand.Seed(time.Now().Unix())
	testCases := []struct {
		name  string
		input Node
	}{
		{
			name: "random",
			input: Node{
				D: p.Dir{
					Name:   "file", // TODO randomize
					Atime:  uint32(time.Now().Unix()),
					Mtime:  uint32(time.Now().Unix()),
					Length: rand.Uint64(),
					Mode:   rand.Uint32(),
				},
				children: []*Node{&Node{pointer: storage.RandomPointer()}},
				blocks: []*Block{
					&Block{pointer: storage.RandomPointer()},
					&Block{pointer: storage.RandomPointer()},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, c := range tc.input.children {
				c.parent = &tc.input
			}
			if tc.input.D.Mode&p.DMDIR != 0 {
				tc.input.D.Qid.Type = p.QTDIR
				tc.input.D.Length = 0
			}
			b, err := c.encodeNode(&tc.input)
			require.Nil(t, err)
			var output Node
			assert.Nil(t, c.decodeNode(b, &output))
			assert.Equal(t, tc.input, output)
		})
	}
}

func TestEncodeThenDecodeRevision(t *testing.T) {
	rand.Seed(time.Now().Unix())
	testCases := []struct {
		name  string
		input Revision
	}{
		{"empty", Revision{}},
		{"with root key", Revision{
			rootKey: storage.RandomPointer(),
		}},
		{"with a null parent", Revision{
			parents: []storage.Pointer{
				storage.Null,
			},
		}},
		{"with one parent", Revision{
			parents: []storage.Pointer{
				storage.RandomPointer(),
			},
		}},
		{"with two parents", Revision{
			parents: []storage.Pointer{
				storage.RandomPointer(),
				storage.RandomPointer(),
			},
		}},
		{"with timestamp", Revision{when: rand.Int63()}},
		{"with hostname", Revision{hostname: "hostname"}}, // TODO randomize
		{"with comment", Revision{instance: "darkstar"}},  // TODO randomize
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testIdempotent(t, tc.input)
		})
	}
	var in Revision
	in.parents = []storage.Pointer{
		storage.RandomPointer(),
	}
	testIdempotent(t, in)
	in.parents = nil
	testIdempotent(t, in)
}

func testIdempotent(t *testing.T, input Revision) {
	var output Revision
	b, err := c.encodeRevision(&input)
	require.Nil(t, err)
	err = c.decodeRevision(b, &output)
	assert.Nil(t, err)
	assert.Equal(t, input, output)
}
