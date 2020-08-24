package tree

import (
	"testing"
	"time"
)

func TestCodecV14(t *testing.T) {
	t.Run("if stored block size is 0, use hard-coded default", func(t *testing.T) {
		encoded := make([]byte, 35)
		node := Node{}
		codec := codecV14{}
		if err := codec.decodeNode(encoded, &node); err != nil {
			t.Fatal(err)
		}
		// Value of tree.DefaultBlockCapacity at the time of writing.
		if got, want := node.bsize, uint32(1024*1024); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		// Verify the QID.
		if got, want := node.D.Version, uint32(1); got != want {
			t.Errorf("got %v, want %v as the version", got, want)
		}
		if got, want := node.D.ID, uint64(time.Now().UnixNano()); time.Duration(want-got) > time.Second {
			t.Errorf("got %v, want value within 1 second of %v", got, want)
		}
	})
}
