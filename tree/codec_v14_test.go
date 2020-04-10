package tree

import "testing"

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
	})
}
