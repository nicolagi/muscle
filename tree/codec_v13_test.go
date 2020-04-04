package tree

import "testing"

func TestCodecV13(t *testing.T) {
	t.Run("defaults for properties added in later codecs", func(t *testing.T) {
		encoded := make([]byte, 30)
		node := Node{}
		codec := codecV13{}
		codec.decodeNode(encoded, &node)
		// Nodes encoded with V13 are necessarily sealed, i.e., they belong to the repository and are read-only.
		if got, want := node.flags, sealed; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		// Value of tree.DefaultBlockCapacity at the time of writing.
		if got, want := node.bsize, uint32(1024*1024); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}
