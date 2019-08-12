package tree // import "github.com/nicolagi/muscle/tree"

import (
	"github.com/nicolagi/muscle/storage"
)

// TODO This de-facto constant is a very big problem, because it can't ever be changed.
// The constant is used to calculate offsets etc. because there's no explicit size for
// stored blocks. The size should be stored per node or per block.

// Only unit tests should change this.
var blockSizeBytes = 1024 * 1024

type blockState uint8

const (
	// Contents are nil, should use the key to fetch them from the store if
	// needed, and transition to loaded.
	blockNotLoaded blockState = iota

	// The key is the hash of the contents.  This is the state after
	// loading and after saving.
	blockClean

	// The key is no longer the hash of the contents.  Happens when the
	// contents have been updated.  Save the block to go back to clean.
	blockDirty
)

func (s blockState) String() string {
	switch s {
	case blockNotLoaded:
		return "not loaded"
	case blockClean:
		return "clean"
	case blockDirty:
		return "dirty"
	default:
		panic("unknown block state")
	}
}

// Block describes a block of data for a regular file.
type Block struct {
	pointer  storage.Pointer
	contents []byte
	state    blockState
}

func (b *Block) write(p []byte, off int) (n int, delta int) {
	max := blockSizeBytes - off
	if len(p) > max {
		p = p[:max]
	}
	before := len(b.contents)
	if padding := off - before; padding > 0 {
		// TODO: We should have sparse files rather than pad with zeros.
		b.contents = append(b.contents, make([]byte, padding)...)
	}
	copied := copy(b.contents[off:], p)
	if copied < len(p) {
		b.contents = append(b.contents, p[copied:]...)
	}
	b.state = blockDirty
	return len(p), len(b.contents) - before
}

// expand pads the block to the right with up to increment zero bytes. Returns the
// number of zero bytes actually added (this is constrained by the block size).
func (b *Block) expand(incr uint64) uint64 {
	if b == nil || len(b.contents) == blockSizeBytes {
		return 0
	}
	if avail := uint64(blockSizeBytes - len(b.contents)); incr > avail {
		incr = avail
	}
	b.contents = append(b.contents, make([]byte, incr)...)
	b.state = blockDirty
	return incr
}
