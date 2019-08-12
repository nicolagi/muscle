package tree

import (
	"bytes"
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/muscle/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyBlockLoader struct {
	callCount      int
	cannedContents []byte
	cannedError    error
}

func (loader *dummyBlockLoader) LoadBlock(block *Block) error {
	loader.callCount++
	if loader.cannedContents != nil {
		block.contents = loader.cannedContents
	}
	return loader.cannedError
}

func TestEnsureBlocksForWriting(t *testing.T) {
	t.Run("no blocks required", func(t *testing.T) {
		n := new(Node)
		n.ensureBlocksForWriting(0)
		assert.Nil(t, n.blocks)
	})
	t.Run("one partial block required", func(t *testing.T) {
		n := new(Node)
		n.ensureBlocksForWriting(1)
		assert.Len(t, n.blocks, 1)
	})
	t.Run("one full block required", func(t *testing.T) {
		n := new(Node)
		n.ensureBlocksForWriting(int64(blockSizeBytes))
		assert.Len(t, n.blocks, 1)
	})
	t.Run("one full block and a byte required", func(t *testing.T) {
		n := new(Node)
		n.ensureBlocksForWriting(int64(blockSizeBytes + 1))
		assert.Len(t, n.blocks, 2)
	})
}

func TestBlockRange(t *testing.T) {
	offByOneBlock := int64(blockSizeBytes)
	testCases := []struct {
		byteOff    int64
		byteCount  int
		blockOff   int
		blockCount int
	}{
		{0, 0, 0, 0},
		{0, 1, 0, 1},
		{0, blockSizeBytes / 2, 0, 1},
		{0, blockSizeBytes, 0, 1},
		{0, blockSizeBytes + 1, 0, 2},
		{1, 0, 0, 0},
		{1, 1, 0, 1},
		{1, blockSizeBytes - 1, 0, 1},
		{1, blockSizeBytes, 0, 2},
		{offByOneBlock, 0, 1, 0},
		{offByOneBlock, 1, 1, 1},
		{offByOneBlock, blockSizeBytes, 1, 1},
		{offByOneBlock, blockSizeBytes + 1, 1, 2},
		{offByOneBlock / 2, blockSizeBytes / 2, 0, 1},
		{offByOneBlock / 2, blockSizeBytes/2 + 1, 0, 2},
		{offByOneBlock / 2, blockSizeBytes, 0, 2},
		{100, blockSizeBytes - 100, 0, 1},
		{100, blockSizeBytes - 99, 0, 2},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			blockOff, blockCount := blockRange(tc.byteOff, tc.byteCount)
			assert.Equal(t, tc.blockOff, blockOff)
			assert.Equal(t, tc.blockCount, blockCount)
		})
	}
}

func TestEnsureBlocksForReading(t *testing.T) {
	t.Run("no block required", func(t *testing.T) {
		assert.Nil(t, new(Node).ensureBlocksForReading(nil, 0, 0))
	})
	t.Run("no block required from non-empty node", func(t *testing.T) {
		n := new(Node)
		n.blocks = append(n.blocks, &Block{state: blockNotLoaded})
		loader := &dummyBlockLoader{}
		assert.Nil(t, n.ensureBlocksForReading(loader, 0, 0))
		assert.Equal(t, 0, loader.callCount)
	})
	t.Run("block required from empty node", func(t *testing.T) {
		assert.Nil(t, new(Node).ensureBlocksForReading(nil, 0, 1))
	})
	t.Run("block required from non-empty node", func(t *testing.T) {
		n := new(Node)
		n.blocks = append(n.blocks, &Block{state: blockNotLoaded})
		loader := &dummyBlockLoader{}
		assert.Nil(t, n.ensureBlocksForReading(loader, 0, 1))
		assert.Len(t, n.blocks, 1)
		assert.Equal(t, blockClean, n.blocks[0].state)
		assert.Equal(t, 1, loader.callCount)
	})
	t.Run("block not found error", func(t *testing.T) {
		n := new(Node)
		n.blocks = append(n.blocks, &Block{state: blockNotLoaded})
		loader := &dummyBlockLoader{cannedError: storage.ErrNotFound}
		assert.Nil(t, n.ensureBlocksForReading(loader, 0, 1))
		assert.Len(t, n.blocks, 1)
		assert.Equal(t, blockClean, n.blocks[0].state)
		assert.Equal(t, 1, loader.callCount)
	})
	t.Run("any other error", func(t *testing.T) {
		n := new(Node)
		n.blocks = append(n.blocks, &Block{state: blockNotLoaded})
		loader := &dummyBlockLoader{cannedError: errors.New("any other error")}
		assert.NotNil(t, n.ensureBlocksForReading(loader, 0, 1))
		assert.Len(t, n.blocks, 1)
		assert.Equal(t, blockNotLoaded, n.blocks[0].state)
		assert.Equal(t, 1, loader.callCount)
	})
	t.Run("block required but already loaded", func(t *testing.T) {
		n := new(Node)
		n.blocks = append(n.blocks, &Block{state: blockClean})
		n.blocks = append(n.blocks, &Block{state: blockDirty})
		loader := &dummyBlockLoader{}
		assert.Nil(t, n.ensureBlocksForReading(loader, 0, blockSizeBytes+1))
		assert.Len(t, n.blocks, 2)
		assert.Equal(t, blockClean, n.blocks[0].state)
		assert.Equal(t, blockDirty, n.blocks[1].state)
		assert.Equal(t, 0, loader.callCount)
	})
}

func TestNodeRead(t *testing.T) {
	const testBlockSizeBytes = 5
	newAlphabetBlock := func() *Block {
		block := new(Block)
		block.state = blockClean
		block.contents = make([]byte, testBlockSizeBytes)
		for alphaOff := 0; alphaOff < testBlockSizeBytes; alphaOff++ {
			block.contents[alphaOff] = byte('a' + alphaOff)
		}
		return block
	}
	readString := func(node *Node, off int64, count int) string {
		p := make([]byte, count)
		n := node.read(p, off)
		return string(p[:n])
	}
	t.Run("from empty node", func(t *testing.T) {
		p := make([]byte, 2)
		n := new(Node)
		assert.Equal(t, 0, n.read(p, 0))
		assert.Equal(t, 0, n.read(p, 1))
		assert.Equal(t, []byte{0, 0}, p)
	})
	t.Run("a part of first block", func(t *testing.T) {
		defer overrideBlockSize(testBlockSizeBytes)()
		n := new(Node)
		n.blocks = append(n.blocks, newAlphabetBlock())
		assert.Equal(t, "abc", readString(n, 0, 3))
		assert.Equal(t, "bcd", readString(n, 1, 3))
		assert.Equal(t, "cde", readString(n, 2, 3))
		assert.Equal(t, "de", readString(n, 3, 3))
		assert.Equal(t, "e", readString(n, 4, 3))
		assert.Equal(t, "", readString(n, 5, 3))
	})
	t.Run("a part of first block and a part of second block", func(t *testing.T) {
		defer overrideBlockSize(testBlockSizeBytes)()
		n := new(Node)
		n.blocks = append(n.blocks, newAlphabetBlock(), newAlphabetBlock())
		assert.Equal(t, "deabc", readString(n, 3, 5))
		assert.Equal(t, "eabcd", readString(n, 4, 5))
		assert.Equal(t, "abcde", readString(n, 5, 5))
		assert.Equal(t, "bcde", readString(n, 6, 5))
	})
	t.Run("reading past a block that is not full", func(t *testing.T) {
		defer overrideBlockSize(testBlockSizeBytes)()
		n := new(Node)
		block := newAlphabetBlock()
		block.contents = block.contents[:2]
		assert.Equal(t, "ab", string(block.contents)) // checking the test...
		n.blocks = append(n.blocks, block)
		assert.Equal(t, "", readString(n, 2, 3))
		assert.Equal(t, "", readString(n, 3, 3))
	})
}

func TestTruncateDirPrevented(t *testing.T) {
	n := new(Node)
	n.D.Mode = p.DMDIR
	assert.NotNil(t, n.truncate(nil, 42))
}

func TestTruncateExtendEmptyNode(t *testing.T) {
	n := new(Node)
	require.Nil(t, n.truncate(nil, 42))
	assert.Equal(t, uint64(42), n.D.Length)
	assert.Len(t, n.blocks, 1)
	assert.Len(t, n.blocks[0].contents, 42)
}

func TestTruncateExtends(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 1000; i++ {
		t.Run("", func(t *testing.T) {
			// Get random test parameters.
			blockSize := 5 + rand.Intn(16) // 5..20
			initialContent := make([]byte, rand.Intn(80))
			rand.Read(initialContent)
			// Always extend with zeros for this test.
			requestedLength := len(initialContent) + rand.Intn(len(initialContent)+1)
			defer overrideBlockSize(blockSize)()
			t.Logf("The block size is %d and the content size is %d, and we extend it to %d.",
				blockSize, len(initialContent), requestedLength)

			// Load the initial content into a new node, with a nil loader, because no blocks will need to be loaded at this
			// stage. Will also randomize each block state - the fake loader will simply set the state to clean or return an
			// error. This is just to prepare the node for the truncate operation.
			node := newNodeWithInitialContentAndRandomStateBlocks(t, initialContent)

			// Exercise the system under test.
			truncateWithBlockCleaner(t, node, requestedLength)

			// Verify all properties. The sum of all block sizes must match the requested size and must also match the node
			// size. All blocks that remain are in clean or dirty state (in other words, they need to have been loaded). The
			// remaining content is a prefix of the original content. All blocks but the last should be the same size as the
			// block size. (In a separate oracle test, in case of an error loading, the truncate operation fails.)
			assert.Equal(t, uint64(requestedLength), node.D.Length)

			// Prepare a buffer larger than needed to see if we have more bytes than expected.
			extendedContent := make([]byte, requestedLength+10)
			n := node.read(extendedContent, 0)
			assert.Equal(t, requestedLength, n)
			extendedContent = extendedContent[:n]
			assert.True(t, bytes.HasPrefix(extendedContent, initialContent))
			for i := len(initialContent); i < len(extendedContent); i++ {
				assert.Equalf(t, uint8(0), extendedContent[i], "Non-zero at index %d", i)
			}

			assertComputedSizeEquals(t, node, blockSize, requestedLength, len(initialContent))
		})
	}
}

// A property-based test for truncate. Will test edge cases separately. I will not consider 0 as a case to be tested
// because the probability of hitting that case below is at least 1-0.99^1000 which is ~0.9999568. The only case to test
// separately would be that the error from the load function is propagated and fails the truncate operation.
func TestTruncateProperties(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 1000; i++ {
		t.Run("", func(t *testing.T) {
			// Get random test parameters.
			blockSize := 5 + rand.Intn(16) // 5..20
			initialContent := make([]byte, rand.Intn(80))
			rand.Read(initialContent)
			// Always proper truncation for this test.
			requestedLength := rand.Intn(len(initialContent) + 1)
			defer overrideBlockSize(blockSize)()
			t.Logf("The block size is %d and the content size is %d, and we truncate it to %d.",
				blockSize, len(initialContent), requestedLength)

			// Load the initial content into a new node, with a nil loader, because no blocks will need to be loaded at this
			// stage. Will also randomize each block state - the fake loader will simply set the state to clean or return an
			// error. This is just to prepare the node for the truncate operation.
			node := newNodeWithInitialContentAndRandomStateBlocks(t, initialContent)

			// Exercise the system under test.
			truncateWithBlockCleaner(t, node, requestedLength)

			// Verify all properties. The sum of all block sizes must match the requested size and must also match the node
			// size. All blocks that remain are in clean or dirty state (in other words, they need to have been loaded). The
			// remaining content is a prefix of the original content. All blocks but the last should be the same size as the
			// block size. (In a separate oracle test, in case of an error loading, the truncate operation fails.)

			assert.Equal(t, uint64(requestedLength), node.D.Length)

			// Prepare a buffer larger than needed to see if we have more bytes than expected.
			truncatedContent := make([]byte, requestedLength+10)
			n := node.read(truncatedContent, 0)
			assert.Equal(t, requestedLength, n)
			truncatedContent = truncatedContent[:n]
			assert.True(t, bytes.HasPrefix(initialContent, truncatedContent))

			assertComputedSizeEquals(t, node, blockSize, requestedLength, len(initialContent))
		})
	}
}

func newNodeWithInitialContentAndRandomStateBlocks(t *testing.T, p []byte) *Node {
	node := &Node{}
	require.Nil(t, node.writeAt(nil, p, 0))
	for i, b := range node.blocks {
		b.state = blockState(rand.Intn(3))
		t.Logf("Setting block state %v at index %d", b.state, i)
	}
	return node
}

func truncateWithBlockCleaner(t *testing.T, node *Node, requestedLength int) {
	require.Nil(t, node.truncate(func(block *Block) error {
		if block.state == blockNotLoaded {
			block.state = blockClean
		}
		return nil
	}, uint64(requestedLength)))
}

func assertComputedSizeEquals(t *testing.T, node *Node, nodeBlockSize int, requestedLength int, initialLength int) {
	var computedSize int
	for i, block := range node.blocks {
		// If the request was out of bounds, no point in loading any blocks.
		if requestedLength != initialLength {
			assert.NotEqual(t, blockNotLoaded, block.state)
		}
		if i < len(node.blocks)-1 {
			assert.Equal(t, nodeBlockSize, len(block.contents))
		} else {
			assert.True(t, len(block.contents) <= nodeBlockSize)
		}
		computedSize += len(block.contents)
	}
	assert.Equal(t, requestedLength, computedSize)
}

func overrideBlockSize(size int) (restore func()) {
	prev := blockSizeBytes
	blockSizeBytes = size
	return func() {
		blockSizeBytes = prev
	}
}
