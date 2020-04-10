package tree

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureBlocksForWriting(t *testing.T) {
	bf := blockFactory(t, nil)
	t.Run("no blocks required", func(t *testing.T) {
		n := &Node{}
		err := n.ensureBlocksForWriting(0)
		if err != nil {
			t.Error(err)
		}
		if got := len(n.blocks); got != 0 {
			t.Errorf("got %d, want 0 blocks", got)
		}
	})
	t.Run("one partial block required", func(t *testing.T) {
		n := &Node{blockFactory: bf}
		err := n.ensureBlocksForWriting(1)
		if err != nil {
			t.Error(err)
		}
		if got := len(n.blocks); got != 1 {
			t.Errorf("got %d, want 1 block", got)
		}
	})
	t.Run("one full block required", func(t *testing.T) {
		n := &Node{blockFactory: bf}
		err := n.ensureBlocksForWriting(int64(DefaultBlockCapacity))
		if err != nil {
			t.Error(err)
		}
		if got := len(n.blocks); got != 1 {
			t.Errorf("got %d, want 1 block", got)
		}
	})
	t.Run("one full block and a byte required", func(t *testing.T) {
		n := &Node{blockFactory: bf}
		err := n.ensureBlocksForWriting(int64(DefaultBlockCapacity + 1))
		if err != nil {
			t.Error(err)
		}
		if got := len(n.blocks); got != 2 {
			t.Errorf("got %d, want 2 blocks", got)
		}
	})
}

func TestNodeRead(t *testing.T) {
	const testBlockSizeBytes = 5
	key := make([]byte, 16)
	rand.Read(key)
	blockFactory, err := block.NewFactory(nil, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	newAlphabetBlock := func() *block.Block {
		block, err := blockFactory.New(nil, testBlockSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		n, increase, err := block.Write([]byte("abcde"), 0)
		if err != nil {
			t.Fatal(err)
		}
		if want := 5; n != want {
			t.Fatalf("got %d, want %d bytes written", n, want)
		}
		if want := 5; increase != want {
			t.Fatalf("got %d, want %d-byte size increase", increase, want)
		}
		return block
	}
	readString := func(node *Node, off int64, count int) string {
		p := make([]byte, count)
		n, err := node.ReadAt(p, off)
		if err != nil {
			t.Fatal(err)
		}
		return string(p[:n])
	}
	t.Run("from empty node", func(t *testing.T) {
		node := new(Node)
		p := make([]byte, 5)
		for i := int64(0); i < 10; i++ {
			n, err := node.ReadAt(p, i)
			if err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Errorf("got %d, want 0 bytes read", n)
			}
		}
		if !bytes.Equal([]byte{0, 0, 0, 0, 0}, p) {
			t.Errorf("got %q, expected 5 null bytes", p)
		}
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
		block.Truncate(2)
		n.blocks = append(n.blocks, block)
		assert.Equal(t, "", readString(n, 2, 3))
		assert.Equal(t, "", readString(n, 3, 3))
	})
}

func TestTruncateDirPrevented(t *testing.T) {
	n := &Node{}
	n.D.Mode = p.DMDIR
	assert.NotNil(t, n.Truncate(42))
}

func TestTruncateExtendEmptyNode(t *testing.T) {
	bf := blockFactory(t, nil)
	n := &Node{blockFactory: bf}
	require.Nil(t, n.Truncate(42))
	assert.Equal(t, uint64(42), n.D.Length)
	assert.Len(t, n.blocks, 1)
	size, err := n.blocks[0].Size()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := size, 42; got != want {
		t.Errorf("got %d, want %d-byte block", got, want)
	}
}

func TestTruncateExtends(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		t.Run("", func(t *testing.T) {
			// Get random test parameters.
			blockSize := 5 + rand.Intn(16) // 5..20
			initialContentSize := rand.Intn(80)
			initialContent := make([]byte, initialContentSize)
			rand.Read(initialContent)
			// Always extend with zeros for this test.
			requestedLength := len(initialContent) + rand.Intn(len(initialContent)+1)
			defer overrideBlockSize(blockSize)()
			t.Logf("The block size is %d and the content size is %d, and we extend it to %d.",
				blockSize, len(initialContent), requestedLength)

			// Load the initial content into a new node, with a nil loader, because no blocks will need to be loaded at this
			// stage. Will also randomize each block state - the fake loader will simply set the state to clean or return an
			// error. This is just to prepare the node for the truncate operation.
			node := newNodeWithInitialContent(t, initialContent)

			// Exercise the system under test.
			require.Nil(t, node.Truncate(uint64(requestedLength)))

			// Verify all properties. The sum of all block sizes must match the requested size and must also match the node
			// size. All blocks that remain are in clean or dirty state (in other words, they need to have been loaded). The
			// remaining content is a prefix of the original content. All blocks but the last should be the same size as the
			// block size. (In a separate oracle test, in case of an error loading, the truncate operation fails.)
			assert.Equal(t, uint64(requestedLength), node.D.Length)

			// Prepare a buffer larger than needed to see if we have more bytes than expected.
			extendedContent := make([]byte, requestedLength+10)
			t.Logf("node length: %d", node.D.Length)
			n, err := node.ReadAt(extendedContent, 0)
			if err != nil {
				t.Fatal(err)
			}
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
			node := newNodeWithInitialContent(t, initialContent)

			// Exercise the system under test.
			require.Nil(t, node.Truncate(uint64(requestedLength)))

			// Verify all properties. The sum of all block sizes must match the requested size and must also match the node
			// size. All blocks that remain are in clean or dirty state (in other words, they need to have been loaded). The
			// remaining content is a prefix of the original content. All blocks but the last should be the same size as the
			// block size. (In a separate oracle test, in case of an error loading, the truncate operation fails.)

			assert.Equal(t, uint64(requestedLength), node.D.Length)

			// Prepare a buffer larger than needed to see if we have more bytes than expected.
			truncatedContent := make([]byte, requestedLength+10)
			n, err := node.ReadAt(truncatedContent, 0)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, requestedLength, n)
			truncatedContent = truncatedContent[:n]
			assert.True(t, bytes.HasPrefix(initialContent, truncatedContent))

			assertComputedSizeEquals(t, node, blockSize, requestedLength, len(initialContent))
		})
	}
}

func newNodeWithInitialContent(t *testing.T, p []byte) *Node {
	t.Helper()
	bf := blockFactory(t, nil)
	node := &Node{
		blockFactory: bf,
	}
	require.Nil(t, node.WriteAt(p, 0))
	return node
}

func assertComputedSizeEquals(t *testing.T, node *Node, nodeBlockSize int, requestedLength int, initialLength int) {
	t.Helper()
	var computedSize int
	for i, block := range node.blocks {
		size, err := block.Size()
		if err != nil {
			t.Fatal(err)
		}
		if i < len(node.blocks)-1 {
			if got, want := size, nodeBlockSize; got != want {
				t.Errorf("got %d, want a %d-byte block", got, want)
			}
		} else {
			assert.True(t, size <= nodeBlockSize)
		}
		computedSize += size
	}
	assert.Equal(t, requestedLength, computedSize)
}

func overrideBlockSize(size int) (restore func()) {
	prev := DefaultBlockCapacity
	DefaultBlockCapacity = size
	return func() {
		DefaultBlockCapacity = prev
	}
}
