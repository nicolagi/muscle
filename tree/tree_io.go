package tree

import (
	"errors"

	"github.com/nicolagi/muscle/storage"
	log "github.com/sirupsen/logrus"
)

func (tree *Tree) Truncate(node *Node, size uint64) error {
	if err := node.truncate(tree.store.LoadBlock, size); err != nil {
		return err
	}
	node.markDirty()
	return nil
}

func (node *Node) truncate(load func(*Block) error, requestedSize uint64) error {
	if node.IsDir() {
		return errors.New("impossible to truncate a directory")
	}
	if requestedSize == node.D.Length {
		return nil
	}
	node.markDirty()
	return node.actualTruncate(load, requestedSize)
}

func (node *Node) actualTruncate(load func(*Block) error, requestedLength uint64) error {
	var newBlocks []*Block
	var newLength uint64

	for _, b := range node.blocks {
		if newLength == requestedLength {
			break
		}
		// Have to make sure the node was loaded because that's the only way to know its size.
		if b.state == blockNotLoaded {
			if err := load(b); err != nil {
				return err
			}
		}
		if newLength+uint64(len(b.contents)) > requestedLength {
			newBlockLength := requestedLength - newLength
			b.contents = b.contents[:newBlockLength]
		}
		newBlocks = append(newBlocks, b)
		newLength += uint64(len(b.contents))
	}

	if incr := requestedLength - newLength; incr > 0 && len(node.blocks) > 0 {
		newLength += node.blocks[len(node.blocks)-1].expand(incr)
	}

	for newLength < requestedLength {
		block := &Block{state: blockDirty}
		newLength += block.expand(requestedLength - newLength)
		newBlocks = append(newBlocks, block)
	}

	node.blocks = newBlocks
	node.D.Length = newLength
	node.updateMTime()
	return nil
}

// TODO This needs the tree only because it needs the store...
func (tree *Tree) WriteAt(node *Node, p []byte, off int64) error {
	return node.writeAt(tree.store, p, off)
}

func (node *Node) writeAt(store BlockLoader, p []byte, off int64) error {
	err := node.ensureBlocksForReading(store, int64(off), len(p))
	if err != nil {
		return err
	}
	node.ensureBlocksForWriting(off + int64(len(p)))
	node.write(p, off)
	node.updateMTime()
	node.markDirty() // Is this needed? Was this lost in some refactoring?
	return nil
}

// TODO This needs the tree only because it needs the store...
func (tree *Tree) ReadAt(node *Node, p []byte, off int64) (int, error) {
	if err := node.ensureBlocksForReading(tree.store, off, len(p)); err != nil {
		return 0, err
	}
	return node.read(p, off), nil
}

func (node *Node) ensureBlocksForWriting(requiredBytes int64) {
	bs := int64(blockSizeBytes)
	requiredBlocks := int(requiredBytes / bs)
	if requiredBytes%bs != 0 {
		requiredBlocks++
	}
	for len(node.blocks) < requiredBlocks {
		node.blocks = append(node.blocks, &Block{state: blockDirty})
	}
}

type BlockLoader interface {
	// LoadBlock should fill in the contents of the block.
	// It should use the block's key to locate its contents, so that must be
	// correctly populated before loading the block.
	LoadBlock(*Block) error
}

func blockRange(byteOff int64, byteCount int) (blockOff int, blockCount int) {
	blockOff = int(byteOff / int64(blockSizeBytes))
	if byteCount > 0 {
		blockCount++
		byteCount -= blockSizeBytes - int(byteOff%int64(blockSizeBytes))
	}
	for byteCount > 0 {
		blockCount++
		byteCount -= blockSizeBytes
	}
	return
}

func (node *Node) ensureBlocksForReading(store BlockLoader, off int64, count int) error {
	blockOff, blockCount := blockRange(off, count)
	for i := blockOff; i < blockOff+blockCount; i++ {
		if i >= len(node.blocks) {
			break
		}
		block := node.blocks[i]
		if block.state != blockNotLoaded {
			continue
		}
		if err := store.LoadBlock(block); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				log.WithFields(log.Fields{
					"key": block.pointer.Hex(),
				}).Error("Block not found in store, assuming empty")
			} else {
				return err
			}
		}
		block.state = blockClean
	}
	return nil
}

func (node *Node) getBlock(off int64) *Block {
	index := int(off / int64(blockSizeBytes))
	if index >= len(node.blocks) {
		return nil
	}
	return node.blocks[index]
}

func (node *Node) write(p []byte, off int64) {
	if len(p) == 0 {
		return
	}
	bs := int64(blockSizeBytes)
	written, delta := node.getBlock(off).write(p, int(off%bs))
	off -= off % bs
	off += bs
	node.D.Length += uint64(delta)
	node.write(p[written:], off)
}

func (node *Node) read(p []byte, off int64) int {
	if len(p) == 0 {
		return 0
	}
	block := node.getBlock(off)
	if block == nil {
		return 0
	}
	o := int(off % int64(blockSizeBytes))
	if o >= len(block.contents) {
		return 0
	}
	n := copy(p, block.contents[o:])
	return n + node.read(p[n:], off+int64(n))
}
