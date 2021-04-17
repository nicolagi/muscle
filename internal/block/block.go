package block

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nicolagi/muscle/internal/storage"
)

type state uint8

const (
	primed state = iota
	clean
	dirty
)

type location uint8

const (
	index location = iota
	repository
)

type Block struct {
	capacity int

	// In primed state, the value is nil and the ref is non-nil, the value can be
	// loaded from storage. In clean state, the value is non-nil and corresponds to
	// what's stored (according to location and ref). In dirty state, the value is
	// non-nil and does not correspond to anything stored in neither the index nor
	// the repository.
	state state

	// Where the block is or will be stored.
	// It must be index for dirty state.
	location location

	ref   Ref
	value []byte

	cipher     blockCipher
	index      storage.Store
	repository storage.Store

	// When was the block last used?
	atime time.Time
}

// TODO: panic if block is dirty.
func (block *Block) Ref() Ref {
	return block.ref
}

func (block *Block) Size() (n int, err error) {
	block.atime = time.Now()
	if err := block.ensureReadable(); err != nil {
		return 0, fmt.Errorf("block.Block.Size: %w", err)
	}
	return len(block.value), nil
}

func (block *Block) Read(p []byte, off int) (n int, err error) {
	block.atime = time.Now()
	if err := block.ensureReadable(); err != nil {
		return 0, fmt.Errorf("block.Block.Read: %w", err)
	}
	if off >= len(block.value) {
		return 0, nil
	}
	return copy(p, block.value[off:]), nil
}

// ReadAll returns a copy of the content of the block.
func (block *Block) ReadAll() ([]byte, error) {
	block.atime = time.Now()
	if err := block.ensureReadable(); err != nil {
		return nil, err
	}
	dup := make([]byte, len(block.value))
	copy(dup, block.value)
	return dup, nil
}

// Truncate shrinks or grows a block up to the block capacity.
// If the requested size exceeds the capacity, an error is returned.
func (block *Block) Truncate(size int) error {
	block.atime = time.Now()
	if size > block.capacity {
		return fmt.Errorf("block.Block.Truncate: requested %d bytes with capacity %d", size, block.capacity)
	}
	if err := block.ensureWritable(); err != nil {
		return fmt.Errorf("block.Block.Truncate: %w", err)
	}
	if size <= len(block.value) {
		block.value = block.value[:size]
		block.state = dirty
		return nil
	}
	block.value = append(block.value, make([]byte, size-len(block.value))...)
	block.state = dirty
	return nil
}

func (block *Block) Write(p []byte, off int) (n int, sizeIncrease int, err error) {
	block.atime = time.Now()
	if len(p) == 0 {
		return 0, 0, nil
	}
	if err := block.ensureWritable(); err != nil {
		return 0, 0, fmt.Errorf("block.Block.Write: %w", err)
	}
	max := block.capacity - off
	if len(p) > max {
		p = p[:max]
	}
	before := len(block.value)
	if padding := off - before; padding > 0 {
		block.value = append(block.value, make([]byte, padding)...)
	}
	copied := copy(block.value[off:], p)
	if copied < len(p) {
		block.value = append(block.value, p[copied:]...)
	}
	block.state = dirty
	return len(p), len(block.value) - before, nil
}

// Flush ensures the block is synced to disk.
// Returns whether the block needed flushing or not, or an error.
func (block *Block) Flush() (flushed bool, err error) {
	if block.state != dirty {
		return false, nil
	}
	if block.location != index {
		panic("block.Block.Flush: dirty but not backed by index")
	}
	if err := block.flush(); err != nil {
		return false, fmt.Errorf("block.Block.Flush: %w", err)
	}
	return true, nil
}

// Pre-condition: the block is dirty and backed by the index.
// Post-condition: the block is clean and backed by the index, or an error is returned.
func (block *Block) flush() error {
	ciphertext, err := block.cipher.encrypt(block.value)
	if err != nil {
		return fmt.Errorf("block.Block.flush: %w", err)
	}
	err = block.index.Put(block.ref.Key(), ciphertext)
	if err != nil {
		return fmt.Errorf("block.Block.flush: %w", err)
	}
	block.state = clean
	return nil
}

// Seal ensures a read-only version of the block is written to the repository.
func (block *Block) Seal() (sealed bool, err error) {
	block.atime = time.Now()
	if block.location == repository && (block.state == primed || block.state == clean) {
		return false, nil
	}
	if err := block.ensureReadable(); err != nil {
		return false, fmt.Errorf("block.Block.Seal: %w", err)
	}
	if err := block.seal(); err != nil {
		return false, fmt.Errorf("block.Block.Seal: %w", err)
	}
	return true, nil
}

// Pre-condition: block state is clean or dirty, backed by index.
// Post-condition: block state is clean, backed by repository.
func (block *Block) seal() error {
	ref := RefOf(block.value)
	ciphertext, err := block.cipher.encrypt(block.value)
	if err != nil {
		return fmt.Errorf("block.Block.seal: %w", err)
	}
	if err := block.repository.Put(ref.Key(), ciphertext); err != nil {
		return fmt.Errorf("block.Block.seal: %w", err)
	}
	if err := block.index.Delete(block.ref.Key()); err != nil && !errors.Is(err, storage.ErrNotFound) {
		log.Printf("block.Block.seal: left garbage behind: %v", err)
	}
	block.ref = ref
	block.state = clean
	block.location = repository
	return nil
}

// Forget nils out the block's value byte slice if possible, so memory can be reclaimed.
// Unused at the moment, tree.Node.Trim nils out the whole slice of blocks.
func (block *Block) Forget() (forgotten bool) {
	if block.state != clean {
		return false
	}
	if time.Since(block.atime) < time.Minute {
		return false
	}
	block.forget()
	return true
}

// Pre-condition: block state is clean.
// Post-condition: block state is primed.
func (block *Block) forget() {
	block.state = primed
	block.value = nil
}

// Discard nils out the block value and tries to remove the block from the index,
// if it's backed by the index. The block should not be used for anything after
// this method is called.
func (block *Block) Discard() {
	block.value = nil
	if block.location == index {
		if err := block.index.Delete(block.ref.Key()); err != nil && !errors.Is(err, storage.ErrNotFound) {
			log.Printf("block.Block.Discard left garbage behind: %v", err)
		}
	}
}

// SameValue compares block values.
// It will load values from the index/repository if required.
// Works with dirty blocks too (will be useful when moving merge from the muscle
// CLI to musclefs control node). (The CLI merge is dangerous because it uses
// the root key file, which is not the same as the mounted file system root
// unless it has just been flushed. Always flush before merge.)
func (block *Block) SameValue(other *Block) (same bool, err error) {
	var hash1, hash2 RepositoryRef
	hash1, err = block.valueHash()
	if err != nil {
		return false, fmt.Errorf("block.Block.SameValue: %w", err)
	}
	hash2, err = other.valueHash()
	if err != nil {
		return false, fmt.Errorf("block.Block.SameValue: %w", err)
	}
	return hash1 == hash2, nil
}

func (block *Block) valueHash() (ref RepositoryRef, err error) {
	if block.location == repository {
		if block.state == dirty {
			panic("block.Block.valueHash: dirty block backed by repository")
		}
		return block.ref.(RepositoryRef), nil
	}
	if err := block.ensureReadable(); err != nil {
		return ref, fmt.Errorf("block.Block.valueHash: %w", err)
	}
	return RefOf(block.value), nil
}

func (block *Block) ensureReadable() error {
	if block.state != primed {
		return nil
	}
	return block.load()
}

// Pre-condition: block is primed.
// Post-condition: block is clean.
func (block *Block) load() (err error) {
	const method = "Block.load"
	var ciphertext []byte
	switch block.location {
	case index:
		ciphertext, err = block.index.Get(block.ref.Key())
	case repository:
		ciphertext, err = block.repository.Get(block.ref.Key())
	default:
		panic("block.Block.load: unknown location")
	}
	if err != nil {
		return errorv(method, err)
	}
	if l, min := len(ciphertext), block.cipher.BlockSize(); l < min {
		return errorf(method, "%v is %d bytes long; need at least %d bytes", block.ref.Key(), l, min)
	}
	block.value = block.cipher.decrypt(ciphertext)
	block.state = clean
	return nil
}

func (block *Block) ensureWritable() error {
	if err := block.ensureReadable(); err != nil {
		return fmt.Errorf("block.Block.ensureWritable: %w", err)
	}
	// Possible states at this line: Iclean, Rclean, Idirty.
	if block.state == clean && block.location == repository {
		ref, err := NewRef(nil)
		if err != nil {
			return fmt.Errorf("block.Block.ensureWritable: %w", err)
		}
		block.ref = ref
		block.state = dirty
		block.location = index
	}
	return nil
}
