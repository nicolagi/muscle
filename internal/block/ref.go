package block

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"

	"github.com/nicolagi/muscle/storage"
)

const (
	indexRefLen      = 16
	repositoryRefLen = 32
)

type Ref interface {
	fmt.Stringer

	// Len returns the length in number of bytes.
	Len() int

	// Bytes returns a copy of the underlying byte slice.
	Bytes() []byte

	// Key is used to store/load the reference to/from a storage.Store.
	Key() storage.Key
}

func NewRef(p []byte) (Ref, error) {
	switch l := len(p); l {
	case 0:
		var ref IndexRef
		if n, err := rand.Read(ref[:]); err != nil {
			return nil, fmt.Errorf("block.NewRef: %w", err)
		} else if n != indexRefLen {
			return nil, fmt.Errorf("block.NewRef: got %d, want %d random bytes", n, indexRefLen)
		}
		return ref, nil
	case indexRefLen:
		var ref IndexRef
		copy(ref[:], p)
		return ref, nil
	case repositoryRefLen:
		var ref RepositoryRef
		copy(ref[:], p)
		return ref, nil
	default:
		return nil, fmt.Errorf("block.NewRef: unhandled ref length: %v", l)
	}
}

type IndexRef [indexRefLen]byte

func (ref IndexRef) Len() int {
	return indexRefLen
}

func (ref IndexRef) Bytes() []byte {
	return ref[:]
}

func (ref IndexRef) Key() storage.Key {
	return storage.Key(fmt.Sprintf("%x", ref[:]))
}

func (ref IndexRef) String() string {
	return fmt.Sprintf("%x", ref[:])
}

type RepositoryRef [repositoryRefLen]byte

func RefOf(value []byte) RepositoryRef {
	return sha256.Sum256(value)
}

func (ref RepositoryRef) Len() int {
	return repositoryRefLen
}

func (ref RepositoryRef) Bytes() []byte {
	return ref[:]
}

func (ref RepositoryRef) Key() storage.Key {
	return storage.Key(fmt.Sprintf("%x", ref[:]))
}

func (ref RepositoryRef) String() string {
	return fmt.Sprintf("%x", ref[:])
}
