package block

import (
	"fmt"

	"github.com/nicolagi/muscle/storage"
)

type Factory struct {
	cipher     blockCipher
	index      storage.Store
	repository storage.Store

	// Capacity in bytes of each block this factory makes.
	capacity int
}

// NewFactory creates a factory that creates blocks sharing the given cipher,
// index, repository, and all of the given capacity.
func NewFactory(index storage.Store, repository storage.Store, key []byte, capacity int) (*Factory, error) {
	cipher, err := newBlockCipher(key)
	if err != nil {
		return nil, err
	}
	return &Factory{
		cipher:     cipher,
		index:      index,
		repository: repository,
		capacity:   capacity,
	}, nil
}

func (factory *Factory) New(ref Ref) (*Block, error) {
	block := &Block{
		capacity:   factory.capacity,
		cipher:     factory.cipher,
		index:      factory.index,
		repository: factory.repository,
	}
	switch ref.(type) {
	case nil:
		var err error
		block.ref, err = NewRef(nil)
		if err != nil {
			return nil, err
		}
		block.state = dirty
		block.location = index
	case IndexRef:
		block.ref = ref
		block.state = primed
		block.location = index
	case RepositoryRef:
		block.ref = ref
		block.state = primed
		block.location = repository
	default:
		return nil, fmt.Errorf("unhandled ref type: %v", ref)
	}
	return block, nil
}
