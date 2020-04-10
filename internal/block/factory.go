package block

import (
	"fmt"

	"github.com/nicolagi/muscle/storage"
)

type Factory struct {
	cipher     blockCipher
	index      storage.Store
	repository storage.Store
}

// NewFactory creates a factory that creates blocks sharing the given cipher,
// index, and repository.
func NewFactory(index storage.Store, repository storage.Store, key []byte) (*Factory, error) {
	cipher, err := newBlockCipher(key)
	if err != nil {
		return nil, err
	}
	return &Factory{
		cipher:     cipher,
		index:      index,
		repository: repository,
	}, nil
}

func (factory *Factory) New(ref Ref, capacity int) (*Block, error) {
	block := &Block{
		capacity:   capacity,
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
