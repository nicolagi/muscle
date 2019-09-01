package tree

import (
	"github.com/nicolagi/muscle/storage"
)

// Factory creates trees that share an underlying store.
type Factory struct {
	store *Store
}

// NewFactory returns a *Factory that creates trees that share the
// given store. For instance, you could get the tree representing
// the local filesystem and one representing the remote one; or many
// trees representing a number of past snapshots of the filesystem.
func NewFactory(store *Store) *Factory {
	return &Factory{
		store: store,
	}
}

// NewTree returns the *Tree corresponding to the given revision.
// If read-only, some operations will be disabled or will return errors.
func (f *Factory) NewTree(revisionKey storage.Pointer, readOnly bool) (*Tree, error) {
	return newTree(f.store, revisionKey, readOnly)
}

func (f *Factory) NewTreeForInstance(instance string, revision storage.Pointer) (*Tree, error) {
	t, err := f.NewTree(revision, false)
	if err != nil {
		return nil, err
	}
	t.instance = instance
	return t, nil
}
