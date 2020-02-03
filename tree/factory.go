package tree

import (
	"github.com/nicolagi/go9p/p"
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

func (f *Factory) NewTreeForInstanceFromRoot(instance string, key storage.Pointer) (*Tree, error) {
	t, err := f.NewTreeFromRoot(key, false)
	if err != nil {
		return nil, err
	}
	t.instance = instance
	return t, nil
}

func (f *Factory) NewTreeReadOnlyFromRevisionRoot(r *Revision) (*Tree, error) {
	return f.NewTreeFromRoot(r.rootKey, true)
}

func (f *Factory) NewTreeFromRoot(rootKey storage.Pointer, readOnly bool) (*Tree, error) {
	tree := &Tree{
		store:    f.store,
		readOnly: readOnly,
	}
	if rootKey == nil {
		tree.root = &Node{
			D: p.Dir{
				Name: "root",
				Mode: 0700 | p.DMDIR,
				Qid: p.Qid{
					Type: p.QTDIR,
				},
			},
			dirty: true,
		}
		tree.Add(tree.root, "ctl", 0600)
		return tree, nil
	}
	tree.root = &Node{pointer: rootKey}
	err := tree.store.LoadNode(tree.root)
	if err != nil {
		return nil, err
	}
	// Fix mode for roots created before mode was used...
	tree.root.D.Mode |= 0700 | p.DMDIR
	// TODO when does it exit?
	go tree.trimPeriodically()
	return tree, err
}
