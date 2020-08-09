package tree

import (
	"errors"
	"fmt"

	"github.com/lionkov/go9p/p"
	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
)

// Factory creates trees that share an underlying store.
type Factory struct {
	blockFactory *block.Factory
	store        *Store
	blockSize    uint32
}

// NewFactory returns a *Factory that creates trees that share the
// given store. For instance, you could get the tree representing
// the local filesystem and one representing the remote one; or many
// trees representing a number of past snapshots of the filesystem.
func NewFactory(blockFactory *block.Factory, store *Store, c *config.C) *Factory {
	return &Factory{
		blockFactory: blockFactory,
		store:        store,
		blockSize:    c.BlockSize,
	}
}

type factoryOption func(*Tree) error

var ErrOptionClash = errors.New("option clash")

func (*Factory) Mutable() factoryOption {
	return func(t *Tree) error {
		t.readOnly = false
		return nil
	}
}

func (f *Factory) WithRevisionKey(value storage.Pointer) factoryOption {
	return func(t *Tree) error {
		if !t.revision.IsNull() {
			return fmt.Errorf("revision: %w", ErrOptionClash)
		}
		if t.root != nil {
			return fmt.Errorf("root: %w", ErrOptionClash)
		}
		r, err := f.store.LoadRevisionByKey(value)
		if err != nil {
			return err
		}
		t.revision = r.key
		t.root = &Node{pointer: r.rootKey}
		if err := f.store.LoadNode(t.root); err != nil {
			return err
		}
		return nil
	}
}

func (f *Factory) WithRootKey(value storage.Pointer) factoryOption {
	return func(t *Tree) error {
		if t.root != nil {
			return fmt.Errorf("root: %w", ErrOptionClash)
		}
		if value.IsNull() {
			return nil
		}
		t.root = &Node{pointer: value}
		return f.store.LoadNode(t.root)
	}
}

func (f *Factory) NewTree(options ...factoryOption) (*Tree, error) {
	t := &Tree{store: f.store, readOnly: true}
	for _, o := range options {
		if err := o(t); err != nil {
			return nil, err
		}
	}
	if t.root == nil {
		parent := &Node{blockFactory: f.blockFactory}
		root, err := t.Add(parent, "root", 0700|p.DMDIR)
		if err != nil {
			return nil, err
		}
		t.root = root
		// Clear out the fake parent, only introduced to re-use the logic in tree.Add.
		t.root.parent = nil
	}
	// Fix mode for roots created before mode was used...
	t.root.D.Mode |= 0700 | p.DMDIR
	if !t.readOnly {
		t.blockSize = f.blockSize
	}
	// TODO when does it exit?
	go t.trimPeriodically()
	return t, nil
}
