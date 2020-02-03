package tree

import (
	"errors"
	"fmt"

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

type factoryOption func(*Tree) error

var ErrOptionClash = errors.New("option clash")

func (*Factory) WithInstance(value string) factoryOption {
	return func(t *Tree) error {
		if t.instance != "" {
			return fmt.Errorf("instance: %w", ErrOptionClash)
		}
		t.instance = value
		return nil
	}
}

func (*Factory) Mutable() factoryOption {
	return func(t *Tree) error {
		t.readOnly = false
		return nil
	}
}

func (f *Factory) WithRevisionKey(value storage.Pointer) factoryOption {
	return func(t *Tree) error {
		if t.instance != "" {
			return fmt.Errorf("instance: %w", ErrOptionClash)
		}
		if !t.revision.IsNull() {
			return fmt.Errorf("revision: %w", ErrOptionClash)
		}
		if t.root != nil {
			return fmt.Errorf("root: %w", ErrOptionClash)
		}
		r := Revision{key: value}
		if err := f.store.LoadRevision(&r); err != nil {
			return err
		}
		t.instance = r.instance
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
		t.root = &Node{}
		t.root.D.Name = "root"
		t.root.D.Mode = 0700 | p.DMDIR
		t.root.D.Qid.Type = p.QTDIR
		t.root.dirty = true
		t.Add(t.root, "ctl", 0600)
	}
	// Fix mode for roots created before mode was used...
	t.root.D.Mode |= 0700 | p.DMDIR
	// TODO when does it exit?
	go t.trimPeriodically()
	return t, nil
}
