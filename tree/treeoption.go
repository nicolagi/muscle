package tree

import (
	"github.com/nicolagi/muscle/storage"
	"github.com/pkg/errors"
)

// TreeOption values influence the behavior of NewTree.
type TreeOption func(*Tree) error

// The WithMutable option specifies that the tree to be constructed
// should allow mutating operations, like writing data, changing file
// names, and adding new nodes. The block size (in bytes) is the size
// of data blocks for all new nodes added to the tree.
func WithMutable(blockSize uint32) TreeOption {
	return func(t *Tree) error {
		t.readOnly = false
		t.blockSize = blockSize
		return nil
	}
}

// WithRevision specifies that the tree's root node should be the
// revision's root node.
func WithRevision(p storage.Pointer) TreeOption {
	return func(t *Tree) error {
		if !t.revision.IsNull() {
			return errors.Wrapf(ErrPhase, "tree already has a revision")
		}
		if t.root != nil {
			return errors.Wrapf(ErrPhase, "tree already has a root node")
		}
		r, err := t.store.LoadRevisionByKey(p)
		if err != nil {
			return err
		}
		t.revision = r.key
		t.root = &Node{pointer: r.rootKey}
		if err := t.store.LoadNode(t.root); err != nil {
			return err
		}
		return nil
	}
}

// WithRoot specifies the tree's root node.
func WithRoot(p storage.Pointer) TreeOption {
	return func(t *Tree) error {
		if t.root != nil {
			return errors.Wrapf(ErrPhase, "tree already has a root node")
		}
		if p.IsNull() {
			return nil
		}
		t.root = &Node{pointer: p}
		return t.store.LoadNode(t.root)
	}
}
