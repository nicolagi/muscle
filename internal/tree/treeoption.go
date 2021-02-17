package tree

import (
	"fmt"

	"github.com/nicolagi/muscle/internal/storage"
	"github.com/pkg/errors"
)

// TreeOption values influence the behavior of NewTree.
type TreeOption func(*Tree) error

// The WithMutable option specifies that the tree to be constructed
// should allow mutating operations, like writing data, changing file
// names, and adding new nodes.
func WithMutable() TreeOption {
	return func(t *Tree) error {
		t.readOnly = false
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
			return fmt.Errorf("tree.WithRevision: loading revision %v: %v", p, err)
		}
		t.revision = r.key
		t.root = &Node{pointer: r.rootKey}
		if err := t.store.LoadNode(t.root); err != nil {
			return fmt.Errorf("tree.WithRevision: loading node %v: %v", r.rootKey, err)
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
		if err := t.store.LoadNode(t.root); err != nil {
			return fmt.Errorf("tree.WithRoot: loading node %v: %v", p, err)
		}
		return nil
	}
}
