package tree

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Walk navigates the tree starting from the given node following the given branches in sequence.
func (tree *Tree) Walk(sourceNode *Node, branchNames ...string) (visitedNodes []*Node, err error) {
	return tree.walk(sourceNode, tree.Grow, branchNames...)
}

func (tree *Tree) walk(sourceNode *Node, growFn func(*Node) error, branchNames ...string) (visitedNodes []*Node, err error) {
	if sourceNode == nil {
		err = fmt.Errorf("cannot walk tree from nil node")
		return
	}
	n := sourceNode
	for _, name := range branchNames {
		if err = growFn(n); err != nil {
			break
		}
		if n, err = n.followBranch(name); err != nil {
			break
		} else if n == nil {
			err = fmt.Errorf("child %q: %w", name, ErrNotExist)
			break
		}
		visitedNodes = append(visitedNodes, n)
	}
	return
}

// Grow expands the tree at node (if necessary), by loading child nodes (where necessary).
// This method protects against the case where the node has more than one child with any given name,
// by adding a random UUID extension to the duplicates.
// (Unfortunately this type of inconsistency has happened during development of the merge operation.)
// This method will also add a random UUID extension to every node that is not found in the storage layer.
// Such nodes will then represent empty files.
func (tree *Tree) Grow(parent *Node) error {
	if parent == nil {
		return errors.New("cannot grow tree at nil node")
	}
	return tree.grow(parent, tree.store.LoadNode)
}

// TODO: load should take a context for cancellation.
func (tree *Tree) grow(parent *Node, load func(*Node) error) error {
	semc := make(chan struct{}, 32)
	g, _ := errgroup.WithContext(context.Background())
	for _, child := range parent.children {
		if child.flags&loaded != 0 {
			continue
		}
		child := child
		g.Go(func() error {
			semc <- struct{}{}
			defer func() { <-semc }()
			if err := load(child); err != nil {
				return errors.Wrapf(err, "tree.Tree.grow: loading a child of %q", parent.Path())
			}
			return nil
		})
	}
	return g.Wait()
}
