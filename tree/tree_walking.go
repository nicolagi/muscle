package tree

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/nicolagi/muscle/storage"
	log "github.com/sirupsen/logrus"
)

var ErrNotFound = errors.New("node not found")

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
			log.Error(err.Error())
			break
		}
		var found bool
		if n, found = n.followBranch(name); !found {
			err = fmt.Errorf("child %q: %w", name, ErrNotFound)
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

func (tree *Tree) grow(parent *Node, loadFn func(*Node) error) error {
	seen := make(map[string]struct{})
	inflight := 0
	childc := make(chan *Node)
	errc := make(chan error)
	for _, child := range parent.children {
		if child.isLoaded() {
			seen[child.D.Name] = struct{}{}
			continue
		}
		inflight++
		go load(errc, childc, loadFn, parent, child)
	}
	// Do not bother starting a goroutine if we already know there's nothing to wait for.
	if inflight == 0 {
		return nil
	}
	var firsterr error
	for inflight > 0 {
		select {
		case err := <-errc:
			if firsterr == nil {
				firsterr = err
			}
		case c := <-childc:
			if _, ok := seen[c.D.Name]; ok {
				randomRename(c)
				c.markDirty()
			} else {
				seen[c.D.Name] = struct{}{}
			}
		}
		inflight--
	}

	return firsterr
}

func load(errc chan error, childc chan *Node, loadFn func(*Node) error, parent, child *Node) {
	if err := loadFn(child); errors.Is(err, storage.ErrNotFound) {
		log.WithFields(log.Fields{
			"key": child.pointer.Hex(),
		}).Error("Child not found in storage")
		child.D.Name = "vanished"
		randomRename(child)
		child.markDirty()
		childc <- child
	} else if err == errNoCodec {
		child.D.Name = "nocodec"
		randomRename(child)
		child.markDirty()
		childc <- child
	} else if err != nil {
		errc <- fmt.Errorf("interrupted growth of node %q at child %q: %v",
			parent.D.Name, child.D.Name, err)
	} else {
		childc <- child
	}
}

func randomRename(node *Node) {
	node.D.Name += "." + uuid.New().String()
}
