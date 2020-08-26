package tree

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/nicolagi/muscle/storage"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var ErrInUse = errors.New("in use")

type Tree struct {
	store *Store

	revision  storage.Pointer
	root      *Node
	blockSize uint32 // For new nodes.

	readOnly bool

	ignored map[string]map[string]struct{}

	lastFlushed time.Time
}

func (tree *Tree) Attach() *Node {
	return tree.root
}

func (tree *Tree) trimPeriodically() {
	for {
		time.Sleep(time.Minute)
		// This, I think, is the only protection against loading large files temporarily.
		// The problem with large files is that they take up a lot of memory and changes the
		// GC target too much. This is the only way to free up that memory.
		tree.root.Trim()
		runtime.GC()
	}
}

func (tree *Tree) Root() (storage.Pointer, *Node) { return tree.revision, tree.root }

func (tree *Tree) Add(node *Node, name string, perm uint32) (*Node, error) {
	child := &Node{
		flags:        loaded | dirty,
		blockFactory: node.blockFactory,
		bsize:        uint32(tree.blockSize),
		parent:       node,
		info: NodeInfo{
			Name: name,
			Mode: perm,
		},
	}
	child.info.ID = uint64(time.Now().UnixNano())
	child.info.Version = 1
	child.touchNow()
	if err := node.addChild(child); err != nil {
		return nil, err
	}
	node.touchNow()
	child.markDirty()
	return child, nil
}

func (tree *Tree) Remove(node *Node) error {
	if node.IsRoot() {
		return errors.Wrapf(ErrPermission, "removing the file system root is not allowed")
	}
	if len(node.children) > 0 {
		// Don't wrap the error, don't add stack trace.
		// We don't want to log it.
		return ErrNotEmpty
	}
	node.parent.removeChild(node.info.Name)
	node.parent.markDirty()
	node.discard()
	return nil
}

// RemoveForMerge unlinks the node from its parent even if it is a non-empty directory.
// This is required when running the 3-way merge algorithm.
// Also, if we don't find the node within the parent, we return an error, as that is an inconsistency.
// If we have to remove more than one node (the parent has more than one child matching the name),
// we do so, and log an error about the inconsistency. We don't return an error as we don't want to
// prevent the merge algorithm from resolving the inconsistency somehow.
// It is an error trying to remove the root (it has no parent).
// The code will panic if the parent of the node is nil. That would be a programming error
// and I don't want to defend against that.
func (tree *Tree) RemoveForMerge(node *Node) error {
	if node.IsRoot() {
		return errors.New("the root cannot be removed")
	}
	if node.refs > 0 {
		node.markUnlinked()
	}
	if removed := node.parent.removeChild(node.info.Name); removed == 0 {
		log.Printf("warning: %q does not contain %q; remove is a no-op", node.parent.Path(), node.info.Name)
	} else if removed > 1 {
		log.Printf("warning: %q contained %d nodes named %q; removed them all", node.parent.Path(), removed, node.info.Name)
	}
	node.parent.touchNow()
	return nil
}

func (tree *Tree) ReachableKeys(accumulator map[string]struct{}) (map[string]struct{}, error) {
	if accumulator == nil {
		accumulator = make(map[string]struct{})
	}
	accumulator[tree.revision.Hex()] = struct{}{}
	err := tree.reachableKeys(tree.root, accumulator)
	return accumulator, err
}

func (tree *Tree) reachableKeys(node *Node, accumulator map[string]struct{}) error {
	if node == nil {
		return nil
	}
	key := node.pointer
	accumulator[key.Hex()] = struct{}{}
	for _, b := range node.blocks {
		accumulator[string(b.Ref().Key())] = struct{}{}
	}
	if err := tree.Grow(node); err != nil {
		return err
	}
	for _, child := range node.children {
		if err := tree.reachableKeys(child, accumulator); err != nil {
			return err
		}
	}
	return nil
}

func (tree *Tree) Flush() error {
	// Make sure it looks like more than 5 minutes have passed.
	tree.lastFlushed = time.Unix(0, 0)
	return tree.FlushIfNotDoneRecently()
}

// Graft is a low-level operation. The child may come from a historical tree.
// The parent from the local tree. We will make the child a child of
// the parent.
func (tree *Tree) Graft(parent *Node, child *Node, childName string) error {
	if e := tree.Grow(parent); e != nil {
		return e
	}
	if node, err := parent.followBranch(childName); err != nil {
		return err
	} else if node != nil {
		if err := tree.RemoveForMerge(node); err != nil {
			return fmt.Errorf("tree.Tree.Graft: parent: %v: %w", parent, err)
		}
	}
	if err := parent.addChild(child); err != nil {
		return err
	}
	child.info.Name = childName
	child.markDirty()
	return nil
}

func (tree *Tree) Rename(source, target string) error {
	sourceWalkNames := strings.Split(source, "/")
	targetWalkNames := strings.Split(target, "/")

	visitedNodes, err := tree.Walk(tree.root, sourceWalkNames...)
	if err != nil {
		return err
	}
	if len(visitedNodes) != len(sourceWalkNames) {
		return fmt.Errorf("incomplete source node walk")
	}
	nodeToMove := visitedNodes[len(visitedNodes)-1]

	visitedNodes, err = tree.Walk(tree.root, targetWalkNames[:len(targetWalkNames)-1]...)
	if err != nil {
		return err
	}
	if len(visitedNodes) != len(targetWalkNames)-1 {
		return fmt.Errorf("incomplete new parent walk")
	}
	newParent := tree.root
	if len(visitedNodes) > 0 {
		newParent = visitedNodes[len(visitedNodes)-1]
	}
	newName := targetWalkNames[len(targetWalkNames)-1]

	if err = tree.Grow(newParent); err != nil {
		return err
	}
	if cn, err := newParent.followBranch(newName); err != nil {
		return err
	} else if cn != nil {
		return fmt.Errorf("new name already taken")
	}
	if err = tree.RemoveForMerge(nodeToMove); err != nil {
		return err
	}
	nodeToMove.info.Name = newName
	if err := newParent.addChild(nodeToMove); err != nil {
		return err
	}
	nodeToMove.markDirty()
	return nil
}
