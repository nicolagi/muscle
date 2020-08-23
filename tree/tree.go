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
		D: Dir{
			Name: name,
			Mode: perm,
		},
	}
	child.D.Qid.Path = uint64(time.Now().UnixNano())
	child.D.Qid.Version = 1
	child.updateMTime()
	if added := node.add(child); !added {
		return nil, ErrExists
	}
	node.SetMTime(child.D.Modified)
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
	node.parent.removeChild(node.D.Name)
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
		node.markUnlinked(node.Path())
	}
	parent := node.parent
	removedCount := parent.removeChild(node.D.Name)
	if removedCount == 0 {
		return fmt.Errorf("node %q not found within %q", node.D.Name, parent.D.Name)
	}
	if removedCount > 1 {
		log.WithField("count", removedCount).Error("Removed more than one child")
	}
	parent.updateMTime()
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
func (tree *Tree) Graft(parent *Node, child *Node) error {
	if e := tree.Grow(parent); e != nil {
		return e
	}
	if node, found := parent.followBranch(child.D.Name); found {
		if err := tree.RemoveForMerge(node); err != nil {
			return fmt.Errorf("tree.Tree.Graft: parent: %v: %w", parent, err)
		}
	}
	if added := parent.add(child); added {
		parent.markDirty()
		return nil
	}
	return ErrExists
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
	if _, found := newParent.followBranch(newName); found {
		return fmt.Errorf("new name already taken")
	}
	if err = tree.RemoveForMerge(nodeToMove); err != nil {
		return err
	}
	nodeToMove.D.Name = newName
	if added := newParent.add(nodeToMove); added {
		nodeToMove.markDirty()
		return nil
	}
	return fmt.Errorf("add failed")
}
