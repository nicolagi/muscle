package tree

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/nicolagi/muscle/internal/config"
	"github.com/nicolagi/muscle/internal/debug"
	"github.com/nicolagi/muscle/internal/linuxerr"
	"github.com/nicolagi/muscle/internal/storage"
	"github.com/pkg/errors"
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

// NewTree constructs a new tree object using the given store, and
// according to the given options (see the TreeOption section).
func NewTree(store *Store, opts ...TreeOption) (*Tree, error) {
	debug.Assert(store.blockFactory != nil)
	t := &Tree{
		store:     store,
		readOnly:  true,
		blockSize: config.BlockSize,
	}
	for _, o := range opts {
		if err := o(t); err != nil {
			return nil, fmt.Errorf("tree.NewTree: %v", err)
		}
	}
	if t.root == nil {
		parent := &Node{
			blockFactory: store.blockFactory,
			flags:        loaded,
			info:         NodeInfo{Mode: 0700 | DMDIR},
		}
		root, err := t.Add(parent, "root", 0700|DMDIR)
		if err != nil {
			return nil, fmt.Errorf("tree.NewTree: %v", err)
		}
		t.root = root
		// Clear out the fake parent,
		// which was only introduced to re-use the logic in tree.Add.
		t.root.parent = nil
	}
	return t, nil
}

func (tree *Tree) Attach() *Node {
	return tree.root
}

func (tree *Tree) Root() (storage.Pointer, *Node) { return tree.revision, tree.root }

func (tree *Tree) Add(node *Node, name string, perm uint32) (*Node, error) {
	debug.Assert(node.blockFactory != nil)
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
	if err := tree.Grow(node); err != nil {
		return nil, err
	}
	if err := node.addChild(child); err != nil {
		return nil, err
	}
	node.touchNow()
	child.markDirty()
	return child, nil
}

func (tree *Tree) Unlink(node *Node) error {
	if node.IsRoot() {
		return fmt.Errorf("unlink root: %w", linuxerr.EPERM)
	}
	if len(node.children) > 0 {
		return linuxerr.ENOTEMPTY
	}
	node.parent.removeChild(node.info.Name)
	node.parent.markDirty()
	node.markUnlinked()
	return nil
}

func (tree *Tree) Discard(node *Node) {
	node.discard()
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
		return linuxerr.EBUSY
	}
	node.markUnlinked()
	var g func(*Node) error
	g = func(n *Node) error {
		// We actually need to recurse only of the node is in the index.
		// If it's in the repository, so are all its children nodes or data blocks.
		if len(n.pointer) == 32 {
			return nil
		}
		if n.flags&loaded == 0 {
			if err := tree.store.LoadNode(n); err != nil {
				return err
			}
		}
		for _, c := range n.children {
			if err := g(c); err != nil {
				return err
			}
		}
		if n.refs == 0 {
			n.discard()
		}
		return nil
	}
	if err := g(node); err != nil {
		return err
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
		if node.refs > 0 {
			return fmt.Errorf("%q: %w", childName, linuxerr.EBUSY)
		}
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

func (tree *Tree) Rename(sourcepath, targetpath string) error {
	sourcepath = filepath.Clean(sourcepath)
	targetpath = filepath.Clean(targetpath)

	// Validation peculiar to musclefs.
	if sourcepath == "/" || targetpath == "/" {
		return fmt.Errorf("root directory: %w", linuxerr.EINVAL)
	}
	if sourcepath == "." || targetpath == "." {
		return fmt.Errorf("dot: %w", linuxerr.EINVAL)
	}
	if sourcepath[0] == '/' || targetpath[0] == '/' {
		return fmt.Errorf("rooted path: %w", linuxerr.EINVAL)
	}
	if strings.HasPrefix(sourcepath, "./") || strings.HasPrefix(targetpath, "./") {
		return fmt.Errorf("dot slash: %w", linuxerr.EINVAL)
	}

	snames := strings.Split(sourcepath, "/")
	tnames := strings.Split(targetpath, "/")

	snodes, err := tree.trywalk(snames)
	if err != nil {
		return fmt.Errorf("walking to %q: %w", sourcepath, err)
	}
	tnodes, err := tree.trywalk(tnames)
	if err != nil {
		return fmt.Errorf("walking to %q: %w", targetpath, err)
	}

	if len(snodes) < len(snames) {
		return fmt.Errorf("%q: %w", sourcepath, linuxerr.ENOENT)
	}
	if len(tnodes) < len(tnames)-1 {
		return fmt.Errorf("%q: %w", filepath.Join(tnames[:len(tnames)-1]...), linuxerr.ENOENT)
	}

	// Do the check here, after we checked that the source exists.
	if sourcepath == targetpath {
		return nil
	}

	if len(targetpath) != len(sourcepath) {
		if strings.HasPrefix(targetpath, sourcepath) {
			return fmt.Errorf("nesting: %w", linuxerr.EINVAL)
		}
		if strings.HasPrefix(sourcepath, targetpath) {
			return fmt.Errorf("nesting: %w", linuxerr.ENOTEMPTY)
		}
	}

	source := snodes[len(snodes)-1]
	sourceparent := source.parent

	// Peculiar to musclefs:
	if source.refs > 0 {
		return fmt.Errorf("%q has %d references: %w", sourcepath, source.refs, linuxerr.EBUSY)
	}

	var target, targetparent *Node
	if len(tnodes) == len(tnames) {
		target = tnodes[len(tnodes)-1]
		targetparent = target.parent
	} else {
		target = nil
		if len(tnodes) > 0 {
			targetparent = tnodes[len(tnodes)-1]
		} else {
			targetparent = tree.root
		}
	}

	if target != nil {
		if target.info.Mode&DMDIR != 0 && source.info.Mode&DMDIR == 0 {
			return fmt.Errorf("file to directory: %w", linuxerr.EISDIR)
		}
		if target.info.Mode&DMDIR != 0 && len(target.children) > 0 {
			return fmt.Errorf("%q: %w", targetpath, linuxerr.ENOTEMPTY)
		}
		if target.info.Mode&DMDIR == 0 && source.info.Mode&DMDIR != 0 {
			return fmt.Errorf("directory to file: %w", linuxerr.ENOTDIR)
		}

		// Peculiar to musclefs:
		if target.refs > 0 {
			return fmt.Errorf("%q has %d references: %w", targetpath, target.refs, linuxerr.EBUSY)
		}

		targetparent.removeChild(target.info.Name)
		targetparent.touchNow()
		target.markUnlinked()
		if target.refs == 0 {
			target.discard()
		}
	}
	sourceparent.removeChild(source.info.Name)
	sourceparent.touchNow()
	source.info.Name = tnames[len(tnames)-1]
	source.parent = targetparent
	targetparent.children = append(targetparent.children, source)
	sourceparent.markDirty()
	source.markDirty()
	// The source may already be dirty, and fail to propagate the flag to the root of the tree!
	targetparent.markDirty()
	return nil
}
