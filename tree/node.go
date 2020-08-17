package tree

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	log "github.com/sirupsen/logrus"
)

type nodeFlags uint8

const (
	loaded nodeFlags = 1 << 0
	// A dirty node is one that has mutated since it was loaded from
	// storage; it should be persisted before exiting and before unloading
	// the node, at the very least.
	dirty  nodeFlags = 1 << 1
	sealed nodeFlags = 1 << 2
	// The node was unlinked from the tree by a merge or rename operation.
	unlinked nodeFlags = 1 << 3
	// If you add flags here, add them to nodeFlags.String as well.
)

// String implements fmt.Stringer for debugging purposes.
func (ff nodeFlags) String() string {
	if ff == 0 {
		return "none"
	}
	var buf bytes.Buffer
	if ff&loaded != 0 {
		buf.WriteString("loaded,")
	}
	if ff&dirty != 0 {
		buf.WriteString("dirty,")
	}
	if ff&sealed != 0 {
		buf.WriteString("sealed,")
	}
	if ff&unlinked != 0 {
		buf.WriteString("unlinked,")
	}
	if ff & ^(loaded|dirty|sealed|unlinked) != 0 {
		buf.WriteString("extraneous,")
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}

// Node describes a node in the filesystem tree.
type Node struct {
	blockFactory *block.Factory

	// Number of 9P fids that refer to this node.  A node that has no
	// references can be unloaded unless it has changed and needs to be
	// saved to the staging area first (see flag below).  Unloading the
	// node means that children and blocks are set to nil so they can be
	// GC'ed, and the name set to the empty string which signifies the node
	// is not loaded.
	refs int

	flags nodeFlags
	bsize uint32 // Block size, for future extension.

	// Pointer to the parent node. For the root node (and only for the root
	// node) this will be nil.
	parent *Node

	// A hash of all the data below, before encryption.  Encryption causes
	// the same content to generated different data every time it is
	// encrypted. Therefore the hash would change and we would not be able
	// to tell two nodes are the same.
	pointer storage.Pointer

	// Fields containing the node's metadata, e.g., name, size,
	// permissions, modification time...
	D Dir

	// Only one of the two will be relevant, based on the node type.  The
	// children field is relevant for directories, the blocks field is
	// relevant for regular files.
	children []*Node
	blocks   []*block.Block
}

func (node *Node) followBranch(name string) (*Node, bool) {
	if name == "" {
		panic("should not be looking for a child with no name")
	}
	if name == ".." {
		if node.parent == nil {
			// We're at the root.
			return node, true
		}
		return node.parent, true
	}
	for _, c := range node.children {
		if c.D.Name == name {
			return c, true
		}
	}
	return nil, false
}

// Returns whether the child was added. If it is already present, it does not
// get added.
func (node *Node) add(newChild *Node) bool {
	if newChild.flags&loaded != 0 {
		if _, ok := node.followBranch(newChild.D.Name); ok {
			return false
		}
	}
	newChild.parent = node
	node.children = append(node.children, newChild)
	return true
}

// Path returns the full path name to this node.
func (node *Node) Path() string {
	if node == nil {
		return ""
	}
	if node.parent == nil {
		return node.D.Name
	}
	return path.Join(node.parent.Path(), node.D.Name)
}

func (node *Node) IsDir() bool {
	return node.D.Mode&DMDIR != 0
}

func (node *Node) String() string {
	if node == nil {
		return "(nil node)"
	}
	return fmt.Sprintf("%s@%s", node.D.Name, node.pointer)
}

func (node *Node) Children() []*Node {
	return node.children
}

func (node *Node) childrenMap() map[string]*Node {
	m := make(map[string]*Node)
	for _, child := range node.children {
		m[child.D.Name] = child
	}
	return m
}

func (node *Node) hasEqualBlocks(other *Node) (bool, error) {
	if node == nil && other == nil {
		return true, nil
	}
	if node == nil || other == nil {
		return false, nil
	}
	if len(node.blocks) != len(other.blocks) {
		log.Printf("Different number of blocks: %v %v", node, other)
		return false, nil
	}
	for i, b := range node.blocks {
		same, err := b.SameValue(other.blocks[i])
		if err != nil {
			return false, err
		}
		if !same {
			log.Printf("Difference at block %d: %v %v", i, node, other)
			return false, nil
		}
	}
	return true, nil
}

// Ref increments the node's ref count, and that of all its ancestors.
// It also sets the node's access time. Since we can only stat() after
// walk(), this means we're updating the atime also to answer a stat
// syscall. That's not correct. But the use case for atime is, as I said,
// to track when last used in musclefs. It should perhaps be called last ref'd.
func (node *Node) Ref(reason string) {
	log.WithFields(log.Fields{
		"path":      node.Path(),
		"reason":    reason,
		"increment": 1,
	}).Debug("REF/UNREF")
	for n := node; n != nil; n = n.parent {
		n.refs++
	}
}

// Unref decrements the node's ref count, and that of all its ancestors.
func (node *Node) Unref(reason string) {
	log.WithFields(log.Fields{
		"path":      node.Path(),
		"reason":    reason,
		"increment": -1,
	}).Debug("REF/UNREF")
	for n := node; n != nil; n = n.parent {
		n.refs--
	}
}

// Trim removes links from the given node to blocks (for files) and
// children nodes (for directories), thus such resources can be
// garbage collected. It will recurse into children, so that
// invoking it on the root of the tree will effectively mark as
// garbage all resources associated with nodes not in use (according
// to their reference counts. Note that a dirty node can not be
// trimmed because its information can not be retrieved from local
// or remote storage.
func (node *Node) Trim() {

	now := uint32(time.Now().Unix())
	minAge := uint32(300) // 5 minutes

	var trim func(node *Node)

	trim = func(node *Node) {
		for _, child := range node.children {
			if child.flags&loaded != 0 {
				trim(child)
			}
		}

		age := now - node.D.Modified

		le := log.WithFields(log.Fields{
			"path":  node.Path(),
			"key":   node.pointer.Hex(),
			"age":   age,
			"refs":  node.refs,
			"flags": node.flags,
		})

		if node.IsRoot() || node.flags&dirty != 0 || node.refs != 0 || age < minAge {
			le.Debug("Not trimming")
			return
		}

		le.Debug("Trimming")
		node.flags &^= loaded
		node.D.Name = ""
		node.blocks = nil
		node.children = nil
	}

	trim(node)
}

// Returns the number of children removed (hopefully only 0 or 1).
func (node *Node) removeChild(name string) (removedCount int) {
	var newChildren []*Node
	for _, child := range node.children {
		if child.D.Name != name {
			newChildren = append(newChildren, child)
		} else {
			removedCount++
		}
	}
	node.children = newChildren
	if removedCount > 0 {
		node.touchNow()
	}
	return
}

func (node *Node) touchNow() {
	node.D.Modified = uint32(time.Now().Unix())
	node.markDirty()
}

// Touch updates the modification timestamp of the node and marks the node dirty,
// so that it is later flushed to disk.
func (node *Node) Touch(seconds uint32) {
	node.D.Modified = seconds
	node.markDirty()
}

func (node *Node) IsRoot() bool {
	return node.D.Mode&DMDIR != 0 && node.parent == nil
}

// SetPerm sets the Unix permission bits.
// All bits other than 0777 are silently ignored.
func (node *Node) SetPerm(perm uint32) {
	node.D.Mode &= 0xfffffe00
	node.D.Mode |= (0x000001ff & perm)
	node.markDirty()
}

func (node *Node) Rename(newName string) {
	node.parent.removeChild(newName)
	node.D.Name = newName
	node.markDirty()
}

func (node *Node) Truncate(requestedSize uint64) error {
	if node.IsDir() {
		return errors.New("impossible to truncate a directory")
	}
	var err error
	if requestedSize == node.D.Size {
		return nil
	} else if requestedSize > node.D.Size {
		err = node.grow(requestedSize)
	} else {
		err = node.shrink(requestedSize)
	}
	if err != nil {
		return err
	}
	node.D.Size = requestedSize
	node.touchNow()
	node.D.Qid.Version++
	return nil
}

func (node *Node) grow(requestedSize uint64) (err error) {
	add := func(size int) error {
		b, err := node.blockFactory.New(nil, int(node.bsize))
		if err != nil {
			return err
		}
		if err = b.Truncate(size); err != nil {
			return err
		}
		node.blocks = append(node.blocks, b)
		return nil
	}
	blockSize := uint64(node.bsize)
	q, r := node.D.Size/blockSize, int(node.D.Size%blockSize)
	nextq, nextr := requestedSize/blockSize, int(requestedSize%blockSize)
	if q < nextq && r > 0 {
		if err := node.blocks[q].Truncate(int(node.bsize)); err != nil {
			return err
		}
		q, r = q+1, 0
	}
	for ; q < nextq; q++ {
		if err := add(int(node.bsize)); err != nil {
			return err
		}
	}
	if nextr > 0 {
		if r > 0 {
			err = node.blocks[q].Truncate(nextr)
		} else {
			err = add(nextr)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (node *Node) shrink(requestedSize uint64) error {
	// The requested size requires q full blocks and one block with only r bytes.
	q := int(requestedSize / uint64(node.bsize))
	r := int(requestedSize % uint64(node.bsize))
	if r > 0 {
		if err := node.blocks[q].Truncate(r); err != nil {
			return err
		}
		q++
	}
	l := len(node.blocks)
	for i := q; i < l; i++ {
		node.blocks[i].Discard()
	}
	node.blocks = node.blocks[:q]
	return nil
}

func (node *Node) WriteAt(p []byte, off int64) error {
	if err := node.ensureBlocksForWriting(off + int64(len(p))); err != nil {
		return err
	}
	err := node.write(p, off)
	if err != nil {
		return err
	}
	node.touchNow()
	node.D.Qid.Version++
	return nil
}

func (node *Node) write(p []byte, off int64) error {
	if len(p) == 0 {
		return nil
	}
	bs := int64(node.bsize)
	written, delta, err := node.getBlock(off).Write(p, int(off%bs))
	if err != nil {
		return err
	}
	off -= off % bs
	off += bs
	node.D.Size += uint64(delta)
	return node.write(p[written:], off)
}

// This adds blocks so that looking them up by offset does not panic,
// but does not zero-pad them. In other words, don't use grow().
//  If you do, you have to update node.D.Length as well.
// And that's cheating, because that's for Write to do.
func (node *Node) ensureBlocksForWriting(requiredBytes int64) error {
	bs := int64(node.bsize)
	q := int(requiredBytes / bs)
	if requiredBytes%bs != 0 {
		q++
	}
	for len(node.blocks) < q {
		b, err := node.blockFactory.New(nil, int(node.bsize))
		if err != nil {
			return err
		}
		node.blocks = append(node.blocks, b)
	}
	return nil
}

func (node *Node) getBlock(off int64) *block.Block {
	index := int(off / int64(node.bsize))
	if index >= len(node.blocks) {
		return nil
	}
	return node.blocks[index]
}

func (node *Node) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	block := node.getBlock(off)
	if block == nil {
		return 0, nil
	}
	o := int(off % int64(node.bsize))
	n, err := block.Read(p, o)
	if n == 0 || err != nil {
		return n, err
	}
	m, err := node.ReadAt(p[n:], off+int64(n))
	return n + m, err
}

func (node *Node) metadataBlock() (*block.Block, error) {
	ref, err := block.NewRef([]byte(node.pointer))
	if err != nil {
		return nil, err
	}
	b, err := node.blockFactory.New(ref, metadataBlockMaxSize)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (node *Node) discard() {
	for _, b := range node.blocks {
		b.Discard()
	}
	if len(node.pointer) > 0 {
		if b, err := node.metadataBlock(); err != nil {
			log.Printf("tree.Node.discard: %v", err)
		} else if b != nil {
			b.Discard()
		}
	}
	node.blocks = nil
	node.pointer = nil
}
