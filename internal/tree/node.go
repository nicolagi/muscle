package tree

import (
	"bytes"
	"errors"
	"fmt"
	stdlog "log"
	"time"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/debug"
	"github.com/nicolagi/muscle/internal/linuxerr"
	"github.com/nicolagi/muscle/internal/storage"
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
	info NodeInfo

	// Only one of the two will be relevant, based on the node type.  The
	// children field is relevant for directories, the blocks field is
	// relevant for regular files.
	children []*Node
	blocks   []*block.Block
}

// Info returns a copy of the node's information struct.
func (node *Node) Info() NodeInfo {
	return node.info
}

func (node *Node) followBranch(name string) (*Node, error) {
	const method = "Node.followBranch"
	if node.flags&loaded == 0 {
		return nil, errorf(method, "looking up %q within %q, which hasn't been loaded", name, node.Path())
	}
	if name == "" {
		return nil, errorf(method, "looking up child with no name within %q", node.Path())
	}
	if name == ".." {
		if node.parent == nil {
			return node, nil
		}
		return node.parent, nil
	}
	for _, c := range node.children {
		if c.info.Name == name {
			return c, nil
		}
	}
	return nil, nil
}

func (node *Node) addChildPointer(p storage.Pointer) error {
	debug.Assert(node.blockFactory != nil)
	debug.Assert(node.flags&loaded == 0)
	var stub Node
	stub.blockFactory = node.blockFactory
	stub.parent = node
	stub.pointer = p
	node.children = append(node.children, &stub)
	return nil
}

// addChild fails if the node already has a child with matching name.
func (node *Node) addChild(newChild *Node) error {
	const method = "node.addChild"
	if node.flags&loaded == 0 {
		return errorf(method, "add child %v to node %v, which wasn't loaded", newChild, node)
	}
	if newChild.flags&loaded == 0 {
		return errorf(method, "add child %v, which wasn't loaded, to node %v", newChild, node)
	}
	if cn, err := node.followBranch(newChild.info.Name); err != nil {
		return err
	} else if cn != nil {
		return errorf(method, "%q within %q: %w", newChild.info.Name, node.Path(), ErrExist)
	}
	newChild.parent = node
	node.children = append(node.children, newChild)
	newChild.markLinked()
	return nil
}

// Path returns the full path to the node, e.g.,
// "/src/muscle/tree/node.go". A non-loaded node is represented by an
// asterisk; as a consequence, a path can take the form
// "/src/muscle/tree/*".
func (node *Node) Path() string {
	if node == nil {
		return ""
	}
	if node.parent == nil {
		return "/"
	}
	var stk []*Node
	for n := node; n != nil; n = n.parent {
		stk = append(stk, n)
	}
	var buf bytes.Buffer
	for i := len(stk) - 2; i >= 0; i-- {
		if stk[i].flags&loaded != 0 {
			buf.WriteRune('/')
			buf.WriteString(stk[i].info.Name)
		} else {
			buf.WriteString("/*")
		}
	}
	return buf.String()
}

func (node *Node) IsDir() bool {
	return node.info.Mode&DMDIR != 0
}

// String returns the path to the node and its hash pointer, plus a
// "-dirty" suffix if the node hasn't been flushed to disk.
func (node *Node) String() string {
	switch {
	case node == nil:
		return "nil"
	case node.flags&dirty != 0:
		return fmt.Sprintf("%s@%s-dirty", node.Path(), node.pointer)
	default:
		return fmt.Sprintf("%s@%s", node.Path(), node.pointer)
	}
}

func (node *Node) Children() []*Node {
	return node.children
}

func (node *Node) childrenMap() map[string]*Node {
	m := make(map[string]*Node)
	for _, child := range node.children {
		m[child.info.Name] = child
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
	// Legacy check. File systems should no longer have mixed-size blocks.
	if node.bsize != other.bsize {
		log.Printf("Different block sizes (%d and %d), assuming nodes have different contents", node.bsize, other.bsize)
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
func (node *Node) Ref() int {
	for n := node; n != nil; n = n.parent {
		n.refs++
	}
	return node.refs
}

// Unref decrements the node's ref count, and that of all its ancestors.
func (node *Node) Unref() int {
	for n := node; n != nil; n = n.parent {
		n.refs--
	}
	return node.refs
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

		age := now - node.info.Modified

		le := log.WithFields(log.Fields{
			"path":  node.Path(),
			"key":   node.pointer.Hex(),
			"age":   age,
			"refs":  node.refs,
			"flags": node.flags,
		})

		if node.IsRoot() || node.flags&dirty != 0 || node.refs != 0 || age < minAge {
			le.Debug("Not trimming, but maybe we can trim blocks")
			for _, b := range node.blocks {
				if b.Forget() {
					stdlog.Printf("Trimmed node %q block %q", node.Path(), b.Ref())
				}
			}
			return
		}

		le.Debug("Trimming")
		node.flags &^= loaded
		node.info.Name = ""
		node.blocks = nil
		node.children = nil
	}

	trim(node)
}

// Returns the number of children removed (hopefully only 0 or 1).
func (node *Node) removeChild(name string) (removedCount int) {
	var newChildren []*Node
	for _, child := range node.children {
		if child.info.Name != name {
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
	node.info.Modified = uint32(time.Now().Unix())
	node.markDirty()
}

// Touch updates the modification timestamp of the node and marks the node dirty,
// so that it is later flushed to disk.
func (node *Node) Touch(seconds uint32) {
	node.info.Modified = seconds
	node.markDirty()
}

func (node *Node) IsRoot() bool {
	return node.info.Mode&DMDIR != 0 && node.parent == nil
}

const validMode = 0x000001ff | DMDIR | DMAPPEND | DMEXCL

func (node *Node) SetMode(mode uint32) {
	node.info.Mode = mode & validMode
	node.markDirty()
}

// Rename changes the node's name. If the parent already contains a
// file/empty directory with the new name, that file/directory is
// unlinked first. Stat(5) says that renaming should fail in that
// case, but conforming to the manual page makes it impossible to use
// git (which renames 'index.lock' to an already existing 'index',
// for example) under both 9pfuse and v9fs.
func (node *Node) Rename(newName string) error {
	if p := node.parent; p != nil {
		var kept []*Node
		for _, c := range p.children {
			if c.info.Name != newName {
				kept = append(kept, c)
			} else {
				if c.IsDir() && len(c.children) != 0 {
					return linuxerr.ENOTEMPTY
				}
				c.markUnlinked()
				if c.refs == 0 {
					c.discard()
				}
				p.touchNow()
			}
		}
		p.children = kept
	}
	node.info.Name = newName
	node.markDirty()
	return nil
}

func (node *Node) Truncate(requestedSize uint64) error {
	if node.IsDir() {
		return errors.New("impossible to truncate a directory")
	}
	var err error
	if requestedSize == node.info.Size {
		return nil
	} else if requestedSize > node.info.Size {
		err = node.grow(requestedSize)
	} else {
		err = node.shrink(requestedSize)
	}
	if err != nil {
		return err
	}
	node.info.Size = requestedSize
	node.touchNow()
	node.info.Version++
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
	q, r := node.info.Size/blockSize, int(node.info.Size%blockSize)
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
	if node.info.Mode&DMAPPEND != 0 {
		off = int64(node.info.Size)
	}
	if err := node.ensureBlocksForWriting(off + int64(len(p))); err != nil {
		return err
	}
	err := node.write(p, off)
	if err != nil {
		return err
	}
	node.touchNow()
	node.info.Version++
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
	node.info.Size += uint64(delta)
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
