package tree

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"path"
	"time"

	"github.com/nicolagi/go9p/p"
	log "github.com/sirupsen/logrus"

	"github.com/nicolagi/muscle/storage"
)

type nodeFlags uint8

const (
	// TODO This is unused. Use it instead of relying on the empty-name convention.
	loaded nodeFlags = 1 << 0
	// A dirty node is one that has mutated since it was loaded from
	// storage; it should be persisted before exiting and before unloading
	// the node, at the very least.
	dirty nodeFlags = 1 << 1
	// This will be used in a later CL.
	sealed nodeFlags = 1 << 2
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
	if ff & ^(loaded|dirty|sealed) != 0 {
		buf.WriteString("extraneous,")
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}

// TODO This is a terribly temporary (that's probably a lie.) kludge to enable snapshotsfs
func (node *Node) Children() []*Node {
	return node.children
}

// Node describes a node in the filesystem tree.
type Node struct {
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

	// 9P serialized directory contents.
	serializedDirectoryEntries        []byte
	serializedDirectoryEntriesOffsets []uint32

	// A hash of all the data below, before encryption.  Encryption causes
	// the same content to generated different data every time it is
	// encrypted. Therefore the hash would change and we would not be able
	// to tell two nodes are the same.
	pointer storage.Pointer

	// 9P structure containing the node's metadata, e.g., name, size,
	// permissions, modification time...
	D p.Dir

	// Only one of the two will be relevant, based on the node type.  The
	// children field is relevant for directories, the blocks field is
	// relevant for regular files.
	children []*Node
	blocks   []*Block
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

func (node *Node) Key() storage.Pointer {
	return node.pointer
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
	return node.D.Mode&p.DMDIR != 0
}

func (node *Node) String() string {
	if node == nil {
		return "(nil node)"
	}
	return fmt.Sprintf("%s@%s", node.D.Name, node.pointer)
}

func (node *Node) PrepareForReads() {
	node.serializedDirectoryEntries = nil
	node.serializedDirectoryEntriesOffsets = nil
	entryEnd := 0
	for _, child := range node.children {
		entry := p.PackDir(&child.D, false)
		node.serializedDirectoryEntries = append(node.serializedDirectoryEntries, entry...)
		entryEnd += len(entry)
		node.serializedDirectoryEntriesOffsets = append(node.serializedDirectoryEntriesOffsets, uint32(entryEnd))
	}
}

// Finds the closest index into the serializedDirectoryEntries array where a dirent starts or ends.
func (node *Node) indexFloor(wanted uint32) (index uint32) {
	// Shortcut.
	if wanted == 0 {
		return 0
	}

	// TODO use binary search.
	for i := len(node.serializedDirectoryEntriesOffsets) - 1; i >= 0; i-- {
		end := node.serializedDirectoryEntriesOffsets[i]
		if end <= wanted {
			return end
		}
	}

	return 0
}

func (node *Node) DirReadAt(b []byte, off int64) (n int, err error) {
	// Validate the offset.
	start := node.indexFloor(uint32(off))
	if start != uint32(off) {
		return -1, &p.Error{Err: "invalid offset", Errornum: p.EINVAL}
	}

	// Shortcut.
	if len(node.serializedDirectoryEntries) == 0 {
		return 0, nil
	}

	// Find end of dir entry.
	end := node.indexFloor(uint32(off) + uint32(len(b)))
	if end == start {
		return 0, nil
	}
	if end-start > uint32(len(b)) {
		return -1, &p.Error{Err: "too small read size for dir entry", Errornum: p.EINVAL}
	}
	return copy(b, node.serializedDirectoryEntries[start:end]), nil
}

func (node *Node) childrenMap() map[string]*Node {
	m := make(map[string]*Node)
	for _, child := range node.children {
		m[child.D.Name] = child
	}
	return m
}

func (node *Node) hasEqualBlocks(other *Node) bool {
	if node == nil && other == nil {
		return true
	}
	if node == nil || other == nil {
		return false
	}
	if len(node.blocks) != len(other.blocks) {
		return false
	}
	for i, b := range node.blocks {
		if b.pointer.Hex() != other.blocks[i].pointer.Hex() {
			return false
		}
	}
	return true
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
		n.D.Atime = uint32(time.Now().Unix())
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

		age := now - node.D.Atime

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
		node.serializedDirectoryEntries = nil
		node.serializedDirectoryEntriesOffsets = nil
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
		node.updateMTime()
	}
	return
}

func (node *Node) updateMTime() {
	node.D.Mtime = uint32(time.Now().Unix())
	node.markDirty()
}

func (node *Node) SetMTime(mtime uint32) {
	node.D.Mtime = mtime
	node.markDirty()
}

func (node *Node) IsController() bool {
	return node.D.Name == "ctl" && node.parent.IsRoot()
}

func (node *Node) SameKind(other *Node) bool {
	return (node.D.Mode&p.DMDIR != 0 && other.D.Mode&p.DMDIR != 0) ||
		(node.D.Mode&p.DMDIR == 0 && other.D.Mode&p.DMDIR == 0)
}

func (node *Node) IsRoot() bool {
	return node.D.Mode&p.DMDIR != 0 && node.parent == nil
}

func (node *Node) IsFile() bool {
	return node.D.Mode&p.DMDIR == 0
}

func (node *Node) SetMode(mode uint32) {
	node.D.Mode = mode
	node.markDirty()
}

func (node *Node) Rename(newName string) {
	node.parent.removeChild(newName)
	node.D.Name = newName
	node.markDirty()
}

func (node *Node) recomputeQID() {
	node.D.Qid.Version = 0
	for _, b := range node.pointer.Bytes()[:4] {
		node.D.Qid.Version = (node.D.Qid.Version << 8) | uint32(b)
	}
	node.D.Qid.Path = 0
	checksum := sha1.Sum([]byte(node.Path()))
	for _, b := range checksum[:8] {
		node.D.Qid.Path = (node.D.Qid.Path << 8) | uint64(b)
	}
}

// TODO nodesByName?
type NodeSlice []*Node

func (ns NodeSlice) Len() int {
	return len(ns)
}

func (ns NodeSlice) Less(i, j int) bool {
	return ns[i].D.Name < ns[j].D.Name
}

func (ns NodeSlice) Swap(i, j int) {
	ns[i], ns[j] = ns[j], ns[i]
}
