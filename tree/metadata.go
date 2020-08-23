package tree

import (
	"fmt"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
)

func (tree *Tree) Seal() error {
	if tree.readOnly {
		return ErrReadOnly
	}
	if err := tree.seal(tree.root); err != nil {
		return err
	}
	return tree.store.updateLocalRootPointer(tree.root.pointer)
}

func (tree *Tree) seal(node *Node) error {
	// Might've been loaded but then trimmed; in that case we still now whether it's sealed or not.
	if node.flags&sealed != 0 {
		log.Printf("Already sealed: %v", node)
		return nil
	}

	if node.flags&loaded == 0 {
		log.Printf("Loading node: %v", node)
		if err := tree.store.LoadNode(node); err != nil {
			// Contrary to tree.Tree.Grow(), we won't handle the case where the node is
			// not found or the codec necessary to decode it is not found. If we did,
			// we'd also have to load all siblings and ensure sibling names are unique.
			return fmt.Errorf("tree.Tree.Seal: %v: %w", node, err)
		}
	}
	// After loading, we may find it was sealed in fact, e.g., when the node was never loaded.
	if node.flags&sealed != 0 {
		log.Printf("Already sealed (after loading): %v", node)
		return nil
	}
	for _, child := range node.children {
		if err := tree.seal(child); err != nil {
			return err
		}
	}
	for _, b := range node.blocks {
		if _, err := b.Seal(); err != nil {
			return err
		}
	}
	log.Printf("Sealing node: %v", node)
	if err := tree.store.SealNode(node); err != nil {
		return err
	}
	log.Printf("Sealed: %v", node)
	return nil
}

// FlushIfNotDoneRecently dumps the in-memory changes to the staging area if not done recently (according to the snapshot frequency constant).
func (tree *Tree) FlushIfNotDoneRecently() error {
	if time.Since(tree.lastFlushed) < SnapshotFrequency {
		return nil
	}
	err := tree.depthFirstSave(tree.root)
	if err != nil {
		return err
	}
	if tree.readOnly {
		return ErrReadOnly
	}
	err = tree.store.updateLocalRootPointer(tree.root.pointer)
	if err != nil {
		return err
	}
	tree.lastFlushed = time.Now()
	return nil
}

// TODO This is a very ugly hack
func (tree *Tree) SetRevision(r *Revision) {
	if !tree.root.pointer.Equals(r.rootKey) {
		panic("can't set a revision with mismatching root")
	}
	tree.revision = r.key
}

func (tree *Tree) depthFirstSave(node *Node) error {
	if node.flags&dirty == 0 {
		return nil
	}
	for _, child := range node.children {
		if err := tree.depthFirstSave(child); err != nil {
			return err
		}
	}
	log.WithFields(log.Fields{
		"path": node.Path(),
		"key":  node.pointer.Hex(),
	}).Debug("Persisting node")
	for _, b := range node.blocks {
		_, err := b.Flush()
		if err != nil {
			return err
		}
	}
	return tree.store.StoreNode(node)
}

// When marking a node dirty (i.e., to be persisted because it changed contents
// or metadata), we need to mark dirty its parent and so on until the root of the
// tree. This is because we use a Merkle tree, which means each node's key must
// be the hash if its contents. If a node changes, its key changes, hence its parent
// changes (the key referring to the child changes).
// If a node is new, we need to set a temporary random key, so that the key can
// still be used to distinguish nodes. TODO investigate and clarify
// Last but not least, a node's data may be stored inside of a node in its
// parent chain (the node's key is said to be packed in this case, and the parent
// with the pack is called a fat node). So we need to remove the key-value pair
// from such pack (or packs would keep growing). (This mechanism makes for
// bigger node metadata blocks but fewer overall node metadata blocks.)
func (node *Node) markDirty() {
	entry := log.WithFields(log.Fields{
		"op":   "markDirty",
		"node": node.String(),
	})
	if node == nil || node.flags&dirty != 0 {
		entry.Debug("Already dirty")
		return
	}
	entry.Debug("Setting dirty and recursing")
	node.flags |= dirty
	node.flags &= ^sealed
	node.parent.markDirty()
}

func (node *Node) Unlinked() bool {
	return node.flags&unlinked != 0
}

func (node *Node) markUnlinked(pathname string) {
	node.flags |= unlinked
	pathname = filepath.Join(pathname, node.D.Name)
	for _, child := range node.children {
		child.markUnlinked(pathname)
	}
}
