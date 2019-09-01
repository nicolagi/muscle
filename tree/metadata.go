package tree

import (
	"time"

	"github.com/nicolagi/muscle/storage"
	log "github.com/sirupsen/logrus"
)

// CreateRevision dumps the in-memory changes to the staging area if not done recently (according to the snapshot frequency constant).
func (tree *Tree) CreateRevision() error {
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
	rev := NewRevision(tree.instance, tree.root.pointer, []storage.Pointer{tree.revision})
	err = tree.store.PushRevisionLocally(rev)
	if err != nil {
		return err
	}
	tree.revision = rev.key
	tree.lastFlushed = time.Now()
	return nil
}

// TODO This is a very ugly hack
func (tree *Tree) SetRevision(r *Revision) {
	if tree.root.pointer.Hex() != r.rootKey.Hex() {
		panic("can't set a revision with mismatching root")
	}
	tree.revision = r.key
}

func (tree *Tree) depthFirstSave(node *Node) error {
	if !node.dirty {
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
	for _, block := range node.blocks {
		if block.state == blockDirty {
			if err := tree.store.StoreBlock(block); err != nil {
				return err
			}
			block.state = blockClean
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
// bigger node metadata blobs but fewer overall node metadata blobs.)
func (node *Node) markDirty() {
	entry := log.WithFields(log.Fields{
		"op":   "markDirty",
		"node": node.String(),
	})
	if node == nil || node.dirty {
		entry.Debug("Already dirty")
		return
	}
	entry.Debug("Setting dirty and recursing")
	node.dirty = true
	if node.pointer.IsNull() {
		node.pointer = storage.RandomPointer()
	}
	node.parent.markDirty()
}
