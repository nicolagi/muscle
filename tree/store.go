package tree

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	"9fans.net/go/plumb"
	"github.com/lionkov/go9p/p"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree/mergebase"
	log "github.com/sirupsen/logrus"
)

// Used for blocks holding serialized revisions and nodes. Since only one block
// is used per revision/node, we can't risk overflowing and ignore
// the configured block size.
const metadataBlockMaxSize = 1024 * 1024

// Store is a high-level entity that takes care of loading and storing
// objects (nodes, revisions) from/to a store. Such operations require
// encryption/decryption, encoding/decoding, actual store put/get.
// It is built on top of the more basic functionality in muscle/storage.
type Store struct {
	localRootKeyFile string
	remoteRootKey    string
	blockFactory     *block.Factory
	pointers         storage.Store
	codec            Codec
}

func NewStore(
	blockFactory *block.Factory,
	pointers storage.Store,
	localRootKeyFile string,
	remoteRootKey string,
) (*Store, error) {
	return &Store{
		blockFactory:     blockFactory,
		localRootKeyFile: localRootKeyFile,
		remoteRootKey:    remoteRootKey,
		pointers:         pointers,
		codec:            newStandardCodec(),
	}, nil
}

func (s *Store) StoreNode(node *Node) error {
	errw := func(e error) error {
		return fmt.Errorf("tree.Store.StoreNode: %w", e)
	}
	encoded, err := s.codec.encodeNode(node)
	if err != nil {
		return errw(err)
	}
	var blk *block.Block
	if len(node.pointer) > 0 {
		blk, err = node.metadataBlock()
	} else {
		blk, err = s.blockFactory.New(nil, metadataBlockMaxSize)
	}
	if err != nil {
		return errw(err)
	}
	if err := blk.Truncate(0); err != nil {
		return errw(err)
	}
	if _, _, err := blk.Write(encoded, 0); err != nil {
		return errw(err)
	}
	if _, err := blk.Flush(); err != nil {
		return errw(err)
	}
	node.pointer = storage.Pointer(blk.Ref().Bytes())
	node.flags &^= dirty
	return nil
}

func (s *Store) SealNode(node *Node) error {
	errw := func(e error) error {
		// If the node failed to seal, let's remove the flag.
		// We have to optimistically set it before sealing or the node hash would be incorrect.
		node.flags &^= sealed
		return fmt.Errorf("tree.Store.SealNode: %w", e)
	}
	node.flags |= sealed
	encoded, err := s.codec.encodeNode(node)
	if err != nil {
		return errw(err)
	}
	var ref block.Ref
	if len(node.pointer) > 0 {
		if ref, err = block.NewRef([]byte(node.pointer)); err != nil {
			return errw(err)
		}
	}
	blk, err := s.blockFactory.New(ref, metadataBlockMaxSize)
	if err != nil {
		return errw(err)
	}
	if err := blk.Truncate(0); err != nil {
		return errw(err)
	}
	if _, _, err := blk.Write(encoded, 0); err != nil {
		return errw(err)
	}
	if _, err := blk.Seal(); err != nil {
		return errw(err)
	}
	node.pointer = storage.Pointer(blk.Ref().Bytes())
	node.flags &^= dirty
	return nil
}

func (s *Store) StoreRevision(r *Revision) error {
	errw := func(e error) error {
		return fmt.Errorf("tree.Store.StoreRevision: %w", e)
	}
	encoded, err := s.codec.encodeRevision(r)
	if err != nil {
		return errw(err)
	}
	// Note: We will treat tree.Revision.key (type storage.Pointer) as the bytes
	// underlying a block.Ref. In future developments, we will change the type of
	// tree.Revision.key.
	var ref block.Ref
	if r.key.Len() > 0 {
		if ref, err = block.NewRef([]byte(r.key)); err != nil {
			return errw(err)
		}
	}
	blk, err := s.blockFactory.New(ref, metadataBlockMaxSize)
	if err != nil {
		return errw(err)
	}
	if err := blk.Truncate(0); err != nil {
		return errw(err)
	}
	if _, _, err := blk.Write(encoded, 0); err != nil {
		return errw(err)
	}
	if _, err := blk.Seal(); err != nil {
		return errw(err)
	}
	// Unsafe:
	r.key = storage.Pointer(blk.Ref().Bytes())
	return nil
}

// LoadNode assumes that dst.key is the destination node's key, and the parent pointer is also correct.
// Loading will overwrite any other data.
func (s *Store) LoadNode(dst *Node) error {
	errw := func(e error) error {
		return fmt.Errorf("tree.Store.LoadNode: %w", e)
	}
	dst.blockFactory = s.blockFactory
	blk, err := dst.metadataBlock()
	if err != nil {
		return errw(err)
	}
	encoded, err := blk.ReadAll()
	if err != nil {
		return errw(err)
	}
	if err := s.codec.decodeNode(encoded, dst); err != nil {
		return errw(err)
	}
	dst.D.Uid = nodeUID
	dst.D.Gid = nodeGID
	dst.flags |= loaded
	return nil
}

// TODO: Belongs to musclefs, not to the tree package.
func (s *Store) updateLocalRootPointer(rootKey storage.Pointer) error {
	log.WithField("key", rootKey).Info("Attempting to store new local root")
	tmp := fmt.Sprintf("%s.%d", s.localRootKeyFile, time.Now().Unix())
	if err := ioutil.WriteFile(tmp, []byte(rootKey.Hex()), 0600); err != nil {
		return err
	}
	return os.Rename(tmp, s.localRootKeyFile)
}

func (s *Store) UpdateRemoteRevision(r *Revision) error {
	return s.pointers.Put(storage.Key(s.remoteRootKey), []byte(r.key.Hex()))
}

func (s *Store) LocalRootKey() (storage.Pointer, error) {
	b, err := ioutil.ReadFile(s.localRootKeyFile)
	if os.IsNotExist(err) {
		return storage.Null, nil
	}
	return storage.NewPointerFromHex(string(b))
}

func (s *Store) LocalRoot() (*Node, error) {
	key, err := s.LocalRootKey()
	if err != nil {
		return nil, err
	}
	return s.loadRoot(key)
}

func (s *Store) RemoteRevisionKey(instance string) (storage.Pointer, error) {
	var rootDescriptor string
	// TODO This special treatment of the argument should be avoided.
	if instance == "" {
		rootDescriptor = s.remoteRootKey
	} else {
		rootDescriptor = RemoteRootKeyPrefix + instance
	}
	rootContents, err := s.pointers.Get(storage.Key(rootDescriptor))
	if err != nil {
		return storage.Null, err
	}
	return storage.NewPointerFromHex(string(rootContents))
}

// RemoteRevision loads the most recent revision object associated with
// the given instance and the filesystem root contained therein.
func (s *Store) RemoteRevision(instance string) (rev *Revision, root *Node, err error) {
	key, err := s.RemoteRevisionKey(instance)
	if err != nil {
		return nil, nil, err
	}
	return s.loadRevisionAndRoot(key)
}

func (s *Store) LoadRevisionByKey(key storage.Pointer) (*Revision, error) {
	errw := func(e error) error {
		return fmt.Errorf("tree.Store.LoadRevisionByKey: %w", e)
	}
	// Note: We will treat tree.Revision.key (type storage.Pointer) as the bytes
	// underlying a block.Ref. In future developments, we will change the type of
	// tree.Revision.key.
	ref, err := block.NewRef([]byte(key))
	if err != nil {
		return nil, errw(err)
	}
	blk, err := s.blockFactory.New(ref, metadataBlockMaxSize)
	if err != nil {
		return nil, errw(err)
	}
	b, err := blk.ReadAll()
	if err != nil {
		return nil, err
	}
	r := &Revision{key: key}
	err = s.codec.decodeRevision(b, r)
	if err != nil {
		return nil, errw(err)
	}
	return r, err
}

func (s *Store) loadRevisionAndRoot(key storage.Pointer) (*Revision, *Node, error) {
	revision, err := s.LoadRevisionByKey(key)
	if err != nil {
		return nil, nil, err
	}
	root := &Node{
		pointer: revision.rootKey,
		parent:  nil, // That's what makes it the root.
	}
	if err := s.LoadNode(root); err != nil {
		return nil, nil, err
	}
	// These two lines would not be needed if I didn't have some bad metadata in my oldest trees.
	root.D.Mode |= p.DMDIR
	root.D.Type = p.QTDIR
	return revision, root, nil
}

func (s *Store) loadRoot(key storage.Pointer) (*Node, error) {
	root := &Node{
		pointer: key,
		parent:  nil, // That's what makes it the root.
	}
	if err := s.LoadNode(root); err != nil {
		return nil, err
	}
	// These two lines would not be needed if I didn't have some bad metadata in my oldest trees.
	root.D.Mode |= p.DMDIR
	root.D.Type = p.QTDIR
	return root, nil
}

func (s *Store) History(maxRevisions int, head *Revision) (rr []*Revision, err error) {
	if head == nil {
		return nil, nil
	}
	return s.history(head, maxRevisions)
}

func (s *Store) history(r *Revision, maxRevisions int) (rr []*Revision, err error) {
	for ; maxRevisions > 0; maxRevisions-- {
		if r == nil {
			break
		}
		rr = append(rr, r)
		if len(r.parents) == 0 {
			break
		}
		// The parent corresponding to the local instance is always the last element in the revision's parent.
		pk := r.parents[len(r.parents)-1]
		if pk.IsNull() {
			break
		}
		r, err = s.LoadRevisionByKey(pk)
		if err != nil {
			break
		}
	}
	return
}

// MergeBase finds a revision that is a common parent of revisions a and b.
func (s *Store) MergeBase(arev, brev *Revision) (storage.Pointer, error) {
	prefetched := make(map[string]*Revision)

	fetch := func(rp storage.Pointer) (*Revision, error) {
		r, err := s.LoadRevisionByKey(rp)
		if err == nil {
			prefetched[rp.Hex()] = r
		}
		return r, err
	}

	convert := func(r *Revision) mergebase.Node {
		return mergebase.Node{
			GraphID: r.instance,
			ID:      r.key.Hex(),
		}
	}

	prefetched[arev.key.Hex()] = arev
	prefetched[brev.key.Hex()] = brev

	graph, base, err := mergebase.Find(convert(arev), convert(brev), func(child mergebase.Node) (parents []mergebase.Node, err error) {
		for _, rp := range prefetched[child.ID].parents {
			r, err := fetch(rp)
			if err != nil {
				return nil, err
			}
			parents = append(parents, convert(r))
		}
		return parents, nil
	})
	if err != nil {
		return storage.Null, err
	}

	// Best effort for my personal use - might expose it someday.
	output, err := ioutil.TempFile("", "muscle*.dot")
	if err == nil {
		_, _ = fmt.Fprintln(output, graph)
		_ = output.Close()
		defer func() {
			if err := exec.Command("dot", "-O", "-Tpng", output.Name()).Run(); err == nil {
				fid, err := plumb.Open("send", p.OWRITE)
				if err != nil {
					log.Printf("Could not open plumb port %q for writing: %v", "send", err)
					return
				}
				msg := plumb.Message{
					Type: "text",
					Data: []byte(output.Name() + ".png"),
				}
				if err := msg.Send(fid); err != nil {
					log.Printf("Could not send %v: %v", &msg, err)
				}
				if err := fid.Close(); err != nil {
					log.Printf("Could not close %v: %v", fid, err)
				}
			}
		}()
	}

	return storage.NewPointerFromHex(base.ID)
}

// Fork writes the first revision for the target instance, using as root
// node the root node of the head revision of the source instance, and using
// as parent revision the head revision of the source instance. The target
// instance will therefore have the same contents as the source instance
// (at the current head revision) and will inherit all the history. Fork
// will try not to overwrite the target.
func (s *Store) Fork(source, target string) error {
	errw := func(e error) error {
		return fmt.Errorf("tree.Store.Fork: %w", e)
	}
	srev, sroot, err := s.RemoteRevision(source)
	if err != nil {
		return err
	}
	trev := NewRevision(target, sroot.Key(), []storage.Pointer{srev.key})
	if err := s.StoreRevision(trev); err != nil {
		return errw(err)
	}
	k := storage.Key(RemoteRootKeyPrefix + target)
	if _, err := s.pointers.Get(k); err == nil {
		return fmt.Errorf("target instance %q already exists", target)
	}
	return s.pointers.Put(k, []byte(trev.key.Hex()))
}
