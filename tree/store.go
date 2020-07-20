package tree

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/lionkov/go9p/p"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/pkg/errors"
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

// LocalBasePointer reads the file $HOME/lib/muscle/base, expecting
// to find a hex-encoded storage.Pointer that points to a revision.
func LocalBasePointer() (storage.Pointer, error) {
	pathname := os.ExpandEnv("$HOME/lib/muscle/base")
	if content, err := ioutil.ReadFile(pathname); os.IsNotExist(err) {
		return storage.Null, nil
	} else if err != nil {
		return storage.Null, errors.WithStack(err)
	} else {
		return storage.NewPointerFromHex(strings.TrimSpace(string(content)))
	}
}

// SetLocalBasePointer atomically updates $HOME/lib/muscle/base, and
// adds an entry to $HOME/lib/muscle/base.history for the previous
// base pointer.
func SetLocalBasePointer(pointer storage.Pointer) error {
	previous, err := LocalBasePointer()
	if err != nil {
		return err
	}
	pathname := os.ExpandEnv("$HOME/lib/muscle/base")
	if f, err := os.OpenFile(pathname+".history", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666); err != nil {
		return errors.WithStack(err)
	} else {
		_, werr := fmt.Fprintf(f, "%d	%s\n", time.Now().Unix(), previous.Hex())
		cerr := f.Close()
		if werr != nil {
			return errors.WithStack(werr)
		}
		if cerr != nil {
			return errors.WithStack(cerr)
		}
	}
	if err := ioutil.WriteFile(pathname+".new", []byte(pointer.Hex()), 0666); err != nil {
		return errors.WithStack(err)
	}
	if err := os.Rename(pathname+".new", pathname); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *Store) RemoteBasePointer() (storage.Pointer, error) {
	if content, err := s.pointers.Get(storage.Key(RemoteRootKeyPrefix + "base")); errors.Is(err, storage.ErrNotFound) {
		return storage.Null, nil
	} else if err != nil {
		return storage.Null, err
	} else {
		return storage.NewPointerFromHex(strings.TrimSpace(string(content)))
	}
}

func (s *Store) SetRemoteBasePointer(pointer storage.Pointer) error {
	return s.pointers.Put(storage.Key(RemoteRootKeyPrefix+"base"), []byte(pointer.Hex()))
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
