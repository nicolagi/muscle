package tree

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/debug"
	"github.com/nicolagi/muscle/internal/storage"
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
	blockFactory *block.Factory
	pointers     storage.Store
	codec        Codec
	baseDir      string // e.g., $HOME/lib/muscle.
}

func NewStore(
	blockFactory *block.Factory,
	pointers storage.Store,
	baseDir string,
) (*Store, error) {
	return &Store{
		blockFactory: blockFactory,
		pointers:     pointers,
		codec:        newStandardCodec(),
		baseDir:      baseDir,
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
	if n, _, err := blk.Write(encoded, 0); err != nil {
		return errw(err)
	} else if n != len(encoded) {
		// We get here if we try to write a more than 32 GB file.
		return errw(fmt.Errorf("(actual) %d != (expected) %d", n, len(encoded)))
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
	debug.Assert(s.blockFactory != nil)
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
	// Once in a blue moon, a new bug manifests itself...
	if dst.info.Name == "" {
		b := make([]byte, 8)
		rand.Read(b)
		dst.info.Name = fmt.Sprintf("%x.%s", b, time.Now().UTC().Format(time.RFC3339))
	}
	dst.flags |= loaded
	return nil
}

// TODO: Belongs to musclefs, not to the tree package.
func (s *Store) updateLocalRootPointer(rootKey storage.Pointer) error {
	return setLocalPointer(filepath.Join(s.baseDir, "root"), rootKey)
}

// LocalBasePointer reads the file $HOME/lib/muscle/base, expecting
// to find a hex-encoded storage.Pointer that points to a revision.
func (s *Store) LocalBasePointer() (storage.Pointer, error) {
	pathname := filepath.Join(s.baseDir, "base")
	return localPointer(pathname)
}

func localPointer(pathname string) (storage.Pointer, error) {
	const method = "localPointer"
	content, err := ioutil.ReadFile(pathname)
	if os.IsNotExist(err) {
		return storage.Null, nil
	}
	if err != nil {
		return storage.Null, errorv(method, err)
	}
	p, err := storage.NewPointerFromHex(strings.TrimSpace(string(content)))
	if err != nil {
		return storage.Null, errorv(method, err)
	}
	return p, nil
}

// SetLocalBasePointer atomically updates $HOME/lib/muscle/base, and
// adds an entry to $HOME/lib/muscle/base.history for the previous
// base pointer.
func (s *Store) SetLocalBasePointer(pointer storage.Pointer) error {
	pathname := filepath.Join(s.baseDir, "base")
	return setLocalPointer(pathname, pointer)
}

func setLocalPointer(pathname string, pointer storage.Pointer) error {
	const method = "setLocalPointer"
	previous, err := localPointer(pathname)
	if err != nil {
		return errorv(method, err)
	}
	if f, err := os.OpenFile(pathname+".history", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666); err != nil {
		return errorv(method, err)
	} else {
		_, werr := fmt.Fprintf(f, "%d	%s\n", time.Now().Unix(), previous.Hex())
		cerr := f.Close()
		if werr != nil {
			return errorv(method, err)
		}
		if cerr != nil {
			return errorv(method, err)
		}
	}
	if err := ioutil.WriteFile(pathname+".new", []byte(pointer.Hex()), 0666); err != nil {
		return errorv(method, err)
	}
	if err := os.Rename(pathname+".new", pathname); err != nil {
		return errorv(method, err)
	}
	return nil
}

func (s *Store) RemoteTag(tagName string) (tag Tag, err error) {
	var tags []Tag
	tags, err = s.RemoteTags([]string{tagName})
	if err == nil {
		tag = tags[0]
	}
	return
}

func (s *Store) RemoteTags(tagNames []string) (tags []Tag, err error) {
	var method = "Store.RemoteTags"
	var p storage.Pointer
	for _, n := range tagNames {
		p, err = s.tagPointer(n)
		if err != nil {
			err = errorv(method, err)
			break
		}
		tags = append(tags, Tag{Name: n, Pointer: p})
	}
	return
}

func (s *Store) tagPointer(tag string) (storage.Pointer, error) {
	if content, err := s.pointers.Get(storage.Key(RemoteRootKeyPrefix + tag)); errors.Is(err, storage.ErrNotFound) {
		return storage.Null, nil
	} else if err != nil {
		return storage.Null, err
	} else {
		return storage.NewPointerFromHex(strings.TrimSpace(string(content)))
	}
}

// pointer must point to a revision.
func (s *Store) SetRemoteTags(tagNames []string, pointer storage.Pointer) error {
	const method = "Store.SetRemoteTags"
	value := []byte(pointer.Hex())
	for _, tagName := range tagNames {
		key := storage.Key(RemoteRootKeyPrefix + tagName)
		err := s.pointers.Put(key, value)
		if err != nil {
			return errorv(method, err)
		}
	}
	return nil
}

func (s *Store) LocalRootKey() (storage.Pointer, error) {
	return localPointer(filepath.Join(s.baseDir, "root"))
}

func (s *Store) LocalRoot() (*Node, error) {
	key, err := s.LocalRootKey()
	if err != nil {
		return nil, err
	}
	return s.loadRoot(key)
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

func (s *Store) loadRoot(key storage.Pointer) (*Node, error) {
	root := &Node{
		pointer: key,
	}
	if err := s.LoadNode(root); err != nil {
		return nil, err
	}
	return root, nil
}

func (s *Store) History(maxRevisions int, head *Revision, tagName string) (rr []*Revision, err error) {
	const method = "Store.History"
	for r := head; r != nil && maxRevisions > 0; maxRevisions-- {
		rr = append(rr, r)
		tag, ok := r.Parent(tagName)
		if !ok {
			break
		}
		r, err = s.LoadRevisionByKey(tag.Pointer)
		if err != nil {
			err = errorv(method, err)
			break
		}
	}
	return
}
