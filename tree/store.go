package tree

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree/mergebase"
	log "github.com/sirupsen/logrus"
)

// Store is a high-level entity that takes care of loading and storing
// objects (nodes, revisions) from/to a store. Such operations require
// encryption/decryption, encoding/decoding, actual store put/get.
// It is built on top of the more basic functionality in muscle/storage.
type Store struct {
	localRootKeyFile string
	remoteRootKey    string
	ephemeral        storage.Enumerable
	permanent        storage.Store
	pointers         storage.Store
	cryptography     *cryptography
	codec            Codec
}

func NewStore(
	ephemeral storage.Enumerable,
	permanent storage.Store,
	pointers storage.Store,
	localRootKeyFile string,
	remoteRootKey string,
	key []byte,
) (*Store, error) {
	crp, err := newCryptography(key)
	if err != nil {
		return nil, err
	}
	return &Store{
		localRootKeyFile: localRootKeyFile,
		remoteRootKey:    remoteRootKey,
		ephemeral:        ephemeral,
		permanent:        permanent,
		pointers:         pointers,
		cryptography:     crp,
		codec:            newStandardCodec(),
	}, nil
}

// StoreBlock saves the block to the local store and updates its key to be the hash of the contents before encryption.
func (s *Store) StoreBlock(block *Block) error {
	block.pointer = storage.PointerTo(block.contents)
	encrypted, err := s.cryptography.encrypt(block.contents)
	if err == nil {
		err = s.ephemeral.Put(block.pointer.Key(), encrypted)
	}
	return err
}

func (s *Store) StoreNode(node *Node) error {
	entry := log.WithFields(log.Fields{
		"op":   "StoreNode",
		"node": node.String(),
	})
	encoded, err := s.codec.encodeNode(node)
	if err != nil {
		return err
	}
	encrypted, err := s.cryptography.encrypt(encoded)
	if err != nil {
		return err
	}
	node.pointer = storage.PointerTo(encoded)
	err = s.ephemeral.Put(node.pointer.Key(), encrypted)
	if err == nil {
		entry.Debug("Clearing dirty flag")
		node.dirty = false
		node.recomputeQID()
	}
	return err
}

// LoadBlock assumes block.key is current and loads the block contents from the store.
func (s *Store) LoadBlock(dst *Block) error {
	encrypted, err := s.ephemeral.Get(dst.pointer.Key())
	if errors.Is(err, storage.ErrNotFound) {
		encrypted, err = s.permanent.Get(dst.pointer.Key())
	}
	if err == nil {
		dst.contents = s.cryptography.decrypt(encrypted)
	}
	return err
}

func (s *Store) StoreRevision(r *Revision) error {
	encoded, err := s.codec.encodeRevision(r)
	if err != nil {
		return err
	}
	encrypted, err := s.cryptography.encrypt(encoded)
	if err != nil {
		return err
	}
	r.key = storage.PointerTo(encoded)
	return s.ephemeral.Put(r.key.Key(), encrypted)
}

// LoadNode assumes that dst.key is the destination node's key, and the parent pointer is also correct.
// Loading will overwrite any other data.
func (s *Store) LoadNode(dst *Node) error {
	contents, err := s.ephemeral.Get(dst.pointer.Key())
	if errors.Is(err, storage.ErrNotFound) {
		contents, err = s.permanent.Get(dst.pointer.Key())
	}
	if err == nil {
		err = s.codec.decodeNode(s.cryptography.decrypt(contents), dst)
	}
	dst.D.Uid = nodeUID
	dst.D.Gid = nodeGID
	dst.recomputeQID()
	return err
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

func (s *Store) encryptEncodeRevision(r *Revision) ([]byte, error) {
	encoded, err := s.codec.encodeRevision(r)
	if err != nil {
		return nil, err
	}
	return s.cryptography.encrypt(encoded)
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
	b, err := s.ephemeral.Get(key.Key())
	if errors.Is(err, storage.ErrNotFound) {
		b, err = s.permanent.Get(key.Key())
	}
	if err != nil {
		return nil, err
	}
	b = s.cryptography.decrypt(b)
	r := &Revision{key: key}
	err = s.codec.decodeRevision(b, r)
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
	output, err := ioutil.TempFile("", "*.dot")
	if err == nil {
		_, _ = fmt.Fprintln(output, graph)
		_ = output.Close()
		defer func() {
			if err := exec.Command("dot", "-O", "-Tsvg", output.Name()).Run(); err == nil {
				// A hack for myself, because I don't even want for this to be an option.
				if os.Getenv("MUSCLE_AUTO_OPEN_SVG") != "" {
					_ = exec.Command("firefox", output.Name()+".svg").Start()
				}
			}
		}()
	}

	return storage.NewPointerFromHex(base.ID)
}

// TODO I wish this method could be avoided.
func (s *Store) IsStaging(k storage.Pointer) (bool, error) {
	return s.ephemeral.Contains(k.Key())
}

// Fork writes the first revision for the target instance, using as root
// node the root node of the head revision of the source instance, and using
// as parent revision the head revision of the source instance. The target
// instance will therefore have the same contents as the source instance
// (at the current head revision) and will inherit all the history. Fork
// will try not to overwrite the target.
func (s *Store) Fork(source, target string) error {
	srev, sroot, err := s.RemoteRevision(source)
	if err != nil {
		return err
	}
	trev := NewRevision(target, sroot.Key(), []storage.Pointer{srev.key})
	data, err := s.encryptEncodeRevision(trev)
	if err != nil {
		return err
	}
	trev.key = storage.PointerTo(data)
	err = s.permanent.Put(trev.key.Key(), data)
	if err != nil {
		return err
	}
	k := storage.Key(RemoteRootKeyPrefix + target)
	if _, err := s.pointers.Get(k); err == nil {
		return fmt.Errorf("target instance %q already exists", target)
	}
	return s.pointers.Put(k, []byte(trev.key.Hex()))
}
