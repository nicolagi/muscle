package tree

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/muscle/storage"
)

// Store is a high-level entity that takes care of loading and storing
// objects (nodes, revisions) from/to a store. Such operations require
// encryption/decryption, encoding/decoding, actual store put/get.
// It is built on top of the more basic functionality in muscle/storage.
type Store struct {
	localRootKeyFile string
	remoteRootKey    string
	martino          *storage.Martino
	remote           storage.Store
	cryptography     *cryptography
	codec            Codec
}

func NewStore(
	martino *storage.Martino,
	remote storage.Store,
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
		martino:          martino,
		remote:           remote,
		cryptography:     crp,
		codec:            newStandardCodec(),
	}, nil
}

// StoreBlock saves the block to the local store and updates its key to be the hash of the contents before encryption.
func (s *Store) StoreBlock(block *Block) error {
	block.pointer = storage.PointerTo(block.contents)
	encrypted, err := s.cryptography.encrypt(block.contents)
	if err == nil {
		err = s.martino.PutWithKey(block.pointer, encrypted)
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
	err = s.martino.PutWithKey(node.pointer, encrypted)
	if err == nil {
		entry.Debug("Clearing dirty flag")
		node.dirty = false
		node.recomputeQID()
	}
	return err
}

// LoadBlock assumes block.key is current and loads the block contents from the store.
func (s *Store) LoadBlock(dst *Block) error {
	encrypted, err := s.martino.Get(dst.pointer)
	if err == nil {
		dst.contents = s.cryptography.decrypt(encrypted)
	}
	return err
}

func (s *Store) LoadRevision(dst *Revision) error {
	encrypted, err := s.martino.Get(dst.key)
	if err == nil {
		err = s.codec.decodeRevision(s.cryptography.decrypt(encrypted), dst)
	}
	log.WithField("key", dst.key.Hex()).Info("Loaded revision")
	return err
}

// LoadNode assumes that dst.key is the destination node's key, and the parent pointer is also correct.
// Loading will overwrite any other data.
func (s *Store) LoadNode(dst *Node) error {
	contents, err := s.martino.Get(dst.pointer)
	if err == nil {
		err = s.codec.decodeNode(s.cryptography.decrypt(contents), dst)
	}
	dst.D.Uid = nodeUID
	dst.D.Gid = nodeGID
	dst.recomputeQID()
	return err
}

func (s *Store) PushRevisionLocally(r *Revision) error {
	encrypted, err := s.encryptEncodeRevision(r)
	if err != nil {
		return err
	}
	r.key, err = s.martino.Put(encrypted)
	if err != nil {
		return err
	}
	log.WithField("key", r.key.Hex()).Info("Attempting to store new local revision")
	tmp := fmt.Sprintf("%s.%d", s.localRootKeyFile, time.Now().Unix())
	err = ioutil.WriteFile(tmp, []byte(r.key.Hex()), 0600)
	if err != nil {
		return err
	}
	return os.Rename(tmp, s.localRootKeyFile)
}

// PushRevisionRemotely encodes, encrypts, hashes, stores to the remote,
// updates the remote root key to point to the the new revision.
func (s *Store) PushRevisionRemotely(r *Revision) error {
	encrypted, err := s.encryptEncodeRevision(r)
	if err != nil {
		return err
	}
	r.key = storage.PointerTo(encrypted)
	err = s.remote.Put(r.key.Key(), encrypted)
	if err != nil {
		return err
	}
	// TODO These remote root keys are the only keys that aren't hashes of something,
	// and it bothers me.
	return s.remote.Put(storage.Key(s.remoteRootKey), []byte(r.key.Hex()))
}

func (s *Store) encryptEncodeRevision(r *Revision) ([]byte, error) {
	encoded, err := s.codec.encodeRevision(r)
	if err != nil {
		return nil, err
	}
	return s.cryptography.encrypt(encoded)
}

func (s *Store) LocalRevisionKey() (storage.Pointer, error) {
	b, err := ioutil.ReadFile(s.localRootKeyFile)
	if os.IsNotExist(err) {
		return storage.Null, nil
	}
	return storage.NewPointerFromHex(string(b))
}

func (s *Store) LocalRevision() (*Revision, *Node, error) {
	key, err := s.LocalRevisionKey()
	if err != nil {
		return nil, nil, err
	}
	return s.loadRevisionAndRoot(key)
}

func (s *Store) RemoteRevisionKey(instance string) (storage.Pointer, error) {
	var rootDescriptor string
	// TODO This special treatment of the argument should be avoided.
	if instance == "" {
		rootDescriptor = s.remoteRootKey
	} else {
		rootDescriptor = RemoteRootKeyPrefix + instance
	}
	rootContents, err := s.remote.Get(storage.Key(rootDescriptor))
	if err != nil {
		return storage.Null, err
	}
	return storage.NewPointerFromHex(string(rootContents))
}

func (s *Store) RemoteRevision(instance string) (*Revision, *Node, error) {
	key, err := s.RemoteRevisionKey(instance)
	if err != nil {
		return nil, nil, err
	}
	return s.loadRevisionAndRoot(key)
}

func (s *Store) loadRevisionAndRoot(key storage.Pointer) (*Revision, *Node, error) {
	revision := &Revision{key: key}
	if err := s.LoadRevision(revision); err != nil {
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

func (s *Store) History(maxRevisions int, key storage.Pointer) (rr []*Revision, err error) {
	if key.IsNull() {
		return nil, nil
	}
	r, _, err := s.loadRevisionAndRoot(key)
	if err != nil {
		return nil, err
	}
	return s.history(r, maxRevisions)
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
		r = &Revision{key: pk}
		err = s.LoadRevision(r)
		if err != nil {
			break
		}
	}
	return
}

func (s *Store) MergeBase(a, b storage.Pointer) (ancestorRevisionKey storage.Pointer, err error) {
	ar := &Revision{key: a}
	br := &Revision{key: b}
	err = s.LoadRevision(ar)
	if err != nil {
		return storage.Null, err
	}
	err = s.LoadRevision(br)
	if err != nil {
		return storage.Null, err
	}
	output, _ := ioutil.TempFile("", "*.dot")
	fmt.Fprintln(output, "digraph {")
	defer func() {
		fmt.Fprintln(output, "}")
		output.Close()
		// TODO In the graphs generated below I see duplicated edges in some cases.
		// That means the algorithm is not pruning the search, and it should.
		// Not a problem for performance at the moment, but
		// it makes the resulting diagram more complicated than it needs to be.
		// And with 4 participating hosts, it's a bit complicated.
		// Best effort.
		if err := exec.Command("dot", "-O", "-Tsvg", output.Name()).Run(); err == nil {
			// A hack for myself, because I don't even want for this to be an option.
			if os.Getenv("MUSCLE_AUTO_OPEN_SVG") != "" {
				_ = exec.Command("firefox", output.Name()+".svg").Start()
			}
		}
	}()
	return s.findAncestor(ar, br, output)
}

// TODO I wish this method could be avoided.
func (s *Store) IsStaging(k storage.Pointer) (bool, error) {
	return s.martino.BelongsToStagingArea(k)
}
