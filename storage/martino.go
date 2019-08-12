package storage

import (
	"errors"

	log "github.com/sirupsen/logrus"
)

// TODO This is another idiotic temporary (!!!) name.
//
// Martino is a multi-layer store.  It is content-addressable, in
// the sense that the key to any blob is the hash of its contents.
// It combines a staging area (disk store), a cache (disk store), a
// remote store (somewhere in the cloud).  All new blobs go to the
// staging area.  A request to read a blob first goes to the staging
// area.  If it is not present there, it is looked up in the cache.
// If it still isn't found, it is obtained from the remote store and
// copied to the cache. (Never to the staging area.) When a new
// revision of a tree is created, some blobs from the staging area
// will be copied to the cache and the remote store, others will be
// removed (being part of revisions that won't be uploaded).
type Martino struct {
	staging Enumerable
	store   Store
}

// NewMartino returns a new *Martino from the given storage layers.
func NewMartino(staging Enumerable, store Store) *Martino {
	m := new(Martino)
	m.staging = staging
	m.store = store
	return m
}

// Get fetches the data pointed to by the key. It will first try the
// staging area, then the cache. If that also fails, the data is
// fetched from the remote store and the local cache is populated so
// it will satisfy the next request for the same key.
func (m *Martino) Get(pointer Pointer) (contents []byte, err error) {
	contents, err = m.staging.Get(pointer.Key())
	if errors.Is(err, ErrNotFound) {
		contents, err = m.store.Get(pointer.Key())
	}
	return
}

// Put saves the given data to the staging area and returns its key.
// The key is a hash of the data.
func (m *Martino) Put(contents []byte) (pointer Pointer, err error) {
	pointer = PointerTo(contents)
	err = m.staging.Put(pointer.Key(), contents)
	return
}

// Commit uploads the blobs that should be persisted to the remote.
// It will try to add them to the cache as well and remove them from
// the staging area, but failures in these latter operations will
// not fail the commit.  Note that we should prevent operations on
// the underlying stores until the commit is finished. In practice,
// the file server should block all operations until Commit returns.
func (m *Martino) Commit(shouldPersist func(Key) bool) error {
	var examinedKeys []Key
	fatalErr := m.staging.ForEach(func(k Key) error {
		examinedKeys = append(examinedKeys, k)
		if !shouldPersist(k) {
			return nil
		}
		b, err := m.staging.Get(k)
		if err == nil {
			err = m.store.Put(k, b)
		}
		return err
	})
	if fatalErr == nil {
		// All the keys that needed to be uploaded to the
		// remote, were uploaded successfully.  They may
		// have failed entering the cache, but that's not a
		// fatal error.  At this point, we can go through
		// the examined keys and delete all of them from the
		// staging area, as they are either securely stored
		// remotely or garbage.
		for _, k := range examinedKeys {
			if e := m.staging.Delete(k); e != nil {
				log.WithFields(log.Fields{
					"key":   k,
					"cause": e.Error(),
				}).Warn("Failed deleting from staging area, will try at next commit")
			}
		}
	}
	return fatalErr
}

func (m *Martino) BelongsToStagingArea(k Pointer) (bool, error) {
	return m.staging.Contains(k.Key())
}

func (m *Martino) PutWithKey(k Pointer, v []byte) error {
	return m.staging.Put(k.Key(), v)
}
