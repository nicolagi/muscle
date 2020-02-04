package storage

import (
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
