package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
)

const (
	diskStoreDirPerm  = 0700
	diskStoreFilePerm = 0600
)

type DiskStore struct {
	dir string
}

func NewDiskStore(dir string) *DiskStore {
	return &DiskStore{dir: dir}
}

func (s *DiskStore) Get(k Key) (Value, error) {
	b, err := ioutil.ReadFile(s.pathFor(k))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("%q: %w", k, ErrNotFound)
	}
	return b, err
}

func (s *DiskStore) Put(k Key, v Value) error {
	p := s.pathFor(k)
	err := ioutil.WriteFile(p, v, diskStoreFilePerm)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err = os.MkdirAll(path.Dir(p), diskStoreDirPerm); err != nil {
			return err
		}
		return ioutil.WriteFile(p, v, diskStoreFilePerm)
	}
	return nil
}

func (s *DiskStore) Delete(k Key) error {
	err := os.Remove(s.pathFor(k))
	if err != nil {
		perr, ok := err.(*os.PathError)
		if ok {
			serr, ok := perr.Err.(syscall.Errno)
			if ok && serr == syscall.ENOENT {
				return errors.Wrapf(ErrNotFound, "could not delete %v", k)
			}
		}
	}
	return err
}

func (s *DiskStore) ForEach(cb func(Key) error) error {
	var kk []Key
	err := filepath.Walk(s.dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			kk = append(kk, Key(path.Base(p)))
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, k := range kk {
		if err := cb(k); err != nil {
			return err
		}
	}
	return nil
}

func (s *DiskStore) Contains(k Key) (bool, error) {
	_, err := os.Stat(s.pathFor(k))
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (s *DiskStore) pathFor(key Key) string {
	k := string(key)
	return path.Join(s.dir, k[:2], k)
}
