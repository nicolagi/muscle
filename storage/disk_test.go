package storage

import (
	"bytes"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
)

func TestDiskStore(t *testing.T) {
	t.Run("you get what you put", func(t *testing.T) {
		store := NewDiskStore(t.TempDir())
		f := func(key Key, value Value) bool {
			err := store.Put(key, value)
			if err != nil {
				t.Fatal(err)
			}
			v, err := store.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			return bytes.Equal(v, value)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	t.Run("should not get a deleted key", func(t *testing.T) {
		store := NewDiskStore(t.TempDir())
		f := func(key Key, value Value) bool {
			err := store.Put(key, value)
			if err != nil {
				t.Fatal(err)
			}
			err = store.Delete(key)
			if err != nil {
				t.Fatal(err)
			}
			v, err := store.Get(key)
			vok := v == nil
			eok := errors.Is(err, ErrNotFound)
			if !eok {
				t.Errorf("got %v of type %T, want wrapper of %v", err, err, ErrNotFound)
			}
			return vok && eok
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	t.Run("delete inexistent key gives ErrNotFound", func(t *testing.T) {
		store := NewDiskStore(t.TempDir())
		f := func(key Key) bool {
			err := store.Delete(key)
			ok := errors.Is(err, ErrNotFound)
			if !ok {
				t.Errorf("got %v of type %T, want wrapper of %v", err, err, ErrNotFound)
			}
			return ok
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	t.Run("contains keys that were put", func(t *testing.T) {
		store := NewDiskStore(t.TempDir())
		f := func(key Key, value Value) bool {
			ok, err := store.Contains(key)
			if err != nil {
				t.Error(err)
				return false
			}
			if ok {
				return false
			}
			err = store.Put(key, value)
			if err != nil {
				t.Error(err)
				return false
			}
			ok, err = store.Contains(key)
			if err != nil {
				t.Error(err)
				return false
			}
			return ok
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	// Using PBT to catch a %02x vs %x bug.
	// Could easily do it with an oracle test but I wanted to do this exercise.
	t.Run("generated path has the right length", func(t *testing.T) {
		store := NewDiskStore("dir")
		f := func(key Key) bool {
			// 3 + 1 (slash) + 2 (first byte) + 1 (slash) + 64 (all bytes)
			return len(store.pathFor(RandomPointer().Key())) == 71
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	t.Run("iterates over all keys, without repetition", func(t *testing.T) {
		store := NewDiskStore(t.TempDir())
		f := func(keylist []Key, value Value) bool {
			keys := make(map[Key]int)
			for _, key := range keylist {
				keys[key] = 1
			}
			for key := range keys {
				if err := store.Put(key, value); err != nil {
					t.Error(err)
					return false
				}
			}
			seen := make(map[Key]int)
			err := store.ForEach(func(key Key) error {
				seen[key]++
				return store.Delete(key)
			})
			if err != nil {
				t.Error(err)
				return false
			}
			if diff := cmp.Diff(keys, seen); diff != "" {
				t.Log(diff)
				return false
			}
			return true
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}
