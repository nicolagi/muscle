package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

// storeFuncs implements Store.
// Its behavior is fully configurable by setting get, put, delete functions.
// Intended for unit tests in this package.
type storeFuncs struct {
	get    func(Key) (Value, error)
	put    func(Key, Value) error
	delete func(Key) error
}

func (s storeFuncs) Get(key Key) (Value, error) {
	if s.get != nil {
		return s.get(key)
	}
	return nil, nil
}

func (s storeFuncs) Put(key Key, value Value) error {
	if s.put != nil {
		return s.put(key, value)
	}
	return nil
}
func (s storeFuncs) Delete(key Key) error {
	if s.delete != nil {
		return s.delete(key)
	}
	return nil
}

// Generate implements quick.Generator.
// Intended for unit tests in this package.
func (Key) Generate(rand *rand.Rand, size int) reflect.Value {
	return reflect.ValueOf(generateKey(rand, size))
}

func generateKey(r *rand.Rand, size int) Key {
	if size == 0 {
		return Key("")
	}
	if size < 0 {
		size = -size
	}
	b := make([]byte, size)
	var n int
	var err error
	if r != nil {
		n, err = r.Read(b)
	} else {
		n, err = rand.Read(b)
	}
	if err != nil {
		panic(err)
	}
	if n != size {
		panic(fmt.Sprintf("got %d, want %d random bytes", n, size))
	}
	// Note to self: would return length 2 for size 0!
	return Key(fmt.Sprintf("%02x", b))
}

func randomKey(size int) Key {
	return generateKey(nil, size)
}

func TestKeyGenerate(t *testing.T) {
	t.Run("random keys are distinct", func(t *testing.T) {
		f := func(k1, k2 Key) bool {
			return k1 != k2
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	t.Run("random keys are of the required size", func(t *testing.T) {
		f := func(smallSize uint8) bool {
			size := int(smallSize)
			key := randomKey(size)
			return len(key) == 2*size
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}
