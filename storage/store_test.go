package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

// Generate implements quick.Generator.
func (Key) Generate(rand *rand.Rand, size int) reflect.Value {
	if size <= 0 {
		size = 1
	}
	b := make([]byte, size)
	n, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	if n != size {
		panic(fmt.Sprintf("got %d, want %d random bytes", n, size))
	}
	return reflect.ValueOf(Key(fmt.Sprintf("%02x", b)))
}

func TestRandomKey(t *testing.T) {
	t.Run("random keys are distinct", func(t *testing.T) {
		f := func() bool {
			k1, err := RandomKey(16)
			if err != nil {
				t.Log(err)
				return false
			}
			k2, err := RandomKey(16)
			if err != nil {
				t.Log(err)
				return false
			}
			return k1 != k2
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	t.Run("random keys are of the required size", func(t *testing.T) {
		f := func(size uint8) bool {
			key, err := RandomKey(size)
			if err != nil {
				t.Log(err)
				return false
			}
			return len(key) == 2*int(size)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}
