package storage

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/nicolagi/muscle/internal/config"
	"github.com/pkg/errors"
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

func TestStoreImplementations(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*testing.T) (impl Store, teardown func())
	}{
		{
			"disk",
			func(t *testing.T) (impl Store, teardown func()) {
				impl = NewDiskStore(t.TempDir())
				return
			},
		},
		{
			"s3",
			func(t *testing.T) (impl Store, teardown func()) {
				if s3params == "" {
					t.Skip()
				}
				args := strings.Split(s3params, ",")
				if got, want := len(args), 4; got != want {
					t.Fatalf("got %d, want %d args for S3 store", got, want)
				}
				var err error
				impl, err = newS3Store(&config.C{
					S3Region:    args[0],
					S3Bucket:    args[1],
					S3AccessKey: args[2],
					S3SecretKey: args[3],
				})
				if err != nil {
					t.Fatal(err)
				}
				return
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			impl, teardown := c.setup(t)
			if teardown != nil {
				defer teardown()
			}
			testStore(t, impl)
		})
	}
}

var s3params string

func testStore(t *testing.T, impl Store) {
	t.Run("you get what you put", func(t *testing.T) {
		f := func(key Key, value Value) bool {
			err := impl.Put(key, value)
			if err != nil {
				t.Fatal(err)
			}
			v, err := impl.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			return bytes.Equal(v, value)
		}
		if err := quick.Check(f, &quick.Config{MaxCount: 10}); err != nil {
			t.Error(err)
		}
	})
	t.Run("should not get a deleted key", func(t *testing.T) {
		f := func(key Key, value Value) bool {
			err := impl.Put(key, value)
			if err != nil {
				t.Fatal(err)
			}
			err = impl.Delete(key)
			if err != nil {
				t.Fatal(err)
			}
			v, err := impl.Get(key)
			vok := v == nil
			eok := errors.Is(err, ErrNotFound)
			if !eok {
				t.Errorf("got %v of type %T, want wrapper of %v", err, err, ErrNotFound)
			}
			return vok && eok
		}
		if err := quick.Check(f, &quick.Config{MaxCount: 10}); err != nil {
			t.Error(err)
		}
	})
	t.Run("delete inexistent key is successful", func(t *testing.T) {
		f := func(key Key) bool {
			err := impl.Delete(key)
			if err != nil {
				t.Error(err)
				return false
			}
			return true
		}
		if err := quick.Check(f, &quick.Config{MaxCount: 10}); err != nil {
			t.Error(err)
		}
	})
}

func TestMain(m *testing.M) {
	flag.StringVar(&s3params, "s3", "", "region, bucket, access key, and secret key for S3 store testing")
	flag.Parse()
	os.Exit(m.Run())
}
