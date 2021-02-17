package block

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
	"github.com/nicolagi/muscle/internal/storage"
)

func TestBlockTruncate(t *testing.T) {
	key := make([]byte, 16)
	rand.Read(key)
	factory, err := NewFactory(nil, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	bsize := 8192
	t.Run("extend or shrink happy path", func(t *testing.T) {
		f := func(initial []byte, finalSize uint8) bool {
			size := int(finalSize)
			b, err := factory.New(nil, bsize)
			if err != nil {
				t.Log(err)
				return false
			}
			if _, _, err := b.Write(initial, 0); err != nil {
				t.Log(err)
				return false
			}
			if err := b.Truncate(size); err != nil {
				t.Log(err)
				return false
			}
			final, err := b.ReadAll()
			if err != nil {
				t.Log(err)
				return false
			}
			if len(final) != size {
				return false
			}
			if size <= len(initial) {
				return bytes.Equal(initial[:size], final)
			}
			if !bytes.Equal(initial, final[:len(initial)]) {
				return false
			}
			for i := len(initial); i < len(final); i++ {
				if final[i] != 0 {
					return false
				}
			}
			return true
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
	t.Run("request more than block capacity leaves block unchanged and returns error", func(t *testing.T) {
		f := func(initial []byte, finalSizeInExcess uint8) bool {
			size := bsize + int(finalSizeInExcess) + 1
			b, err := factory.New(nil, bsize)
			if err != nil {
				t.Log(err)
				return false
			}
			if _, _, err := b.Write(initial, 0); err != nil {
				t.Log(err)
				return false
			}
			if err := b.Truncate(size); err == nil {
				return false // An error was expected.
			}
			final, err := b.ReadAll()
			if err != nil {
				t.Log(err)
				return false
			}
			return bytes.Equal(initial, final)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestBlockWriting(t *testing.T) {
	key := make([]byte, 16)
	factory, err := NewFactory(nil, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	bsize := 8192
	block, err := factory.New(nil, bsize)
	if err != nil {
		t.Fatal(err)
	}
	// Appends
	n, increase, err := block.Write([]byte("foo"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := 3; n != want {
		t.Errorf("got %d, want %d bytes written", n, want)
	}
	if want := 3; increase != want {
		t.Errorf("got %d, want %d-byte size increase", increase, want)
	}
	if diff := cmp.Diff("foo", string(block.value)); diff != "" {
		t.Error(diff)
	}

	// Overwrites
	n, increase, err = block.Write([]byte("bar"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := 3; n != want {
		t.Errorf("got %d, want %d bytes written", n, want)
	}
	if want := 0; increase != want {
		t.Errorf("got %d, want %d-byte size increase", increase, want)
	}
	if diff := cmp.Diff("bar", string(block.value)); diff != "" {
		t.Error(diff)
	}

	// Overwrites and appends
	n, increase, err = block.Write([]byte("foobar"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := 6; n != want {
		t.Errorf("got %d, want %d bytes written", n, want)
	}
	if want := 3; increase != want {
		t.Errorf("got %d, want %d-byte size increase", increase, want)
	}
	if diff := cmp.Diff("foobar", string(block.value)); diff != "" {
		t.Error(diff)
	}

	// Overwrites and appends, with offset
	n, increase, err = block.Write([]byte("lishness"), 3)
	if err != nil {
		t.Fatal(err)
	}
	if want := 8; n != want {
		t.Errorf("got %d, want %d bytes written", n, want)
	}
	if want := 5; increase != want {
		t.Errorf("got %d, want %d-byte size increase", increase, want)
	}
	if diff := cmp.Diff("foolishness", string(block.value)); diff != "" {
		t.Error(diff)
	}
}

func TestCorruptedBlockHandling(t *testing.T) {
	index := &storage.InMemory{}
	key := make([]byte, 16)
	rand.Read(key)
	factory, err := NewFactory(index, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("empty block", func(t *testing.T) {
		ref, err := NewRef(nil)
		if err != nil {
			t.Fatal(err)
		}
		err = index.Put(ref.Key(), []byte{})
		if err != nil {
			t.Fatal(err)
		}
		block, err := factory.New(ref, 8192)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 1)
		n, err := block.Read(buf, 0)
		if got, want := n, 0; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := err.Error(), "0 bytes"; !strings.Contains(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}
