package tree

import (
	"fmt"
	"testing"

	"github.com/nicolagi/muscle/internal/storage"
)

func TestCodec15(t *testing.T) {
	// Derived from decommissioned method to encode a Revision.
	// Had to add revParent because rev.Parent does not exist now!
	encode := func(rev *Revision, revParent storage.Pointer) []byte {
		size := 16 + len(rev.host)
		if !rev.rootKey.IsNull() {
			size += int(rev.rootKey.Len())
		}
		if !revParent.IsNull() {
			size += int(revParent.Len())
		}
		buf := make([]byte, size)
		ptr := buf
		ptr = pint8(15, ptr)
		if rev.rootKey.IsNull() {
			ptr = pint8(0, ptr)
		} else {
			ptr = pint8(rev.rootKey.Len(), ptr)
			ptr = pbytes(rev.rootKey.Bytes(), ptr)
		}
		ptr = pint8(1, ptr) /* only one parent */
		if revParent.IsNull() {
			ptr = pint8(0, ptr)
		} else {
			ptr = pint8(revParent.Len(), ptr)
			ptr = pbytes(revParent.Bytes(), ptr)
		}
		ptr = pint64(uint64(rev.when), ptr)
		ptr = pstr(rev.host, ptr)
		// Empty instance field, we don't want it anymore.
		ptr = pstr("", ptr)
		if len(ptr) != 0 {
			panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
		}
		return buf
	}
	t.Run(`the nil parent pointer of a revision is deserialized into nil parents slice`, func(t *testing.T) {
		encoded := encode(&Revision{}, storage.Null)
		var r Revision
		err := codecV15{}.decodeRevision(encoded[1:], &r)
		if err != nil {
			t.Error(err)
		}
		if r.parents != nil {
			t.Errorf("non-nil parents: %v", r.parents)
		}
	})
	t.Run(`the  non-nil parent pointer of a revision is deserialized into a single tag named "base"`, func(t *testing.T) {
		revParent := storage.RandomPointer()
		encoded := encode(&Revision{}, revParent)
		var r Revision
		err := codecV15{}.decodeRevision(encoded[1:], &r)
		if err != nil {
			t.Error(err)
		}
		if got := len(r.parents); got != 1 {
			t.Fatalf("got %d, want 1 parent", got)
		}
		tag := r.parents[0]
		if got, want := tag.Name, "base"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tag.Pointer.Hex(), revParent.Hex(); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}
