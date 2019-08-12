package storage // import "github.com/nicolagi/muscle/storage"

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
)

// Pointer is the SHA-256 of some blob stored somewhere.
// In that sense, it points to that blob.
type Pointer []byte

// Null is the hash pointer that can't be resolved.
var Null = Pointer(nil)

var ErrNotHashPointer = errors.New("not hash pointer")

func allZeros(bb []byte) bool {
	for _, b := range bb {
		if b != 0 {
			return false
		}
	}
	return true
}

// IsNull should be used instead of making explicit comparisons with
// Null.
func (p Pointer) IsNull() bool {
	return allZeros(p)
}

func (p Pointer) String() string {
	if p.IsNull() {
		return "Null"
	}
	return p.Hex()
}

// Hex returns the hex representation of the bytes making up the
// hash pointer (32 bytes, therefore 64 characters).
func (p Pointer) Hex() string {
	return hex.EncodeToString(p)
}

// Bytes returns the hash pointer as a byte slice rather than a hex
// string, so it's as small as possible.  This should be used in
// packing to storage. It should be treated as read-only, we own
// the byte slice.
func (p Pointer) Bytes() []byte {
	return p
}

func (p Pointer) Len() uint8 {
	return uint8(len(p))
}

// NewPointer is the inverse function to the Bytes method.
// This should be used in unpacking from storage.
// We don't own the passed slice so we'll make a copy.
func NewPointer(b []byte) Pointer {
	if allZeros(b) {
		return Null
	}
	c := make([]byte, len(b))
	copy(c, b)
	return Pointer(c)
}

// RandomPointer returns a hash pointer that does not point to anything known.
// It is used to assign an initial key to a brand new child node.
func RandomPointer() Pointer {
	b := make([]byte, 32)
	rand.Read(b)
	return Pointer(b)
}

func PointerTo(value []byte) Pointer {
	hash := sha256.New()
	hash.Write(value)
	sum := hash.Sum(nil)
	return Pointer(sum)
}

// NewPointerFromHex interprets a hex string as a hash pointer.
func NewPointerFromHex(hexDigits string) (Pointer, error) {
	b, err := hex.DecodeString(hexDigits)
	if len(b) != 32 || err != nil {
		return Null, fmt.Errorf("%q: %w", hexDigits, ErrNotHashPointer)
	}
	return Pointer(b), nil
}

func (p Pointer) Equals(q Pointer) bool {
	if len(p) != len(q) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] != q[i] {
			return false
		}
	}
	return true
}

func (p Pointer) Key() Key {
	return Key(p.Hex())
}

func (p Pointer) Value() interface{} {
	return Value(p)
}
