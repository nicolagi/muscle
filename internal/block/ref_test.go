package block_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/nicolagi/muscle/internal/block"
)

func TestRefBytesReturnsCopy(t *testing.T) {
	test1 := func(t *testing.T, ref block.Ref) {
		p := ref.Bytes()
		for i := 0; i < len(p); i++ {
			p[i] = 42
		}
		q := ref.Bytes()
		if bytes.Equal(p, q) {
			t.Error("could modify reference via returned bytes")
		}
	}
	t.Run("for an index reference", func(t *testing.T) {
		ref, err := block.NewRef(nil)
		if err != nil {
			t.Fatal(err)
		}
		test1(t, ref)
	})
	t.Run("for a repository reference", func(t *testing.T) {
		b := make([]byte, 32)
		rand.Read(b)
		test1(t, block.RefOf(b))
	})
}
