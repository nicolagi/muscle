package tree

import (
	"testing"

	"github.com/nicolagi/muscle/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestMarkDirty(t *testing.T) {
	t.Run("nil node", func(t *testing.T) {
		(*Node)(nil).markDirty()
	})
	t.Run("node with no parent", func(t *testing.T) {
		a := new(Node)
		a.markDirty()
		assert.Equal(t, dirty, a.flags&dirty)
		assert.Nil(t, a.pointer)
	})
	t.Run("node with parent", func(t *testing.T) {
		inner := &Node{flags: loaded}
		inner.info.Name = "inner"
		outer := &Node{flags: loaded}
		if err := outer.addChild(inner); err != nil {
			t.Fatal(err)
		}
		inner.markDirty()
		assert.Equal(t, dirty, inner.flags&dirty)
		assert.Equal(t, dirty, outer.flags&dirty)
		assert.Nil(t, inner.pointer)
		assert.Nil(t, outer.pointer)
	})
	t.Run("node that already has a key", func(t *testing.T) {
		expected := storage.RandomPointer()
		a := new(Node)
		a.pointer = expected
		a.markDirty()
		assert.Equal(t, dirty, a.flags&dirty)
		assert.Equal(t, expected, a.pointer)
	})
}
