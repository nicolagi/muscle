package diff_test

import (
	"testing"

	"github.com/nicolagi/muscle/diff"
	"github.com/stretchr/testify/assert"
)

func TestByteNodeSameAs(t *testing.T) {
	a := diff.ByteNode("some text")
	b := diff.ByteNode("other text")
	assert.True(t, !a.SameAs(b))
	assert.True(t, !b.SameAs(a))
	assert.True(t, a.SameAs(a))
	assert.True(t, b.SameAs(b))
	assert.True(t, a.SameAs(diff.ByteNode("some text")))
	assert.True(t, diff.ByteNode("some text").SameAs(a))
	assert.True(t, !a.SameAs(nil))
	assert.True(t, !a.SameAs(diff.StringNode("some text")))
}

func TestByteNodeContent(t *testing.T) {
	node := diff.ByteNode("some text")
	content, err := node.Content()
	assert.Equal(t, "some text", content)
	assert.Nil(t, err)
}

func TestStringNodeSameAs(t *testing.T) {
	a := diff.StringNode("some text")
	b := diff.StringNode("other text")
	assert.True(t, !a.SameAs(b))
	assert.True(t, !b.SameAs(a))
	assert.True(t, a.SameAs(a))
	assert.True(t, b.SameAs(b))
	assert.True(t, a.SameAs(diff.StringNode("some text")))
	assert.True(t, diff.StringNode("some text").SameAs(a))
	assert.True(t, !a.SameAs(nil))
	assert.True(t, !a.SameAs(diff.ByteNode{}))
}

func TestStringNodeContent(t *testing.T) {
	node := diff.StringNode("some text")
	content, err := node.Content()
	assert.Equal(t, "some text", content)
	assert.Nil(t, err)
}
