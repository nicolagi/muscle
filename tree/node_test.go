package tree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodePath(t *testing.T) {
	a := new(Node)
	b := new(Node)
	c := new(Node)

	a.D.Name = "root"
	b.D.Name = "child"
	c.D.Name = "rosemary"

	a.add(b)
	b.add(c)

	assert.Equal(t, "root", a.Path())
	assert.Equal(t, "root/child", b.Path())
	assert.Equal(t, "root/child/rosemary", c.Path())

	nodes, ok := a.walk("child", "rosemary")
	assert.True(t, ok)
	assert.Equal(t, []*Node{b, c}, nodes)

	nodes, ok = a.walk("kid")
	assert.False(t, ok)
	assert.Len(t, nodes, 0)

	nodes, ok = a.walk("child", "sage")
	assert.False(t, ok)
	assert.Equal(t, []*Node{b}, nodes)

	assert.Equal(t, "", (*Node)(nil).Path())
}
