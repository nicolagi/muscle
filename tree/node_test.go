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

	assert.Equal(t, "", (*Node)(nil).Path())
}
