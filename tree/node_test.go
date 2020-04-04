package tree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeFlagsString(t *testing.T) {
	testCases := []struct {
		input  nodeFlags
		output string
	}{
		{0, "none"},
		{loaded, "loaded"},
		{dirty, "dirty"},
		{sealed, "sealed"},
		{loaded | dirty, "loaded,dirty"},
		{42, "dirty,extraneous"},
	}
	for _, tc := range testCases {
		if got, want := tc.input.String(), tc.output; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

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
