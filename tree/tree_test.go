package tree

import (
	"errors"
	"testing"

	"github.com/nicolagi/muscle/config"
	"github.com/stretchr/testify/assert"
)

func TestTreeAdd(t *testing.T) {
	t.Run("added node's owner and group are inherited from the process", func(t *testing.T) {
		t.Logf("nodeUID=%s nodeGID=%s", nodeUID, nodeGID)
		if nodeUID == "" || nodeGID == "" {
			t.Fatal("nodeUID or nodeGID not set")
		}
		parent := &Node{}
		tree := newTestTree(t)
		child, err := tree.Add(parent, "file", 0666)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := child.D.Owner, nodeUID; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestTreeRemove(t *testing.T) {
	t.Run("removing the root is not allowed", func(t *testing.T) {
		tree := newTestTree(t)
		// The root is just the directory with no parent.
		parent := &Node{}
		parent.D.Mode = DMDIR
		err := tree.Remove(parent)
		if !errors.Is(err, ErrPermission) {
			t.Errorf("got %v, want a wrapper of %v", err, ErrPermission)
		}
	})
	t.Run("removing a non-empty directory is not allowed", func(t *testing.T) {
		tree := newTestTree(t)
		parent := &Node{}
		if _, err := tree.Add(parent, "file", 0666); err != nil {
			t.Fatal(err)
		}
		err := tree.Remove(parent)
		if !errors.Is(err, ErrNotEmpty) {
			t.Errorf("got %v, want a wrapper of %v", err, ErrNotEmpty)
		}
	})
}

func TestTreeWalking(t *testing.T) {
	tree := newTestTree(t)
	_, root := tree.Root()
	assert.Equal(t, root.Path(), "root")

	// Walk to inexistent file.
	nodes, err := tree.Walk(root, "foo")
	assert.Len(t, nodes, 0)
	assert.True(t, errors.Is(err, ErrNotFound))

	// Add such file.
	child, _ := tree.Add(root, "foo", 0700)
	assert.Equal(t, child.Path(), "root/foo")
	assert.Equal(t, child.parent, root)
	assert.Equal(t, child.D.Name, "foo")

	// Walk to it.
	nodes, err = tree.Walk(root, "foo")
	assert.Len(t, nodes, 1)
	assert.Nil(t, err)
	assert.Equal(t, nodes[0], child)

	// Walk to non-existent from child.
	nodes, err = tree.Walk(child, "bar")
	assert.Len(t, nodes, 0)
	assert.True(t, errors.Is(err, ErrNotFound))

	// Walk to non-existent from root.
	nodes, err = tree.Walk(root, "foo", "bar")
	assert.Len(t, nodes, 1)
	assert.True(t, errors.Is(err, ErrNotFound))
	assert.Equal(t, nodes[0], child)

	// Create nested child and walk to it.
	nested, _ := tree.Add(child, "bar", 0700)
	assert.Equal(t, "root/foo/bar", nested.Path())
	nodes, err = tree.Walk(root, "foo", "bar")
	assert.Nil(t, err)
	assert.Len(t, nodes, 2)
	assert.Equal(t, "root/foo", nodes[0].Path())
	assert.Equal(t, "root/foo/bar", nodes[1].Path())
}

func newTestTree(t *testing.T) *Tree {
	tree, err := NewFactory(newTestBlockFactory(t), newTestStore(t), &config.C{
		BlockSize: 8192,
	}).NewTree()
	if err != nil {
		t.Fatal(err)
	}
	return tree
}
