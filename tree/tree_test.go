package tree

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/stretchr/testify/assert"
)

func TestTreeWalking(t *testing.T) {
	tree, tempDir, err := scratchTree()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
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

func scratchTree() (*Tree, string, error) {
	tdir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, "", err
	}
	rootFile := path.Join(tdir, "root")
	cacheDir := path.Join(tdir, "cache")
	diskStore := storage.NewDiskStore(cacheDir)
	paired, err := storage.NewPaired(diskStore, nil, "/tmp/tree.test.paired.log")
	if err != nil {
		return nil, "", err
	}
	bf, err := block.NewFactory(diskStore, paired, []byte("ciaociaociaociao"))
	if err != nil {
		return nil, "", err
	}
	st, err := NewStore(
		bf,
		diskStore,
		paired,
		nil,
		rootFile,
		"remote.root.darkstar",
		[]byte("ciaociaociaociao"),
	)
	if err != nil {
		return nil, "", err
	}
	tt, err := NewFactory(bf, st, &config.C{
		BlockSize: 8192,
	}).NewTree()
	return tt, tdir, err
}
