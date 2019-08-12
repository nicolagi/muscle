package tree

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/nicolagi/muscle/storage"
	"github.com/stretchr/testify/assert"
)

func TestBlockWriting(t *testing.T) {
	block := &Block{}

	// Appends
	block.write([]byte("foo"), 0)
	assert.Equal(t, "foo", string(block.contents))

	// Overwrites
	block.write([]byte("bar"), 0)
	assert.Equal(t, "bar", string(block.contents))

	// Overwrites and appends
	block.write([]byte("foobar"), 0)
	assert.Equal(t, "foobar", string(block.contents))

	// Overwrites and appends, with offset
	block.write([]byte("lishness"), 3)
	assert.Equal(t, "foolishness", string(block.contents))
}

func readAll(node *Node) string {
	sz := 0
	for _, block := range node.blocks {
		sz += len(block.contents)
	}
	b := make([]byte, sz)
	node.read(b, 0)
	return string(b)
}

func TestNodeWriting(t *testing.T) {
	pbs := blockSizeBytes
	blockSizeBytes = 6
	defer func() {
		blockSizeBytes = pbs
	}()
	node := &Node{}
	tree := Tree{root: node}

	// Append first block and to first block.
	tree.WriteAt(node, []byte("foo"), 0)
	assert.Equal(t, "foo", readAll(node))
	assert.Len(t, node.blocks, 1)

	// Cross blocks
	s := "012345012345012345"
	tree.WriteAt(node, []byte(s), 0)
	assert.Equal(t, s, readAll(node))
	assert.Len(t, node.blocks, 3)
	for _, b := range node.blocks {
		assert.Equal(t, "012345", string(b.contents))
	}

	// Overwrite one block.
	tree.WriteAt(node, []byte("xxxxxx"), 6)
	assert.Equal(t, "012345xxxxxx012345", readAll(node))
	assert.Len(t, node.blocks, 3)

	// Cross-block overwrite.
	tree.WriteAt(node, []byte("yyyyyy"), 9)
	assert.Equal(t, "012345xxxyyyyyy345", readAll(node))
	assert.Len(t, node.blocks, 3)
	assert.Equal(t, "012345", string(node.blocks[0].contents))
	assert.Equal(t, "xxxyyy", string(node.blocks[1].contents))
	assert.Equal(t, "yyy345", string(node.blocks[2].contents))

	t.Run("writing on block that is not loaded", func(t *testing.T) {
		oak, err := newTree(nil, storage.Null, true)
		assert.Nil(t, err)

		// Pretend something was on the root node (which is a dir, here treating it like a file).
		oak.root.blocks = append(oak.root.blocks, &Block{pointer: storage.RandomPointer()})

		loader := &dummyBlockLoader{}
		loader.cannedContents = []byte("whiteboard")
		assert.Nil(t, oak.root.writeAt(loader, []byte("black"), 0))
		assert.Equal(t, "blackboard", string(oak.root.blocks[0].contents))
	})
}

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
	blobStore := storage.NewMartino(diskStore, paired)
	st, err := NewStore(
		blobStore,
		nil,
		rootFile,
		"remote.root.darkstar",
		[]byte("ciaociaociaociao"),
	)
	if err != nil {
		return nil, "", err
	}
	tt, err := newTree(st, storage.Null, true)
	return tt, tdir, err
}
