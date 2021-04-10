package tree

import (
	"fmt"
	"io"
	"path"
)

func (tree *Tree) DumpNodes(w io.Writer) {
	tree.dumpNodesFrom(w, nil, tree.root, "/")
}

func (tree *Tree) dumpNodesFrom(w io.Writer, werr error, node *Node, pathname string) {
	out := func(format string, a ...interface{}) {
		if werr == nil {
			_, werr = fmt.Fprintf(w, format, a...)
		}
	}
	out("%s bsize=%d pointer=%v", pathname, node.bsize, node.pointer)
	for _, c := range node.children {
		out(" childpointer=%v", c.pointer)
	}
	for _, blk := range node.blocks {
		out(" block=%v", blk.Ref())
	}
	_, _ = fmt.Fprintln(w)
	for _, c := range node.children {
		if c.flags&loaded != 0 {
			tree.dumpNodesFrom(w, werr, c, path.Join(pathname, c.info.Name))
		}
	}
}

func (tree *Tree) ListNodesInUse() (paths []string) {
	var list func(*Node, string)
	list = func(node *Node, prefix string) {
		if node.refs == 0 {
			return
		}
		p := path.Join(prefix, node.info.Name)
		paths = append(paths, p)
		for _, c := range node.children {
			list(c, p)
		}
	}
	list(tree.root, "")
	return
}
