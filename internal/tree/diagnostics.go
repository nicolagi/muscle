package tree

import (
	"bytes"
	"log"
	"path"
)

func (tree *Tree) DumpNodes() {
	tree.dumpNodesFrom(tree.root, "/")
}

func (tree *Tree) dumpNodesFrom(node *Node, pathname string) {
	b := bytes.NewBufferString(pathname)
	b.WriteString(" pointer=")
	b.WriteString(node.pointer.String())
	for _, c := range node.children {
		b.WriteString(" childpointer=")
		b.WriteString(c.pointer.String())
	}
	for _, blk := range node.blocks {
		b.WriteString(" block=")
		b.WriteString(blk.Ref().String())
	}
	log.Print(b.String())
	for _, c := range node.children {
		if c.flags&loaded != 0 {
			tree.dumpNodesFrom(c, path.Join(pathname, c.info.Name))
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
