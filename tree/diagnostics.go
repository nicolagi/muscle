package tree

import (
	"fmt"
	"path"
	"time"

	log "github.com/sirupsen/logrus"
)

func (tree *Tree) DumpNodes() {
	tree.dumpNodesFrom(tree.root, fmt.Sprintf("%d", time.Now().Unix()), "")
}

func (tree *Tree) dumpNodesFrom(node *Node, absolutePrefix, pathPrefix string) {
	p := path.Join(pathPrefix, node.D.Name)
	log.WithFields(log.Fields{
		"prefix": absolutePrefix,
		"path":   p,
		"node":   node,
	}).Info("Node dump")
	for _, c := range node.children {
		if c.flags&loaded != 0 {
			tree.dumpNodesFrom(c, absolutePrefix, p)
		}
	}
}

func (tree *Tree) ListNodesInUse() (paths []string) {
	var list func(*Node, string)
	list = func(node *Node, prefix string) {
		if node.refs == 0 {
			return
		}
		p := path.Join(prefix, node.D.Name)
		paths = append(paths, p)
		for _, c := range node.children {
			list(c, p)
		}
	}
	list(tree.root, "")
	return
}
