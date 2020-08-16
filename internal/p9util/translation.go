package p9util

import (
	"log"
	"os/user"

	"github.com/lionkov/go9p/p"
	"github.com/nicolagi/muscle/tree"
)

var (
	NodeUID string
	NodeGID string
)

func init() {
	u, err := user.Current()
	if err != nil {
		log.Fatalf("could not get current user: %v", err)
	}
	NodeUID = u.Username
	g, err := user.LookupGroupId(u.Gid)
	if err != nil {
		log.Fatalf("could not get group %v: %v", u.Gid, err)
	}
	NodeGID = g.Name
}

func NodeQID(node *tree.Node) (qid p.Qid) {
	NodeQIDVar(node, &qid)
	return
}

func NodeQIDVar(node *tree.Node, qid *p.Qid) {
	qid.Path = node.D.Qid.Path
	qid.Version = node.D.Qid.Version
	if node.D.Mode&tree.DMDIR != 0 {
		qid.Type = p.QTDIR
	} else {
		qid.Type = 0
	}
}

func NodeDir(node *tree.Node) (dir p.Dir) {
	NodeDirVar(node, &dir)
	return
}

func NodeDirVar(node *tree.Node, dir *p.Dir) {
	NodeQIDVar(node, &dir.Qid)
	dir.Uid = NodeUID
	dir.Gid = NodeGID
	dir.Length = node.D.Size
	dir.Mode = node.D.Mode
	dir.Mtime = node.D.Modified
	dir.Atime = node.D.Modified
	dir.Name = node.D.Name
}
