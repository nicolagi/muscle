package p9util

import (
	"log"
	"os/user"

	"github.com/lionkov/go9p/p"
	"github.com/nicolagi/muscle/internal/tree"
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
	ni := node.Info()
	qid.Path = ni.ID
	qid.Version = ni.Version
	qid.Type = 0
	if ni.Mode&tree.DMDIR != 0 {
		qid.Type |= p.QTDIR
	}
	if ni.Mode&tree.DMEXCL != 0 {
		qid.Type |= p.QTEXCL
	}
	if ni.Mode&tree.DMAPPEND != 0 {
		qid.Type |= p.QTAPPEND
	}
}

func NodeDir(node *tree.Node) (dir p.Dir) {
	NodeDirVar(node, &dir)
	return
}

func NodeDirVar(node *tree.Node, dir *p.Dir) {
	ni := node.Info()
	dir.Qid.Path = ni.ID
	dir.Qid.Version = ni.Version
	dir.Qid.Type = 0
	if ni.Mode&tree.DMDIR != 0 {
		dir.Qid.Type |= p.QTDIR
	}
	if ni.Mode&tree.DMEXCL != 0 {
		dir.Qid.Type |= p.QTEXCL
	}
	if ni.Mode&tree.DMAPPEND != 0 {
		dir.Qid.Type |= p.QTAPPEND
	}
	dir.Uid = NodeUID
	dir.Gid = NodeGID
	dir.Length = ni.Size
	dir.Mode = ni.Mode
	dir.Mtime = ni.Modified
	dir.Atime = ni.Modified
	dir.Name = ni.Name
}
