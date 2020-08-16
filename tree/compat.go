package tree

import (
	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/srv"
)

type Qid struct {
	Path    uint64
	Type    uint8
	Version uint32
}

type Dir struct {
	Qid

	Atime  uint32
	Dev    uint32
	Gid    string
	Length uint64
	Mode   uint32
	Mtime  uint32
	Muid   string
	Name   string
	Size   uint16
	Type   uint16
	Uid    string
}

const (
	DMDIR = p.DMDIR
	QTDIR = p.QTDIR
)

var (
	Eexist    = srv.Eexist
	Enotempty = srv.Enotempty
	Eperm     = srv.Eperm
)
