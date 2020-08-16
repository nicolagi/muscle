package tree

import (
	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/srv"
)

type Dir = p.Dir

const (
	DMDIR = p.DMDIR
	QTDIR = p.QTDIR
)

var (
	Eexist    = srv.Eexist
	Enotempty = srv.Enotempty
	Eperm     = srv.Eperm
)
