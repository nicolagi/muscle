package tree

import "fmt"

type Qid struct {
	Path    uint64
	Version uint32
}

type Dir struct {
	Qid

	Length uint64
	Mode   uint32
	Mtime  uint32
	Name   string
	Uid    string
}

const (
	DMDIR = 0x80000000
)

var (
	ErrExists     = fmt.Errorf("file already exists")
	ErrNotEmpty   = fmt.Errorf("directory not empty")
	ErrPermission = fmt.Errorf("permission denied")
)
