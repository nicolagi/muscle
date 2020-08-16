package tree

import "fmt"

type Qid struct {
	Path    uint64
	Version uint32
}

type Dir struct {
	Qid

	Dev    uint32
	Gid    string
	Length uint64
	Mode   uint32
	Mtime  uint32
	Name   string
	Size   uint16
	Type   uint16
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
