package tree

import (
	"fmt"
)

type Qid struct {
	Path    uint64
	Version uint32
}

type Dir struct {
	Qid

	Name     string
	Size     uint64
	Mode     uint32
	Modified uint32
}

const (
	DMDIR = 0x80000000
)

var (
	ErrExists     = fmt.Errorf("file already exists")
	ErrNotEmpty   = fmt.Errorf("directory not empty")
	ErrPermission = fmt.Errorf("permission denied")
)
