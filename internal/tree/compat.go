package tree

type NodeInfo struct {
	ID       uint64
	Version  uint32
	Name     string
	Size     uint64
	Mode     uint32
	Modified uint32
}

const (
	DMDIR = 0x80000000
)
