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
	DMDIR    = 0x80000000
	DMAPPEND = 0x40000000
	DMEXCL   = 0x20000000
)
