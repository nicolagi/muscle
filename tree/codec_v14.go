package tree

import (
	"fmt"
	"time"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
)

// This was the default before node size was configurable and included in the serialized node. There are nodes that have
// a serialized block capacity of 0, and should instead use the default capacity at that time.
const v14DefaultBlockCapacity = 1024 * 1024

type codecV14 struct{}

func (codecV14) encodeNode(node *Node) ([]byte, error) {
	panic("decommissioned")
}

func (codecV14) decodeNode(data []byte, dest *Node) error {
	ptr := data

	var u8 uint8
	var u32 uint32

	// This data was not saved with v14.
	dest.info.ID = uint64(time.Now().UnixNano())
	dest.info.Version = 1

	dest.info.Name, ptr = gstr(ptr)
	u8, ptr = gint8(ptr)
	dest.flags = nodeFlags(u8)
	dest.bsize, ptr = gint32(ptr)
	if dest.bsize == 0 {
		dest.bsize = v14DefaultBlockCapacity
	}
	dest.info.Mode, ptr = gint32(ptr)
	if dest.info.Mode&DMDIR != 0 {
		// Ignore the length, it's 0 for directories, see stat(9p) or stat(5).
		_, ptr = gint64(ptr)
	} else {
		dest.info.Size, ptr = gint64(ptr)
	}
	dest.info.Modified, ptr = gint32(ptr)

	u32, ptr = gint32(ptr)
	if u32 > 0 {
		ptr = ptr[u32:]
	}

	u32, ptr = gint32(ptr)
	for i := uint32(0); i < u32; i++ {
		u8, ptr = gint8(ptr)
		dest.add(&Node{pointer: storage.NewPointer(ptr[:u8])})
		ptr = ptr[u8:]
	}
	u32, ptr = gint32(ptr)
	for i := uint32(0); i < u32; i++ {
		u8, ptr = gint8(ptr)
		// TODO Direct dependency on internal/block, instead of dest.blockFactory.*.
		// May not be extensible enough.
		r, err := block.NewRef(ptr[:u8])
		if err != nil {
			return err
		}
		// Block size isn't configurable yet.
		b, err := dest.blockFactory.New(r, int(dest.bsize))
		if err != nil {
			return err
		}
		dest.blocks = append(dest.blocks, b)
		ptr = ptr[u8:]
	}

	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}

	return nil
}

func (codecV14) encodeRevision(rev *Revision) ([]byte, error) {
	panic("decommissioned")
}

func (codecV14) decodeRevision(data []byte, rev *Revision) error {
	var u8 uint8
	var u64 uint64
	ptr := data
	u8, ptr = gint8(ptr)
	if u8 == 0 {
		rev.rootKey = storage.Null
	} else {
		rev.rootKey = storage.NewPointer(ptr[:u8])
		ptr = ptr[u8:]
	}
	u8, ptr = gint8(ptr)
	nparents := u8
	for i := uint8(0); i < nparents; i++ {
		u8, ptr = gint8(ptr)
		// Not interested in parent revisions for this codec (too stale).
		ptr = ptr[u8:]
	}
	u64, ptr = gint64(ptr)
	rev.when = int64(u64)
	rev.host, ptr = gstr(ptr)
	// Discard instance field, we don't want it anymore.
	_, ptr = gstr(ptr)
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return nil
}
