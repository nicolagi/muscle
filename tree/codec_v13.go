package tree

import (
	"fmt"
	"time"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
)

const v13BlockCapacity = 1024 * 1024

type codecV13 struct {
}

func (codecV13) encodeNode(node *Node) ([]byte, error) {
	panic("decommissioned")
}

func (codec codecV13) decodeNode(data []byte, dest *Node) error {
	ptr := data

	var u8 uint8
	var u32 uint32

	// This data was not saved with v13.
	dest.D.Qid.Path = uint64(time.Now().UnixNano())
	dest.D.Qid.Version = 1

	dest.D.Name, ptr = gstr(ptr)
	dest.D.Mode, ptr = gint32(ptr)
	if dest.D.Mode&DMDIR != 0 {
		// Read and ignore the length. I used to calculate and store directory lengths
		// (length of the serialized dir entries) but I later learned that the size is conventionally 0.
		_, ptr = gint64(ptr)
	} else {
		dest.D.Size, ptr = gint64(ptr)
	}
	dest.D.Modified, ptr = gint32(ptr)

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
		b, err := dest.blockFactory.New(r, v13BlockCapacity)
		if err != nil {
			return err
		}
		dest.blocks = append(dest.blocks, b)
		ptr = ptr[u8:]
	}

	// Properties added in V14.
	dest.flags = sealed
	dest.bsize = v13BlockCapacity

	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}

	return nil
}

func (codecV13) encodeRevision(rev *Revision) ([]byte, error) {
	panic("decommissioned")
}

func (codecV13) decodeRevision(data []byte, rev *Revision) error {
	var u8 uint8
	var u64 uint64
	ptr := data
	rev.rootKey = storage.NewPointer(ptr[:32])
	ptr = ptr[32:]
	u8, ptr = gint8(ptr)
	for i := uint8(0); i < u8; i++ {
		// Not interested in parent revisions for this codec (too stale).
		ptr = ptr[32:]
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
