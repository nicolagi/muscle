package tree

import (
	"fmt"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/storage"
)

type codecV15 struct{}

func (codecV15) encodeNode(node *Node) ([]byte, error) {
	size := 49
	size += len(node.info.Name)
	size += len(node.children)
	size += len(node.blocks)
	for _, ptr := range node.children {
		size += int(ptr.pointer.Len())
	}
	for _, b := range node.blocks {
		size += int(b.Ref().Len())
	}
	buf := make([]byte, size)
	ptr := buf
	ptr = pint8(15, ptr)
	// The QID type (file or directory) is derived from the mode (DMDIR flag).
	ptr = pint8(0, ptr)
	ptr = pint64(node.info.ID, ptr)
	ptr = pint32(node.info.Version, ptr)
	ptr = pstr(node.info.Name, ptr)
	ptr = pint8(uint8(node.flags & ^(loaded|dirty)), ptr)
	ptr = pint32(node.bsize, ptr)
	ptr = pint32(node.info.Mode, ptr)
	ptr = pint64(node.info.Size, ptr)
	ptr = pint32(node.info.Modified, ptr)
	ptr = pint32(0, ptr)
	ptr = pint32(uint32(len(node.children)), ptr)
	for _, c := range node.children {
		ptr = pint8(c.pointer.Len(), ptr)
		ptr = pbytes(c.pointer.Bytes(), ptr)
	}
	ptr = pint32(uint32(len(node.blocks)), ptr)
	for _, b := range node.blocks {
		ptr = pint8(uint8(b.Ref().Len()), ptr)
		ptr = pbytes(b.Ref().Bytes(), ptr)
	}
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return buf, nil
}

func (codecV15) decodeNode(data []byte, dest *Node) error {
	ptr := data

	var u8 uint8
	var u32 uint32

	// The QID type (file or directory) is derived from the mode (DMDIR flag).
	_, ptr = gint8(ptr)
	dest.info.ID, ptr = gint64(ptr)
	dest.info.Version, ptr = gint32(ptr)
	dest.info.Name, ptr = gstr(ptr)
	u8, ptr = gint8(ptr)
	dest.flags = nodeFlags(u8)
	dest.bsize, ptr = gint32(ptr)
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
		if err := dest.addChildPointer(storage.NewPointer(ptr[:u8])); err != nil {
			return err
		}
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

func (codecV15) encodeRevision(rev *Revision) ([]byte, error) {
	size := 16 + len(rev.host)
	if !rev.rootKey.IsNull() {
		size += int(rev.rootKey.Len())
	}
	if !rev.parent.IsNull() {
		size += int(rev.parent.Len())
	}
	buf := make([]byte, size)
	ptr := buf
	ptr = pint8(15, ptr)
	if rev.rootKey.IsNull() {
		ptr = pint8(0, ptr)
	} else {
		ptr = pint8(rev.rootKey.Len(), ptr)
		ptr = pbytes(rev.rootKey.Bytes(), ptr)
	}
	ptr = pint8(1, ptr) /* only one parent */
	if rev.parent.IsNull() {
		ptr = pint8(0, ptr)
	} else {
		ptr = pint8(rev.parent.Len(), ptr)
		ptr = pbytes(rev.parent.Bytes(), ptr)
	}
	ptr = pint64(uint64(rev.when), ptr)
	ptr = pstr(rev.host, ptr)
	// Empty instance field, we don't want it anymore.
	ptr = pstr("", ptr)
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return buf, nil
}

func (codecV15) decodeRevision(data []byte, rev *Revision) error {
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
	// Keep only right-most parent, if there are more than one.
	// (In today's code, each snapshot only has one parent snapshot.)
	for i := uint8(0); i < nparents; i++ {
		u8, ptr = gint8(ptr)
		if u8 == 0 {
			rev.parent = storage.Null
		} else {
			rev.parent = storage.NewPointer(ptr[:u8])
			ptr = ptr[u8:]
		}
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
