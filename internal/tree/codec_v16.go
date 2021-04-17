package tree

import (
	"fmt"

	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/storage"
)

type codec16 struct{}

var _ Codec = codec16{}

func (codec16) encodeNode(node *Node) ([]byte, error) {
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
	ptr = pint8(16, ptr)
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

func (codec16) decodeNode(data []byte, dest *Node) error {
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

func (codec16) encodeRevision(rev *Revision) ([]byte, error) {
	size := 14 + len(rev.host)
	if !rev.rootKey.IsNull() {
		size += int(rev.rootKey.Len())
	}
	for _, p := range rev.parents {
		size += int(p.Pointer.Len()) + 1
		size += int(len(p.Name)) + 2
	}
	buf := make([]byte, size)
	ptr := buf
	ptr = pint8(16, ptr)
	if rev.rootKey.IsNull() {
		ptr = pint8(0, ptr)
	} else {
		ptr = pint8(rev.rootKey.Len(), ptr)
		ptr = pbytes(rev.rootKey.Bytes(), ptr)
	}
	ptr = pint16(uint16(len(rev.parents)), ptr)
	for _, tag := range rev.parents {
		ptr = pstr(tag.Name, ptr)
		ptr = pint8(tag.Pointer.Len(), ptr)
		ptr = pbytes(tag.Pointer.Bytes(), ptr)
	}
	ptr = pint64(uint64(rev.when), ptr)
	ptr = pstr(rev.host, ptr)
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return buf, nil
}

func (codec16) decodeRevision(data []byte, rev *Revision) error {
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
	nparents, ptr := gint16(ptr)
	for i := uint16(0); i < nparents; i++ {
		var tag Tag
		tag.Name, ptr = gstr(ptr)
		u8, ptr = gint8(ptr)
		tag.Pointer = storage.NewPointer(ptr[:u8])
		ptr = ptr[u8:]
		rev.parents = append(rev.parents, tag)
	}
	u64, ptr = gint64(ptr)
	rev.when = int64(u64)
	rev.host, ptr = gstr(ptr)
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return nil
}
