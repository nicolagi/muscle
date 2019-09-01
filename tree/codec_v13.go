package tree

import (
	"fmt"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/muscle/storage"
)

type codecV13 struct{}

func (codecV13) encodeNode(node *Node) ([]byte, error) {
	size := 31
	size += len(node.D.Name)
	size += len(node.children)
	size += len(node.blocks)
	for _, ptr := range node.children {
		size += int(ptr.pointer.Len())
	}
	for _, ptr := range node.blocks {
		size += int(ptr.pointer.Len())
	}
	buf := make([]byte, size)
	ptr := buf
	ptr = pint8(13, ptr)
	ptr = pstr(node.D.Name, ptr)
	ptr = pint32(node.D.Mode, ptr)
	ptr = pint64(node.D.Length, ptr)
	ptr = pint32(node.D.Mtime, ptr)
	ptr = pint32(0, ptr)
	ptr = pint32(uint32(len(node.children)), ptr)
	for _, c := range node.children {
		ptr = pint8(c.pointer.Len(), ptr)
		ptr = pbytes(c.pointer.Bytes(), ptr)
	}
	ptr = pint32(uint32(len(node.blocks)), ptr)
	for _, b := range node.blocks {
		ptr = pint8(b.pointer.Len(), ptr)
		ptr = pbytes(b.pointer.Bytes(), ptr)
	}
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return buf, nil
}

func (codecV13) decodeNode(data []byte, dest *Node) error {
	ptr := data

	var u8 uint8
	var u32 uint32

	dest.D.Name, ptr = gstr(ptr)
	dest.D.Mode, ptr = gint32(ptr)
	if dest.D.Mode&p.DMDIR != 0 {
		dest.D.Qid.Type = p.QTDIR
		// Read and ignore the length. I used to calculate and store directory lengths
		// (length of the serialized dir entries) but I later learned that the size is conventionally 0.
		_, ptr = gint64(ptr)
	} else {
		dest.D.Length, ptr = gint64(ptr)
	}
	dest.D.Mtime, ptr = gint32(ptr)
	dest.D.Atime = dest.D.Mtime

	u32, ptr = gint32(ptr)
	if u32 > 0 {
		//dest.pack = storage.NewPack(p[:u32])
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
		dest.blocks = append(dest.blocks, &Block{
			pointer: storage.NewPointer(ptr[:u8]),
		})
		ptr = ptr[u8:]
	}

	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}

	return nil
}

func (codecV13) encodeRevision(rev *Revision) ([]byte, error) {
	size := 46 + len(rev.hostname) + len(rev.comment) + 32*len(rev.parents)
	buf := make([]byte, size)
	ptr := buf
	ptr = pint8(13, ptr)
	if !rev.rootKey.IsNull() {
		ptr = pbytes(rev.rootKey.Bytes(), ptr)
	} else {
		ptr = ptr[32:]
	}
	ptr = pint8(uint8(len(rev.parents)), ptr)
	for _, k := range rev.parents {
		if !k.IsNull() {
			ptr = pbytes(k.Bytes(), ptr)
		} else {
			ptr = ptr[32:]
		}
	}
	ptr = pint64(uint64(rev.when), ptr)
	ptr = pstr(rev.hostname, ptr)
	ptr = pstr(rev.comment, ptr)
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return buf, nil
}

func (codecV13) decodeRevision(data []byte, rev *Revision) error {
	var u8 uint8
	var u64 uint64
	ptr := data
	rev.rootKey = storage.NewPointer(ptr[:32])
	ptr = ptr[32:]
	u8, ptr = gint8(ptr)
	for i := uint8(0); i < u8; i++ {
		rev.parents = append(rev.parents, storage.NewPointer(ptr[:32]))
		ptr = ptr[32:]
	}
	u64, ptr = gint64(ptr)
	rev.when = int64(u64)
	rev.hostname, ptr = gstr(ptr)
	rev.comment, ptr = gstr(ptr)
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return nil
}
