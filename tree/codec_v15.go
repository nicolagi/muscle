package tree

import (
	"fmt"

	"github.com/lionkov/go9p/p"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
)

type codecV15 struct{}

func (codecV15) encodeNode(node *Node) ([]byte, error) {
	size := 49
	size += len(node.D.Name)
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
	ptr = pint8(node.D.Qid.Type, ptr)
	ptr = pint64(node.D.Qid.Path, ptr)
	ptr = pint32(node.D.Qid.Version, ptr)
	ptr = pstr(node.D.Name, ptr)
	ptr = pint8(uint8(node.flags & ^(loaded|dirty)), ptr)
	ptr = pint32(node.bsize, ptr)
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

	dest.D.Qid.Type, ptr = gint8(ptr)
	dest.D.Qid.Path, ptr = gint64(ptr)
	dest.D.Qid.Version, ptr = gint32(ptr)
	dest.D.Name, ptr = gstr(ptr)
	u8, ptr = gint8(ptr)
	dest.flags = nodeFlags(u8)
	dest.bsize, ptr = gint32(ptr)
	dest.D.Mode, ptr = gint32(ptr)
	if dest.D.Mode&p.DMDIR != 0 {
		dest.D.Qid.Type = p.QTDIR
		// Ignore the length, it's 0 for directories, see stat(9p) or stat(5).
		_, ptr = gint64(ptr)
	} else {
		dest.D.Length, ptr = gint64(ptr)
	}
	dest.D.Mtime, ptr = gint32(ptr)
	dest.D.Atime = dest.D.Mtime

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

func (codecV15) encodeRevision(rev *Revision) ([]byte, error) {
	size := 15 + len(rev.parents) + len(rev.hostname) + len(rev.instance)
	if !rev.rootKey.IsNull() {
		size += int(rev.rootKey.Len())
	}
	for _, k := range rev.parents {
		if !k.IsNull() {
			size += int(k.Len())
		}
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
	ptr = pint8(uint8(len(rev.parents)), ptr)
	for _, k := range rev.parents {
		if k.IsNull() {
			ptr = pint8(0, ptr)
		} else {
			ptr = pint8(k.Len(), ptr)
			ptr = pbytes(k.Bytes(), ptr)
		}
	}
	ptr = pint64(uint64(rev.when), ptr)
	ptr = pstr(rev.hostname, ptr)
	ptr = pstr(rev.instance, ptr)
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
	for i := uint8(0); i < nparents; i++ {
		u8, ptr = gint8(ptr)
		if u8 == 0 {
			rev.parents = append(rev.parents, storage.Null)
		} else {
			rev.parents = append(rev.parents, storage.NewPointer(ptr[:u8]))
			ptr = ptr[u8:]
		}
	}
	u64, ptr = gint64(ptr)
	rev.when = int64(u64)
	rev.hostname, ptr = gstr(ptr)
	rev.instance, ptr = gstr(ptr)
	if len(ptr) != 0 {
		panic(fmt.Sprintf("buffer length is non-zero: %d", len(ptr)))
	}
	return nil
}
