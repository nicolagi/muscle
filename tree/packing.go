package tree

// This file contains functions to convert basic types (integers
// of various sizes, strings) to/from byte slices. They have been
// copied from go9p. (They are not exported there, so I copied
// them.) These are used to serialize and unserialize tree nodes
// and tree revisions, in codec files such as codec_v13.go.

func gint8(buf []byte) (uint8, []byte) { return buf[0], buf[1:] }

func gint16(buf []byte) (uint16, []byte) {
	return uint16(buf[0]) | (uint16(buf[1]) << 8), buf[2:]
}

func gint32(buf []byte) (uint32, []byte) {
	return uint32(buf[0]) | (uint32(buf[1]) << 8) | (uint32(buf[2]) << 16) |
			(uint32(buf[3]) << 24),
		buf[4:]
}

func gint64(buf []byte) (uint64, []byte) {
	return uint64(buf[0]) | (uint64(buf[1]) << 8) | (uint64(buf[2]) << 16) |
			(uint64(buf[3]) << 24) | (uint64(buf[4]) << 32) | (uint64(buf[5]) << 40) |
			(uint64(buf[6]) << 48) | (uint64(buf[7]) << 56),
		buf[8:]
}

func gstr(buf []byte) (string, []byte) {
	var n uint16

	if buf == nil {
		return "", nil
	}

	n, buf = gint16(buf)
	if int(n) > len(buf) {
		return "", nil
	}

	return string(buf[0:n]), buf[n:]
}

func pint8(val uint8, buf []byte) []byte {
	buf[0] = val
	return buf[1:]
}

func pint16(val uint16, buf []byte) []byte {
	buf[0] = uint8(val)
	buf[1] = uint8(val >> 8)
	return buf[2:]
}

func pint32(val uint32, buf []byte) []byte {
	buf[0] = uint8(val)
	buf[1] = uint8(val >> 8)
	buf[2] = uint8(val >> 16)
	buf[3] = uint8(val >> 24)
	return buf[4:]
}

func pint64(val uint64, buf []byte) []byte {
	buf[0] = uint8(val)
	buf[1] = uint8(val >> 8)
	buf[2] = uint8(val >> 16)
	buf[3] = uint8(val >> 24)
	buf[4] = uint8(val >> 32)
	buf[5] = uint8(val >> 40)
	buf[6] = uint8(val >> 48)
	buf[7] = uint8(val >> 56)
	return buf[8:]
}

func pstr(val string, buf []byte) []byte {
	n := uint16(len(val))
	buf = pint16(n, buf)
	b := []byte(val)
	copy(buf, b)
	return buf[n:]
}

func pbytes(val []byte, buf []byte) []byte {
	n := len(val)
	copy(buf, val)
	return buf[n:]
}
