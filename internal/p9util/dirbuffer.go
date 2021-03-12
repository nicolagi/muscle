package p9util

import (
	"fmt"
	"sort"

	"github.com/lionkov/go9p/p"
	"github.com/nicolagi/muscle/internal/linuxerr"
)

type DirBuffer struct {
	dirents    []byte
	direntends []int
}

func (dirb *DirBuffer) Reset() {
	dirb.dirents = nil
	dirb.direntends = nil
}

func (dirb *DirBuffer) Write(dir *p.Dir) {
	dirb.dirents = append(dirb.dirents, p.PackDir(dir, false)...)
	dirb.direntends = append(dirb.direntends, len(dirb.dirents))
}

// From read(5): «For directories, read returns an integral number of
// directory entries exactly as in stat (see stat(5)), one for each
// member of the directory. The read request message must have offset
// equal to zero or the value of offset in the previous read on the
// directory, plus the number of bytes returned in the previous read. In
// other words, seeking other than to the beginning is illegal in a
// directory (see seek(2)).»
func (dirb *DirBuffer) Read(b []byte, offset int) (n int, err error) {
	count := len(b)
	// The offset must be the end of one of the dir entries.
	if offset > 0 {
		i := sort.SearchInts(dirb.direntends, offset)
		if i == len(dirb.direntends) || dirb.direntends[i] != offset {
			return 0, fmt.Errorf("%d is not a dir entry offset: %w", offset, linuxerr.EINVAL)
		}
	}
	// We can't return truncated entries, so we may have to decrease count.
	j := sort.SearchInts(dirb.direntends, offset+count)
	if j == len(dirb.direntends) || dirb.direntends[j] != offset+count {
		if j == 0 {
			count = 0
		} else {
			count = dirb.direntends[j-1] - offset
		}
	}
	if count < 0 {
		return 0, fmt.Errorf("dirents %d bytes too small for dir entry: %w", -count, linuxerr.EINVAL)
	}
	return copy(b, dirb.dirents[offset:offset+count]), nil
}
