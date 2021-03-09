package p9util

import (
	"fmt"
	"io"
	"testing"
	"testing/quick"

	"github.com/lionkov/go9p/p"
)

type dirBufferReader struct {
	dirb *DirBuffer
	off  int
}

func (r *dirBufferReader) Read(p []byte) (n int, err error) {
	n, err = r.dirb.Read(p, r.off)
	if n > 0 {
		r.off += n
	} else if err == nil {
		err = io.EOF
	}
	return
}

const IOHDRSZ = 24

// readall is similar to io.ReadAll, adjusted to work for reading a
// whole 9P directory. Note: msize is uint32 in 9P but I won't test
// very large values of msize, so I'm using uint16.
func readall(r io.Reader, msize uint16) ([]byte, error) {
	iounit := int(msize - IOHDRSZ)
	b := make([]byte, 0, iounit)
	for {
		if cap(b)-len(b) < iounit {
			newb := make([]byte, len(b), 2*cap(b))
			copy(newb, b)
			b = newb
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}

func TestDirBuffer(t *testing.T) {
	t.Run("0 bytes when dir entry is larger than read buffer", func(t *testing.T) {
		dirb := &DirBuffer{}
		dir := p.Dir{}
		for i := 0; i < 1000; i++ {
			dirb.Write(&dir)
		}
		b, err := io.ReadAll(&dirBufferReader{dirb: dirb})
		if err != nil {
			t.Errorf("got %v, want nil error", err)
		}
		if got, want := len(b), len(dirb.dirents); got >= want {
			t.Errorf("got %v, want less than %d bytes", got, want)
		}
	})
	t.Run("can read all regardless of iounit or msize", func(t *testing.T) {
		f := func(names []string, iounit uint16) bool {
			msize := iounit + IOHDRSZ
			dirb := &DirBuffer{}
			dir := p.Dir{}
			prevsize := 0
			for _, n := range names {
				dir.Name = fmt.Sprintf("%x", n)
				dirb.Write(&dir)
				newsize := len(dirb.dirents)
				if entrysize := newsize - prevsize; entrysize > int(iounit) {
					// Whoops, too large.
					dirb.dirents = dirb.dirents[:prevsize]
					dirb.direntends = dirb.direntends[:len(dirb.direntends)-1]
				} else {
					prevsize = newsize
				}
			}
			b, err := readall(&dirBufferReader{dirb: dirb}, msize)
			if err != nil {
				t.Errorf("got %q, want nil", err)
				return false
			}
			if got, want := len(b), len(dirb.dirents); got != want {
				t.Errorf("got %d, want %d bytes", got, want)
				return false
			}
			return true
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}
