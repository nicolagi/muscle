package main

import (
	"bytes"
)

// ctl can be used to implement a 9P control file quite easily.
// Use read() for Tread, append() for Twrite, and run the commands
// returned by the append() call, replying with Rwrite or Rerror
// as appropriate.
type ctl struct {
	contents []byte
	offset   int
}

func (f *ctl) read(target []byte, offset int) int {
	if offset > len(f.contents) {
		return 0
	}
	return copy(target, f.contents[offset:])
}

func (f *ctl) append(source []byte) []string {
	if len(source) == 0 {
		return nil
	}
	f.contents = append(f.contents, source...)
	return f.lines()
}

func (f *ctl) lines() []string {
	var ll []string

	for {
		i := bytes.Index(f.contents[f.offset:], []byte{'\n'})
		if i == -1 {
			break
		}
		line := f.contents[f.offset : f.offset+i]
		f.offset += i + 1
		ll = append(ll, string(line))
	}

	return ll
}
