package main

import "github.com/lionkov/go9p/p"

type ctl struct {
	D        p.Dir
	contents []byte
}

func (f *ctl) read(target []byte, offset int) int {
	if offset > len(f.contents) {
		return 0
	}
	return copy(target, f.contents[offset:])
}
