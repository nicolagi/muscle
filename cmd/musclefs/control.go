package main

type ctl struct {
	contents []byte
}

func (f *ctl) read(target []byte, offset int) int {
	if offset > len(f.contents) {
		return 0
	}
	return copy(target, f.contents[offset:])
}
