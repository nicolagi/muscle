package diff

// ringBuffer holds a few lines of context in-between hunks. It will happily
// overwrite values, it won't complain about exceeding the size.
type ringBuffer struct {
	lines []string
	ridx  int
	widx  int
	len   int
	sz    int
}

func newRingBuffer(sz int) *ringBuffer {
	return &ringBuffer{
		lines: make([]string, sz),
		sz:    sz,
	}
}

func (rb *ringBuffer) incr(val int) int {
	return (val + 1) % rb.sz
}

func (rb *ringBuffer) enqueue(line string) {
	if rb.len == rb.sz {
		rb.ridx = rb.incr(rb.ridx)
	} else {
		rb.len++
	}
	rb.lines[rb.widx] = line
	rb.widx = rb.incr(rb.widx)
}

func (rb *ringBuffer) dequeueAll() []string {
	var lines []string
	for rb.len > 0 {
		lines = append(lines, rb.lines[rb.ridx])
		rb.ridx = rb.incr(rb.ridx)
		rb.len--
	}
	return lines
}
