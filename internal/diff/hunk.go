package diff

import (
	"fmt"
	"io"
)

// See https://www.gnu.org/software/diffutils/manual/html_node/Hunks.html.
type hunk struct {
	// These four describe the location of the hunk (left/right offset/count).
	// This is rendered for example as  "@@ -15,3 +17,5 @@".
	lo int
	lc int
	ro int
	rc int

	lines []string

	// Counts the number of lines since the last difference. Used to decide when
	// to close a hunk. For a unified diff with 3 lines of context, for example,
	// the hunk is definitely closed after 7 common lines (4 need to be removed
	// from the hunk). In other words, the maximum distance between lines marked
	// as changed, in the same hunk, is 6.
	sinceLastDiff int

	clen int

	printErr error
}

func newHunk(lo, ro int, backfill []string, contextLines int) *hunk {
	l := len(backfill)
	return &hunk{
		lo:    lo - l,
		ro:    ro - l,
		lc:    l,
		rc:    l,
		lines: backfill,
		clen:  contextLines,
	}
}

func (h *hunk) appendLeft(line string) {
	h.lines = append(h.lines, line)
	h.sinceLastDiff = 0
	h.lc++
}

func (h *hunk) appendRight(line string) {
	h.lines = append(h.lines, line)
	h.sinceLastDiff = 0
	h.rc++
}

func (h *hunk) appendCommon(line string) {
	h.lines = append(h.lines, line)
	h.sinceLastDiff++
	h.lc++
	h.rc++
}

func (h *hunk) isComplete() bool {
	return h.sinceLastDiff >= 2*h.clen+1
}

func (h *hunk) trim() []string {
	if h.sinceLastDiff <= h.clen {
		return nil
	}
	delc := h.sinceLastDiff - h.clen
	del := h.lines[len(h.lines)-delc:]
	h.lines = h.lines[:len(h.lines)-delc]
	h.lc -= delc
	h.rc -= delc
	return del
}

func (h hunk) printLocationTo(w io.Writer) {
	h.print(w, "@@ -%d", h.lo+1)
	if h.lc > 1 {
		h.print(w, ",%d +%d", h.lc, h.ro+1)
	} else {
		h.print(w, " +%d", h.ro+1)
	}
	if h.rc > 1 {
		h.print(w, ",%d @@\n", h.rc)
	} else {
		h.print(w, " @@\n")
	}
}

func (h hunk) printTo(b io.Writer) error {
	h.printLocationTo(b)
	for _, line := range h.lines {
		h.print(b, "%s\n", line)
	}
	return h.printErr
}

func (h *hunk) print(w io.Writer, format string, a ...interface{}) {
	if h.printErr != nil {
		return
	}
	_, h.printErr = fmt.Fprintf(w, format, a...)
}
