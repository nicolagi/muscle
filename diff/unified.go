package diff

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/andreyvit/diff"
)

const bytesForBinaryFileCheck = 1 << 16

// Unified wraps UnifiedTo to return a string instead of writing it to a writer.
func Unified(a, b Node, contextLines int) (string, error) {
	var buf bytes.Buffer
	err := UnifiedTo(&buf, a, b, contextLines)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

// UnifiedTo writes a unified diff of the two nodes to the passed writer. Its
// output should be the same as that of the system diff (in my system!) or quite
// similar (in any Unix system). (The reason why muscle uses an internal
// implementation of diff rather than relying on the system is that diffing huge
// trees, as it happens when looking at inter-revision diffs, muscle can take
// advantage of the Merkle tree structure to avoid diffing entire subtrees.)
func UnifiedTo(w io.Writer, a, b Node, contextLines int) error {
	if a.SameAs(b) {
		return nil
	}
	aContent, aErr := a.Content()
	if aErr != nil {
		return aErr
	}
	bContent, bErr := b.Content()
	if bErr != nil {
		return bErr
	}
	lines := diff.LineDiffAsLines(aContent, bContent)
	if len(lines) == 0 {
		return nil
	}
	return unified(w, lines, contextLines)
}

func unified(w io.Writer, lines []string, contextLines int) error {
	// While processing lines, we're either in a hunk or in common segment. The
	// hunk is nil if we are in a common segment.
	var hunk *hunk

	// When we're not in the middle of a hunk, we keep the most recent common
	// lines in a ring buffer. When starting a new hunk, the common lines will
	// be backfilled into the hunk and the ring buffer will be emptied out.
	common := newRingBuffer(contextLines)

	if isLikelyBinaryFile(lines) {
		_, err := fmt.Fprintln(w, "Binary files differ")
		return err
	}

	var leftOffset, rightOffset int
	for _, line := range lines {
		if line[0] == ' ' {
			// A common line. If in the middle of a hunk, we might get to the
			// point where a hunk cannot be extended so we can print it and add
			// the following common lines to the ring buffer rather than the
			// hunk.
			if hunk != nil {
				hunk.appendCommon(line)
				if hunk.isComplete() {
					for _, line := range hunk.trim() {
						common.enqueue(line)
					}
					if err := hunk.printTo(w); err != nil {
						return err
					}
					hunk = nil
				}
			} else {
				common.enqueue(line)
			}
		} else {
			// A diff line. Add to the current hunk, starting a new one first if
			// necessary.
			if hunk == nil {
				hunk = newHunk(leftOffset, rightOffset, common.dequeueAll(), contextLines)
			}
			if line[0] == '-' {
				hunk.appendLeft(line)
			} else {
				hunk.appendRight(line)
			}
		}
		switch line[0] {
		case '-':
			leftOffset++
		case ' ':
			leftOffset++
			rightOffset++
		case '+':
			rightOffset++
		}
	}
	if hunk != nil {
		hunk.trim()
		return hunk.printTo(w)
	}
	return nil
}

// Look at a few thousand bytes and see if any of them is null.
func isLikelyBinaryFile(lines []string) bool {
	count := 0
	for _, line := range lines {
		if strings.Contains(line, "\x00") {
			return true
		}
		count += len(line)
		if count >= bytesForBinaryFileCheck {
			break
		}
	}
	return false
}
