package tree

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	foobar "github.com/andreyvit/diff"
)

type stateFn func(row int) (nextRow int, nextState stateFn)

func trimContext(lines []string, context int) string {
	buf := bytes.NewBuffer(nil)

	var lastDiffRow int
	var ignored int
	var ignoreState stateFn
	var outputState stateFn

	ignoreState = func(row int) (int, stateFn) {
		if isDiff(lines[row][0]) {
			lastDiffRow = row
			ignored -= context
			if ignored < 0 {
				ignored = 0
			}
			return max(0, row-context), outputState
		}
		ignored++
		return row + 1, ignoreState
	}

	outputState = func(row int) (int, stateFn) {
		if ignored > 0 {
			fmt.Fprintf(buf, "### Skipped %d common lines ###\n", ignored)
			ignored = 0
		}
		buf.WriteString(lines[row])
		buf.WriteRune('\n')
		if isDiff(lines[row][0]) {
			lastDiffRow = row
		} else if row-lastDiffRow >= context {
			return row + 1, ignoreState
		}
		return row + 1, outputState
	}

	state := ignoreState
	row := 0
	for row < len(lines) {
		row, state = state(row)
		if state == nil {
			break
		}
	}

	return buf.String()
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func isDiff(r uint8) bool {
	return r == '+' || r == '-'
}

type diffTreesOptions struct {
	contextLines int
	//namesOnly    bool
	output      io.Writer
	initialPath string
}

// DiffTreesOption follows the functional options pattern to pass options to DiffTrees.
type DiffTreesOption func(*diffTreesOptions)

func DiffTreesOutput(w io.Writer) DiffTreesOption {
	return func(opts *diffTreesOptions) {
		opts.output = w
	}
}
func DiffTreesInitialPath(pathname string) DiffTreesOption {
	return func(opts *diffTreesOptions) {
		opts.initialPath = pathname
	}
}

// DiffTrees produces a metadata diff of the two trees.
func DiffTrees(a, b *Tree, options ...DiffTreesOption) error {
	opts := diffTreesOptions{
		contextLines: 3,
		output:       ioutil.Discard,
	}
	for _, opt := range options {
		opt(&opts)
	}
	fmt.Fprintf(opts.output, "------ a revision %s root %s\n", a.revision.Hex(), a.root.pointer.Hex())
	fmt.Fprintf(opts.output, "++++++ b revision %s root %s\n", b.revision.Hex(), b.root.pointer.Hex())
	aInitial := a.root
	bInitial := b.root
	if opts.initialPath != "" {
		elements := strings.Split(opts.initialPath, "/")
		visitedNodes, err := a.Walk(a.root, elements...)
		if err != nil {
			return fmt.Errorf("could not walk left tree along %s: %v", opts.initialPath, err)
		}
		aInitial = visitedNodes[len(visitedNodes)-1]
		visitedNodes, err = b.Walk(b.root, elements...)
		if err != nil {
			return fmt.Errorf("could not walk right tree along %s: %v", opts.initialPath, err)
		}
		bInitial = visitedNodes[len(visitedNodes)-1]
	}
	return diffTrees(a, b, aInitial, bInitial, &opts)
}

func diffTrees(atree, btree *Tree, a, b *Node, opts *diffTreesOptions) error {
	if a == nil {
		fmt.Fprintf(opts.output, "--- a/dev/null\n+++ b/%s\n", b.Path())
		return nil
	}
	if b == nil {
		fmt.Fprintf(opts.output, "--- a/%s\n+++ b/dev/null\n", a.Path())
		return nil
	}
	if a.pointer.Equals(b.pointer) {
		return nil
	}

	lineDiffLines := foobar.LineDiffAsLines(a.DiffRepr(), b.DiffRepr())
	lineDiff := trimContext(lineDiffLines, opts.contextLines)
	fmt.Fprintf(opts.output, "--- a/%s\n+++ b/%s\n", a.Path(), b.Path())
	fmt.Fprint(opts.output, lineDiff)

	if !a.hasEqualBlocks(b) {
		if a.D.Length < maxBlobSizeForDiff && b.D.Length < maxBlobSizeForDiff {
			aText := make([]byte, int(a.D.Length))
			bText := make([]byte, int(b.D.Length))
			atree.ReadAt(a, aText, 0)
			btree.ReadAt(b, bText, 0)
			lineDiffLines := foobar.LineDiffAsLines(string(aText), string(bText))
			lineDiff := trimContext(lineDiffLines, opts.contextLines)
			lineDiff, printable := extractPrintable(lineDiff)
			if printable {
				fmt.Fprintf(opts.output, "%s\n", lineDiff)
			} else {
				fmt.Fprintf(opts.output, "*** BINARY files differ***\n")
			}
		} else {
			fmt.Fprintf(opts.output, "*** diff of contents OMITTED (too large, aSize=%d bSize=%d maxSize=%d) ***\n", a.D.Length, b.D.Length, maxBlobSizeForDiff)
		}
	}

	// We can recurse only if they are both directories.
	if !a.IsDir() || !b.IsDir() {
		return nil
	}

	if err := atree.Grow(a); err != nil {
		return fmt.Errorf("could not grow %q: %v", a.Path(), err)
	}
	if err := btree.Grow(b); err != nil {
		return fmt.Errorf("could not grow %q: %v", b.Path(), err)
	}

	achildren := a.childrenMap()
	bchildren := b.childrenMap()
	for _, name := range orderedUnionOfChildrenNames(achildren, bchildren) {
		if err := diffTrees(atree, btree, achildren[name], bchildren[name], opts); err != nil {
			return err
		}
	}
	return nil
}

func orderedUnionOfChildrenNames(a, b map[string]*Node) []string {
	m := make(map[string]struct{})
	for n := range a {
		m[n] = struct{}{}
	}
	for n := range b {
		m[n] = struct{}{}
	}
	var names []string
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func extractPrintable(input string) (output string, wasPrintable bool) {
	outputBuffer := bytes.NewBuffer(nil)
	wasPrintable = true
	off := 0

	isPrintable := func(r rune) bool {
		if r == utf8.RuneError {
			return false
		}
		if strings.ContainsRune("\t\r\n", r) {
			return true
		}
		return unicode.IsPrint(r)
	}

	isPrintablePrefix := func(s string, prefixLen int) (flag bool, consumedBytes int, consumedRunes int) {
		for ; prefixLen > 0; prefixLen-- {
			r, size := utf8.DecodeRuneInString(s[consumedBytes:])
			consumedBytes += size
			consumedRunes++
			if !isPrintable(r) {
				return
			}
		}
		flag = true
		return
	}

	// Note: When this is called, we know we're looking at at least 5 printable runes.
	consumePrintable := func() {
		for off < len(input) {
			r, size := utf8.DecodeRuneInString(input[off:])
			if !isPrintable(r) {
				break
			}
			outputBuffer.WriteRune(r)
			off += size
		}
	}

	// Will stop when looking at least 5 printable runes.
	consumeNonPrintable := func() {
		counter := 0
		for off < len(input) {
			if flag, consumedBytes, consumedRunes := isPrintablePrefix(input[off:], 5); flag {
				// Reached end of string or a non-printable rune.
				if counter > 0 {
					wasPrintable = false
					fmt.Fprintf(outputBuffer, "…%d…", counter)
				}
				break
			} else {
				counter += consumedRunes
				off += consumedBytes
			}
		}
	}

	for off < len(input) {
		consumeNonPrintable()
		consumePrintable()
	}

	return outputBuffer.String(), wasPrintable
}
