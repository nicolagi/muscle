package tree

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/nicolagi/muscle/diff"
)

const defaultMaxBlockSizeForDiff = 256 * 1024 // 256 kB

var (
	errTreeNodeLarge = errors.New("tree node too large")

	// Should never happen, but if it does, it's bad. It means a node's length
	// and its blocks are not in sync. Not that the blocks could not be loaded,
	// but that the metadata is indeed inconsistent.
	errTreeNodeTruncated = errors.New("tree node truncated")
)

// nodeMeta is an implementation of diff.Node for file system node metadata.
type nodeMeta struct {
	n *Node
}

func (node nodeMeta) SameAs(otherNode diff.Node) (bool, error) {
	other, ok := otherNode.(nodeMeta)
	if !ok {
		return false, nil
	}
	if node.n == nil && other.n == nil {
		return true, nil
	}
	if node.n == nil || other.n == nil {
		return false, nil
	}
	return node.n.pointer.Equals(other.n.pointer), nil
}

func (node nodeMeta) Content() (string, error) {
	if node.n == nil {
		return "", nil
	}
	var output bytes.Buffer
	_, _ = fmt.Fprintf(
		&output,
		`Key %q
Dir.Size %d
Dir.Type %d
Dir.Dev %d
Dir.Qid.Version %d
Dir.Qid.Path %d
Dir.Mode %d
Dir.Atime %s
Dir.Mtime %s
Dir.Length %d
Dir.Name %q
Dir.Uid %q
Dir.Gid %q
`,
		node.n.pointer.Hex(),
		node.n.D.Size,
		node.n.D.Type,
		node.n.D.Dev,
		node.n.D.Qid.Version,
		node.n.D.Qid.Path,
		node.n.D.Mode,
		time.Unix(int64(node.n.D.Atime), 0).UTC().Format(time.RFC3339),
		time.Unix(int64(node.n.D.Mtime), 0).UTC().Format(time.RFC3339),
		node.n.D.Length,
		node.n.D.Name,
		node.n.D.Uid,
		node.n.D.Gid,
	)
	_, _ = fmt.Fprintf(&output, "blocks:\n")
	for _, b := range node.n.blocks {
		_, _ = fmt.Fprintf(&output, "\t%v\n", b.Ref())
	}
	return output.String(), nil
}

// treeNode is an implementation of diff.Node for file system node contents.
type treeNode struct {
	t       *Tree
	n       *Node
	maxSize int
}

func (node treeNode) SameAs(otherNode diff.Node) (bool, error) {
	other, ok := otherNode.(treeNode)
	if !ok {
		return false, nil
	}
	if node.n == nil && other.n == nil {
		return true, nil
	}
	if node.n == nil || other.n == nil {
		return false, nil
	}
	return node.n.hasEqualBlocks(other.n)
}

func (node treeNode) Content() (string, error) {
	if node.n == nil {
		return "", nil
	}
	if node.n.D.Length > uint64(node.maxSize) {
		return "", fmt.Errorf("%d: %w", node.n.D.Length, errTreeNodeLarge)
	}
	content := make([]byte, node.n.D.Length)
	n, err := node.n.ReadAt(content, 0)
	if err != nil {
		return "", err
	}
	if uint64(n) != node.n.D.Length {
		return "", fmt.Errorf("got %d out of %d bytes: %w", n, node.n.D.Length, errTreeNodeTruncated)
	}
	return string(content), err
}

type diffTreesOptions struct {
	contextLines int
	namesOnly    bool
	verbose      bool
	output       io.Writer
	initialPath  string
	maxSize      int
}

// DiffTreesOption follows the functional options pattern to pass options to DiffTrees.
type DiffTreesOption func(*diffTreesOptions)

func DiffTreesMaxSize(value int) DiffTreesOption {
	return func(opts *diffTreesOptions) {
		opts.maxSize = value
	}
}

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

func DiffTreesContext(value int) DiffTreesOption {
	return func(opts *diffTreesOptions) {
		opts.contextLines = value
	}
}

func DiffTreesNamesOnly(value bool) DiffTreesOption {
	return func(opts *diffTreesOptions) {
		opts.namesOnly = value
	}
}

func DiffTreesVerbose(value bool) DiffTreesOption {
	return func(opts *diffTreesOptions) {
		opts.verbose = value
	}
}

// DiffTrees produces a metadata diff of the two trees.
func DiffTrees(a, b *Tree, options ...DiffTreesOption) error {
	opts := diffTreesOptions{
		contextLines: 3,
		output:       ioutil.Discard,
		maxSize:      defaultMaxBlockSizeForDiff,
	}
	for _, opt := range options {
		opt(&opts)
	}
	if _, err := fmt.Fprintf(opts.output, "------ a root %s\n++++++ b root %s\n", a.root.pointer.Hex(), b.root.pointer.Hex()); err != nil {
		return fmt.Errorf("tree.DiffTrees: %w", err)
	}
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
	var an, bn diff.Node

	an = nodeMeta{n: a}
	bn = nodeMeta{n: b}
	output, err := diff.Unified(an, bn, opts.contextLines)
	if err != nil {
		return err
	}
	if output == "" {
		return nil
	}

	var commonp string
	ap := a.Path()
	if ap == "" {
		ap = "/dev/null"
	} else {
		commonp = ap
		ap = filepath.Join("a", ap)
	}
	bp := b.Path()
	if bp == "" {
		bp = "/dev/null"
	} else {
		commonp = bp
		bp = filepath.Join("b", bp)
	}

	if opts.verbose {
		if opts.namesOnly {
			_, _ = fmt.Fprintln(opts.output, commonp+"+meta")
		} else {
			_, _ = fmt.Fprintf(opts.output, "--- %s+meta\n+++ %s+meta\n", ap, bp)
			_, _ = fmt.Fprint(opts.output, output)
		}
	}

	an = treeNode{t: atree, n: a, maxSize: opts.maxSize}
	bn = treeNode{t: btree, n: b, maxSize: opts.maxSize}
	output, err = diff.Unified(an, bn, opts.contextLines)
	if errors.Is(err, errTreeNodeLarge) {
		_, _ = fmt.Fprintf(opts.output, "omitting diff for large node: %v\n", err)
		err = nil
	}
	if err != nil {
		return err
	}
	if output != "" || a == nil || b == nil || a.D.Mode&DMDIR != b.D.Mode&DMDIR {
		if opts.namesOnly {
			_, _ = fmt.Fprintln(opts.output, commonp)
		} else {
			_, _ = fmt.Fprintf(opts.output, "--- %s\n+++ %s\n", ap, bp)
			_, _ = fmt.Fprint(opts.output, output)
		}
	}

	// We can recurse only if they are both directories.
	if a == nil || b == nil || !a.IsDir() || !b.IsDir() {
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
