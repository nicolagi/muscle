package tree

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nicolagi/muscle/diff"
)

const maxBlobSizeForDiff = 1024 * 1024

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

func (node nodeMeta) SameAs(otherNode diff.Node) bool {
	other, ok := otherNode.(nodeMeta)
	if !ok {
		return false
	}
	if node.n == nil && other.n == nil {
		return true
	}
	if node.n == nil || other.n == nil {
		return false
	}
	return node.n.pointer.Equals(other.n.pointer)
}

func (node nodeMeta) Content() (string, error) {
	return node.n.DiffRepr(), nil
}

// treeNode is an implementation of diff.Node for file system node contents.
type treeNode struct {
	t *Tree
	n *Node
}

func (node treeNode) SameAs(otherNode diff.Node) bool {
	other, ok := otherNode.(treeNode)
	if !ok {
		return false
	}
	if node.n == nil && other.n == nil {
		return true
	}
	if node.n == nil || other.n == nil {
		return false
	}
	return node.n.hasEqualBlocks(other.n)
}

func (node treeNode) Content() (string, error) {
	if node.n == nil {
		return "", nil
	}
	nodeSize := int(node.n.D.Length)
	if nodeSize > maxBlobSizeForDiff {
		return "", fmt.Errorf("%d: %w", nodeSize, errTreeNodeLarge)
	}
	content := make([]byte, nodeSize)
	n, err := node.t.ReadAt(node.n, content, 0)
	if err != nil {
		return "", err
	}
	if n != nodeSize {
		return "", fmt.Errorf("got %d out of %d bytes: %w", n, nodeSize, errTreeNodeTruncated)
	}
	return string(content), err
}

type diffTreesOptions struct {
	contextLines int
	namesOnly    bool
	verbose      bool
	output       io.Writer
	initialPath  string
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

	an = treeNode{t: atree, n: a}
	bn = treeNode{t: btree, n: b}
	output, err = diff.Unified(an, bn, opts.contextLines)
	if errors.Is(err, errTreeNodeLarge) {
		_, _ = fmt.Fprintf(opts.output, "omitting diff for large node: %v\n", err)
		err = nil
	}
	if err != nil {
		return err
	}
	if output != "" {
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
