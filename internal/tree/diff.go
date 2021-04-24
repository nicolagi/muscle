package tree

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type diffTreesOptions struct {
	namesOnly   bool
	verbose     bool
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
func DiffTrees(a, b *Tree, arootpath, brootpath string, options ...DiffTreesOption) error {
	opts := diffTreesOptions{
		output: ioutil.Discard,
	}
	for _, opt := range options {
		opt(&opts)
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
	return diffTrees(a, b, arootpath, brootpath, aInitial, bInitial, &opts)
}

func metaDiff(a, b *Node) string {
	w := new(bytes.Buffer)
	if a == nil && b == nil {
		return ""
	}
	if a != nil && b != nil && a.pointer.Equals(b.pointer) {
		return ""
	}
	modtime := func(node *Node) string {
		return time.Unix(int64(node.info.Modified), 0).UTC().Format(time.RFC3339)
	}
	blockstring := func(node *Node) string {
		var refs []string
		for _, b := range node.blocks {
			refs = append(refs, fmt.Sprintf("%q", b.Ref()))
		}
		return strings.Join(refs, " ")
	}
	if a == nil && b != nil {
		_, _ = fmt.Fprintf(w, "+Key %q\n", b.pointer.Hex())
		_, _ = fmt.Fprintf(w, "+Dir.Qid.Version %x\n", b.info.Version)
		_, _ = fmt.Fprintf(w, "+Dir.Qid.Path %d\n", b.info.ID)
		_, _ = fmt.Fprintf(w, "+Dir.Mode %d\n", b.info.Mode)
		_, _ = fmt.Fprintf(w, "+Dir.Mtime %s\n", modtime(b))
		_, _ = fmt.Fprintf(w, "+Dir.Length %d\n", b.info.Size)
		_, _ = fmt.Fprintf(w, "+Dir.Name %q\n", b.info.Name)
		_, _ = fmt.Fprintf(w, "+Blocks %s\n", blockstring(b))
	} else if a != nil && b == nil {
		_, _ = fmt.Fprintf(w, "-Key %q\n", a.pointer.Hex())
		_, _ = fmt.Fprintf(w, "-Dir.Qid.Version %x\n", a.info.Version)
		_, _ = fmt.Fprintf(w, "-Dir.Qid.Path %d\n", a.info.ID)
		_, _ = fmt.Fprintf(w, "-Dir.Mode %d\n", a.info.Mode)
		_, _ = fmt.Fprintf(w, "-Dir.Mtime %s\n", modtime(a))
		_, _ = fmt.Fprintf(w, "-Dir.Length %d\n", a.info.Size)
		_, _ = fmt.Fprintf(w, "-Dir.Name %q\n", a.info.Name)
		_, _ = fmt.Fprintf(w, "-Blocks %s\n", blockstring(a))
	} else {
		_, _ = fmt.Fprintf(w, "-Key %s\n+Key %s\n", a.pointer.Hex(), b.pointer.Hex())
		if a.info.Version != b.info.Version {
			_, _ = fmt.Fprintf(w, "-Dir.Qid.Version %x\n+Dir.Qid.Version %x\n", a.info.Version, b.info.Version)
		} else {
			_, _ = fmt.Fprintf(w, " Dir.Qid.Version %x\n", a.info.Version)
		}
		if a.info.ID != b.info.ID {
			_, _ = fmt.Fprintf(w, "-Dir.Qid.Path %d\n+Dir.Qid.Path %d\n", a.info.ID, b.info.ID)
		} else {
			_, _ = fmt.Fprintf(w, " Dir.Qid.Path %d\n", a.info.ID)
		}
		if a.info.Mode != b.info.Mode {
			_, _ = fmt.Fprintf(w, "-Dir.Mode %d\n+Dir.Mode %d\n", a.info.Mode, b.info.Mode)
		} else {
			_, _ = fmt.Fprintf(w, " Dir.Mode %d\n", a.info.Mode)
		}
		if a.info.Modified != b.info.Modified {
			_, _ = fmt.Fprintf(w, "-Dir.Mtime %q\n+Dir.Mtime %q\n", modtime(a), modtime(b))
		} else {
			_, _ = fmt.Fprintf(w, " Dir.Mtime %q\n", modtime(a))
		}
		if a.info.Size != b.info.Size {
			_, _ = fmt.Fprintf(w, "-Dir.Length %d\n+Dir.Length %d\n", a.info.Size, b.info.Size)
		} else {
			_, _ = fmt.Fprintf(w, " Dir.Length %d\n", a.info.Size)
		}
		if a.info.Name != b.info.Name {
			_, _ = fmt.Fprintf(w, "-Dir.Name %q\n+Dir.Name %q\n", a.info.Name, b.info.Name)
		} else {
			_, _ = fmt.Fprintf(w, " Dir.Name %q\n", a.info.Name)
		}
		if left, right := blockstring(a), blockstring(b); left != right {
			_, _ = fmt.Fprintf(w, "-Blocks %s\n+Blocks %s\n", left, right)
		} else {
			_, _ = fmt.Fprintf(w, " Blocks %s\n", left)
		}
	}
	return w.String()
}

func diffTrees(atree, btree *Tree, arootpath, brootpath string, a, b *Node, opts *diffTreesOptions) error {
	output := metaDiff(a, b)
	if output == "" {
		return nil
	}

	var ap, bp string
	if a == nil {
		ap = "/dev/null"
	} else {
		ap = filepath.Join(arootpath, a.Path())
	}
	if b == nil {
		bp = "/dev/null"
	} else {
		bp = filepath.Join(brootpath, b.Path())
	}

	if opts.verbose {
		if opts.namesOnly {
			if b == nil {
				_, _ = fmt.Fprintln(opts.output, ap+"+meta")
			} else {
				_, _ = fmt.Fprintln(opts.output, bp+"+meta")
			}
		} else {
			_, _ = fmt.Fprintf(opts.output, "--- %s+meta\n+++ %s+meta\n", ap, bp)
			_, _ = fmt.Fprint(opts.output, output)
		}
	}

	if a == nil || b == nil || !a.IsDir() || !b.IsDir() {
		if opts.namesOnly {
			if b == nil {
				_, _ = fmt.Fprintln(opts.output, ap)
			} else {
				_, _ = fmt.Fprintln(opts.output, bp)
			}
		} else {
			_, _ = fmt.Fprintf(opts.output, "diff -u %s %s\n", ap, bp)
		}
		// We can recurse only if they are both directories.
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
		if err := diffTrees(atree, btree, arootpath, brootpath, achildren[name], bchildren[name], opts); err != nil {
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
