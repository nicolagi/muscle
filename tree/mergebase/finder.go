package mergebase

import (
	"bytes"
	"fmt"
	stdlog "log"
	"sort"

	"github.com/nicolagi/muscle/storage"
	"github.com/pkg/errors"
)

var (
	NullNode  Node
	NullGraph Graph

	ErrNoMergeBase = errors.New("no merge base")
)

type arrow struct {
	graph  string
	source string
	target string
}

type sorted []arrow

func (arrows sorted) Len() int {
	return len(arrows)
}

func (arrows sorted) Less(i, j int) bool {
	if arrows[i].source != arrows[j].source {
		return arrows[i].source < arrows[j].source
	}
	if arrows[i].graph != arrows[j].graph {
		return arrows[i].graph < arrows[j].graph
	}
	return arrows[i].target < arrows[j].target
}

func (arrows sorted) Swap(i, j int) {
	arrows[i], arrows[j] = arrows[j], arrows[i]
}

type Graph struct {
	arrows []arrow
	base   Node
}

// String implements fmt.Stringer. The output can be converted to many image
// formats with dot (graphviz).
func (g Graph) String() string {
	var buf bytes.Buffer
	_, _ = fmt.Fprintln(&buf, "digraph {")
	for _, a := range g.arrows {
		_, _ = fmt.Fprintf(&buf, "%q -> %q [label=%q color=%q];\n", a.source, a.target, a.graph, colors.colorFor(a.graph))
	}
	_, _ = fmt.Fprintf(&buf, "%.8q [style=%q fillcolor=%q];\n", g.base.ID, "filled", "#55ffff")
	_, _ = fmt.Fprintln(&buf, "}")
	return buf.String()
}

type Node struct {
	ID      string
	GraphID string
}

// ParentFunc takes a child node and fetches its parents.
type ParentFunc func(child Node) (parents []Node, err error)

type nodeSet map[Node]struct{}

func newNodeSet(initial ...Node) nodeSet {
	nodes := make(nodeSet)
	for _, node := range initial {
		nodes[node] = struct{}{}
	}
	return nodes
}

func (nodes nodeSet) add(node Node) {
	nodes[node] = struct{}{}
}

func (nodes nodeSet) contains(node Node) bool {
	_, ok := nodes[node]
	return ok
}

type finder struct {
	fn ParentFunc

	// Index into below arrays,  mod 2.
	// Could maybe exposed as a measure of distance from the merge base.
	iter int

	heads   [2]nodeSet
	visited [2]nodeSet

	arrows []arrow
	base   Node
	err    error
}

func newFinder(a, b Node, fn ParentFunc) *finder {
	var f finder
	f.fn = fn
	f.heads[0] = newNodeSet(a)
	f.heads[1] = newNodeSet(b)
	f.visited[0] = newNodeSet()
	f.visited[1] = newNodeSet()
	return &f
}

func (f *finder) grow() {
	if f.base != NullNode || f.err != nil {
		return
	}
	if len(f.heads[0])+len(f.heads[1]) == 0 {
		f.err = ErrNoMergeBase
		return
	}
	a := f.iter % 2
	b := (f.iter + 1) % 2
	nextHeads := newNodeSet()
outerLoop:
	for child := range f.heads[a] {
		if f.visited[a].contains(child) {
			continue
		}
		f.visited[a].add(child)
		parents, err := f.fn(child)
		if errors.Is(err, storage.ErrNotFound) {
			stdlog.Printf("Trimming search path because child %q was not found.", child)
			continue
		}
		if err != nil {
			f.err = err
			break
		}
		for _, parent := range parents {
			f.arrows = append(f.arrows, arrow{
				source: fmt.Sprintf("%.8s", child.ID),
				target: fmt.Sprintf("%.8s", parent.ID),
				graph:  child.GraphID,
			})
			nextHeads.add(parent)
			if f.visited[b].contains(parent) || f.heads[b].contains(parent) {
				f.base = parent
				break outerLoop
			}
		}
	}
	f.heads[a] = nextHeads
	f.iter++
}

// Find finds a merge base for a and b.
func Find(a, b Node, fn ParentFunc) (graph Graph, base Node, error error) {
	if a.ID == b.ID {
		return NullGraph, a, nil
	}
	finder := newFinder(a, b, fn)
	for finder.base == NullNode && finder.err == nil {
		finder.grow()
	}
	sort.Sort(sorted(finder.arrows))
	return Graph{arrows: finder.arrows, base: finder.base}, finder.base, finder.err
}
