package tree

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/nicolagi/muscle/storage"
)

// TODO: Keeping this counter is a kludge that I use to visually trace the merge base algorithm looking at a generated graph.
var edgeOutputCounter int

// revisionsByTime implements sort.Interface.
type revisionsByTime []*Revision

// Len is the number of elements in the collection.
func (rr revisionsByTime) Len() int {
	return len(rr)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (rr revisionsByTime) Less(i int, j int) bool {
	return rr[i].When > rr[j].When
}

// Swap swaps the elements with indexes i and j.
func (rr revisionsByTime) Swap(i int, j int) {
	rr[i], rr[j] = rr[j], rr[i]
}

type revisionSet map[*Revision]struct{}

func (a revisionSet) commonRootPointer(b revisionSet) storage.Pointer {
	var common []*Revision
	for aRev := range a {
		for bRev := range b {
			if aRev.rootKey.Equals(bRev.rootKey) {
				common = append(common, aRev)
			}
		}
	}
	for _, r := range common {
		log.WithField("revision", r).Debug("Common revision")
	}
	// TODO This is not the optimal thing to do, because you can't trust the timestamp.
	// You should minimize the total distance (no. of edges) (a,r)+(b,r) I think.
	sort.Sort(revisionsByTime(common))
	if len(common) > 0 {
		// TODO This is terrible because we're returning a revision key, while the thing in common
		// is the root key. Need to refactor.
		return common[0].key
	}
	return storage.Null
}

func (a revisionSet) merge(b revisionSet) {
	for r := range b {
		a[r] = struct{}{}
	}
}

func (a revisionSet) add(r *Revision) {
	a[r] = struct{}{}
}

func (s *Store) findAncestor(a, b *Revision, output io.Writer) (storage.Pointer, error) {
	aHeads := make(revisionSet)
	bHeads := make(revisionSet)
	aHeads.add(a)
	bHeads.add(b)
	aHistory := make(revisionSet)
	bHistory := make(revisionSet)
	for i := 0; i < 1<<9; i++ {
		if k := aHistory.commonRootPointer(bHistory); !k.IsNull() {
			return k, nil
		}
		aHistory.merge(aHeads)
		bHistory.merge(bHeads)

		var err error
		aHeads, err = s.parents(aHeads, output)
		if err != nil {
			return storage.Null, err
		}
		bHeads, err = s.parents(bHeads, output)
		if err != nil {
			return storage.Null, err
		}
	}
	return storage.Null, errors.New("too many iterations")
}

func (s *Store) parents(heads revisionSet, output io.Writer) (revisionSet, error) {
	allParents := make(revisionSet)
	for child := range heads {
		for _, parentKey := range child.parents {
			parent := &Revision{key: parentKey}
			if err := s.LoadRevision(parent); err != nil {
				return nil, err
			}
			if !child.rootKey.Equals(parent.rootKey) {
				host := strings.Split(child.hostname, ".")[0]
				fmt.Fprintf(
					output,
					"%q -> %q [label=%q color=%q];\n",
					revNode(child),
					revNode(parent),
					fmt.Sprintf("%s-%d", host, edgeOutputCounter),
					colorFor(host),
				)
				edgeOutputCounter++
			}
			allParents.add(parent)
		}
	}
	return allParents, nil
}

func revNode(r *Revision) string {
	return r.rootKey.Hex()[:8]
}

var (
	colors = []string{
		"#ff0000",
		"#0000ff",
		"#00ff00",
		"#ff00ff",
		"#ffff00",
		"#00ffff",
	}
	nextColor = 0
	hostColor = make(map[string]string)
)

func colorFor(host string) string {
	c, ok := hostColor[host]
	if !ok {
		c = colors[nextColor]
		hostColor[host] = c
		nextColor = (nextColor + 1) % len(colors)
	}
	return c
}
