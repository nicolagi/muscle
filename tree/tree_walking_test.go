package tree

import (
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/google/go-cmp/cmp"
	"github.com/nicolagi/muscle/storage"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestWalk(t *testing.T) {
	oak := newTestTree(t)
	t.Run("walking from nil node is an error", func(t *testing.T) {
		visited, err := oak.Walk(nil)
		assert.Nil(t, visited)
		assert.NotNil(t, err)
	})
	t.Run("walking no steps aka clone", func(t *testing.T) {
		visited, err := oak.Walk(new(Node))
		assert.Nil(t, visited)
		assert.Nil(t, err)
	})
	t.Run("grow error at first step", func(t *testing.T) {
		a := new(Node)
		visited, err := oak.walk(a, func(node *Node) error {
			return errors.New("really unexpected")
		}, "usr")
		assert.Nil(t, visited)
		assert.NotNil(t, err)
	})
	t.Run("grow error at second step", func(t *testing.T) {
		var a, b Node
		b.info.Name = "usr"
		if err := a.add(&b); err != nil {
			t.Fatalf("%+v", err)
		}
		called := false
		growErr := errors.New("an error message")
		visited, err := oak.walk(&a, func(node *Node) error {
			if !called {
				node.flags |= loaded
				called = true
				return nil
			}
			return growErr
		}, "usr", "local")
		if got, want := len(visited), 1; got != want {
			t.Fatalf("got %v, want %v nodes", got, want)
		}
		if got, want := visited[0].info.Name, "usr"; got != want {
			t.Errorf("got %v, want %v nodes", got, want)
		}
		if !errors.Is(err, growErr) {
			t.Errorf("got %v, want %w", err, growErr)
		}
	})
	t.Run("interrupting walk at second step", func(t *testing.T) {
		var a, b Node
		b.info.Name = "usr"
		if err := a.add(&b); err != nil {
			t.Fatalf("%+v", err)
		}
		visited, err := oak.walk(&a, func(node *Node) error {
			node.flags |= loaded
			return nil
		}, "usr", "local")
		if got, want := len(visited), 1; got != want {
			t.Fatalf("got %v, want %v nodes", got, want)
		}
		if got, want := visited[0].info.Name, "usr"; got != want {
			t.Errorf("got %v, want %v nodes", got, want)
		}
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("got %v, want %w", err, ErrNotFound)
		}
	})
	t.Run("successfully walking two steps", func(t *testing.T) {
		root := &Node{pointer: storage.RandomPointer(), info: NodeInfo{Name: "root"}}
		usr := &Node{pointer: storage.RandomPointer()}
		bin := &Node{pointer: storage.RandomPointer()}
		if err := root.add(usr); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := usr.add(bin); err != nil {
			t.Fatalf("%+v", err)
		}
		visited, err := oak.walk(root, func(node *Node) error {
			child := node.children[0]
			switch child.pointer.Hex() {
			case usr.pointer.Hex():
				child.info.Name = "usr"
			case bin.pointer.Hex():
				child.info.Name = "bin"
			}
			node.flags |= loaded
			return nil
		}, "usr", "bin")
		assert.Len(t, visited, 2)
		assert.Equal(t, "usr", visited[0].info.Name)
		assert.Equal(t, "bin", visited[1].info.Name)
		assert.Nil(t, err)
	})
	t.Run("walk to parent", func(t *testing.T) {
		root := &Node{pointer: storage.RandomPointer(), info: NodeInfo{Name: "root"}}
		usr := &Node{pointer: storage.RandomPointer(), info: NodeInfo{Name: "usr"}}
		bin := &Node{pointer: storage.RandomPointer(), info: NodeInfo{Name: "bin"}}
		if err := root.add(usr); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := usr.add(bin); err != nil {
			t.Fatalf("%+v", err)
		}
		visited, err := oak.walk(bin, func(node *Node) error {
			node.flags |= loaded
			return nil
		}, "..")
		assert.Nil(t, err)
		assert.Len(t, visited, 1)
		assert.Equal(t, "usr", visited[0].info.Name)
	})
}

func TestGrow(t *testing.T) {
	oak := newTestTree(t)
	t.Run("growing nil is an error", func(t *testing.T) {
		assert.NotNil(t, oak.Grow(nil))
	})
	t.Run("growing at a node without children does nothing", func(t *testing.T) {
		a := Node{}
		b := a
		assert.Nil(t, oak.Grow(&a))
		assert.Equal(t, b, a)
	})
	t.Run("growing a node with only one child, not loaded", func(t *testing.T) {
		a := Node{
			children: []*Node{{}},
		}
		assert.Nil(t, oak.grow(&a, func(node *Node) error {
			node.info.Name = "etc"
			return nil
		}))
		assert.Equal(t, "etc", a.children[0].info.Name)
	})
	t.Run("growing a node with only one child, loaded", func(t *testing.T) {
		a := Node{
			children: []*Node{{flags: loaded, info: NodeInfo{Name: "boot"}}},
		}
		assert.Nil(t, oak.grow(&a, nil))
		assert.Equal(t, "boot", a.children[0].info.Name)
	})
	t.Run("growing a node with only one child missing from storage", func(t *testing.T) {
		a := Node{}
		if err := a.add(new(Node)); err != nil {
			t.Fatalf("%+v", err)
		}
		assert.Nil(t, oak.grow(&a, func(node *Node) error {
			return storage.ErrNotFound
		}))
		assert.Regexp(t, "vanished", a.children[0].info.Name)
		assert.Equal(t, dirty, a.children[0].flags&dirty)
		assert.Equal(t, dirty, a.flags&dirty)
	})
	t.Run("growing a node with two children with same name once loaded", func(t *testing.T) {
		a := new(Node)
		if err := a.add(new(Node)); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := a.add(new(Node)); err != nil {
			t.Fatalf("%+v", err)
		}
		assert.Nil(t, oak.grow(a, func(node *Node) error {
			node.info.Name = "usr"
			node.flags |= loaded
			return nil
		}))
		sort.Slice(a.children, func(i, j int) bool {
			return a.children[i].info.Name < a.children[j].info.Name
		})
		assert.Equal(t, "usr", a.children[0].info.Name)
		assert.Regexp(t, "usr\\.dupe[0-9]+", a.children[1].info.Name)
		assert.EqualValues(t, 0, a.children[0].flags&dirty)
		assert.Equal(t, dirty, a.children[1].flags&dirty)
		assert.Equal(t, dirty, a.flags&dirty)
	})
	t.Run("growing a node with three children and error for the second", func(t *testing.T) {
		defer leaktest.Check(t)()
		a := new(Node)
		for i := 0; i < 3; i++ {
			node := new(Node)
			// Something to track which node it is, to correlate assertions
			node.refs = i
			if err := a.add(node); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		loadCounter := int32(0)
		assert.NotNil(t, oak.grow(a, func(node *Node) error {
			defer func() {
				atomic.AddInt32(&loadCounter, 1)
			}()
			switch node.refs {
			case 0:
				node.info.Name = "var"
				return nil
			case 1:
				return errors.New("something unexpected")
			default:
				node.info.Name = "usr"
				return nil
			}
		}))
		firstName := a.children[0].info.Name
		secondName := a.children[1].info.Name
		thirdName := a.children[2].info.Name
		assert.Equal(t, "var", firstName)
		assert.Equal(t, "", secondName)
		assert.Equal(t, "usr", thirdName)
		assert.Equal(t, int32(3), atomic.LoadInt32(&loadCounter))
	})
	t.Run("duplicate arising when first node is loaded and second is not", func(t *testing.T) {
		a := new(Node)
		a.flags |= loaded
		if err := a.add(&Node{flags: loaded, info: NodeInfo{Name: "home"}}); err != nil {
			t.Fatalf("%+v", err)
		}
		a.flags &^= loaded
		if err := a.add(new(Node)); err != nil {
			t.Fatalf("%+v", err)
		}
		callCount := 0
		assert.Nil(t, oak.grow(a, func(node *Node) error {
			node.info.Name = "home"
			node.flags |= loaded
			callCount++
			return nil
		}))
		assert.Equal(t, "home", a.children[0].info.Name)
		assert.Regexp(t, "home\\.dupe[0-9]+", a.children[1].info.Name)
		assert.EqualValues(t, 0, a.children[0].flags&dirty)
		assert.Equal(t, dirty, a.children[1].flags&dirty)
		assert.Equal(t, dirty, a.flags&dirty)
		assert.Equal(t, 1, callCount)
	})
}

func TestChildNamesAreMadeUnique(t *testing.T) {
	newTestNode := func(names []string) *Node {
		parent := &Node{}
		for _, name := range names {
			child := &Node{}
			child.flags = loaded
			child.info.Name = name
			parent.children = append(parent.children, child)
		}
		return parent
	}
	extractChildNames := func(parent *Node) (allChildren []string, dirtyChildren []string) {
		for _, child := range parent.children {
			allChildren = append(allChildren, child.info.Name)
			if child.flags&dirty != 0 {
				dirtyChildren = append(dirtyChildren, child.info.Name)
			}
		}
		return
	}
	testCases := []struct {
		input  []string
		output []string
		dirty  []string // Children that should be marked dirty.
	}{
		{input: []string{}},
		{input: []string{"one"}, output: []string{"one"}},
		{input: []string{"one", "one"}, output: []string{"one", "one.dupe0"}, dirty: []string{"one.dupe0"}},
		{input: []string{"one", "one", "one"}, output: []string{"one", "one.dupe0", "one.dupe1"}, dirty: []string{"one.dupe0", "one.dupe1"}},
		{input: []string{"one", "two"}, output: []string{"one", "two"}},
		{input: []string{"one", "two", "one", "two"}, output: []string{"one", "two", "one.dupe0", "two.dupe0"}, dirty: []string{"one.dupe0", "two.dupe0"}},
		{input: []string{"one", "one.dupe1"}, output: []string{"one", "one.dupe1"}},
		{input: []string{"one", "one", "one.dupe0"}, output: []string{"one", "one.dupe1", "one.dupe0"}, dirty: []string{"one.dupe1"}},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			parent := newTestNode(tc.input)
			makeChildNamesUnique(parent)
			all, dirty := extractChildNames(parent)
			if diff := cmp.Diff(all, tc.output); diff != "" {
				t.Errorf("Unexpected child names difference: %s", diff)
			}
			if diff := cmp.Diff(dirty, tc.dirty); diff != "" {
				t.Errorf("Unexpected dirty child names difference: %s", diff)
			}
		})
	}
	t.Run("does not touch nodes that were never loaded", func(t *testing.T) {
		parent := newTestNode([]string{"", ""})
		for _, c := range parent.children {
			c.flags &^= loaded
		}
		makeChildNamesUnique(parent)
		all, dirty := extractChildNames(parent)
		if diff := cmp.Diff(all, []string{"", ""}); diff != "" {
			t.Errorf("Unexpected child names difference: %s", diff)
		}
		if diff := cmp.Diff(dirty, []string(nil)); diff != "" {
			t.Errorf("Unexpected dirty child names difference: %s", diff)
		}
	})
}

func TestGrowParallelizationLimit(t *testing.T) {
	tree := &Tree{}
	parent := &Node{}
	for i := 0; i < 64; i++ { // One more than the limit.
		parent.children = append(parent.children, &Node{})
	}
	start := time.Now()
	err := tree.grow(parent, func(*Node) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	elapsed := time.Since(start)
	lb := 75 * time.Millisecond
	if elapsed < lb {
		t.Errorf("got %v, want at least %v", elapsed, lb)
	}
}
