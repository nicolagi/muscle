package tree

import (
	"errors"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/google/go-cmp/cmp"
	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/muscle/storage"
	"github.com/stretchr/testify/assert"
)

func TestWalk(t *testing.T) {
	oak, err := NewFactory(nil).NewTree()
	if err != nil {
		t.Fatal(err)
	}
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
		a := new(Node)
		a.add(new(Node))
		called := false
		visited, err := oak.walk(a, func(node *Node) error {
			if !called {
				node.children[0].D.Name = "usr"
				called = true
				return nil
			}
			return errors.New("really unexpected")
		}, "usr", "local")
		assert.Len(t, visited, 1)
		assert.Equal(t, "usr", visited[0].D.Name)
		assert.NotNil(t, err)
	})
	t.Run("interrupting walk at second step", func(t *testing.T) {
		a := new(Node)
		a.add(new(Node))
		visited, err := oak.walk(a, func(node *Node) error {
			if len(node.children) == 1 {
				node.children[0].D.Name = "usr"
			}
			return nil
		}, "usr", "local")
		assert.Len(t, visited, 1)
		assert.Equal(t, "usr", visited[0].D.Name)
		assert.NotNil(t, err)
	})
	t.Run("successfully walking two steps", func(t *testing.T) {
		root := &Node{pointer: storage.RandomPointer(), D: p.Dir{Name: "root"}}
		usr := &Node{pointer: storage.RandomPointer()}
		bin := &Node{pointer: storage.RandomPointer()}
		root.add(usr)
		usr.add(bin)
		visited, err := oak.walk(root, func(node *Node) error {
			child := node.children[0]
			switch child.pointer.Hex() {
			case usr.pointer.Hex():
				child.D.Name = "usr"
			case bin.pointer.Hex():
				child.D.Name = "bin"
			}
			return nil
		}, "usr", "bin")
		assert.Len(t, visited, 2)
		assert.Equal(t, "usr", visited[0].D.Name)
		assert.Equal(t, "bin", visited[1].D.Name)
		assert.Nil(t, err)
	})

	t.Run("walk to parent", func(t *testing.T) {
		root := &Node{pointer: storage.RandomPointer(), D: p.Dir{Name: "root"}}
		usr := &Node{pointer: storage.RandomPointer(), D: p.Dir{Name: "usr"}}
		bin := &Node{pointer: storage.RandomPointer(), D: p.Dir{Name: "bin"}}
		root.add(usr)
		usr.add(bin)
		visited, err := oak.walk(bin, func(node *Node) error {
			return nil
		}, "..")
		assert.Nil(t, err)
		assert.Len(t, visited, 1)
		assert.Equal(t, "usr", visited[0].D.Name)
	})
}

func TestGrow(t *testing.T) {
	oak, err := NewFactory(nil).NewTree()
	if err != nil {
		t.Fatal(err)
	}
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
			node.D.Name = "etc"
			return nil
		}))
		assert.Equal(t, "etc", a.children[0].D.Name)
	})
	t.Run("growing a node with only one child, loaded", func(t *testing.T) {
		a := Node{
			children: []*Node{{D: p.Dir{Name: "boot"}}},
		}
		assert.Nil(t, oak.grow(&a, nil))
		assert.Equal(t, "boot", a.children[0].D.Name)
	})
	t.Run("growing a node with only one child missing from storage", func(t *testing.T) {
		a := Node{}
		a.add(new(Node))
		assert.Nil(t, oak.grow(&a, func(node *Node) error {
			return storage.ErrNotFound
		}))
		assert.Regexp(t, "vanished", a.children[0].D.Name)
		assert.True(t, a.children[0].dirty)
		assert.True(t, a.dirty)
	})
	t.Run("growing a node with two children with same name once loaded", func(t *testing.T) {
		a := new(Node)
		a.add(new(Node))
		a.add(new(Node))
		assert.Nil(t, oak.grow(a, func(node *Node) error {
			node.D.Name = "usr"
			return nil
		}))
		sort.Sort(NodeSlice(a.children))
		assert.Equal(t, "usr", a.children[0].D.Name)
		assert.Regexp(t, "usr\\.dupe[0-9]+", a.children[1].D.Name)
		assert.False(t, a.children[0].dirty)
		assert.True(t, a.children[1].dirty)
		assert.True(t, a.dirty)
	})
	t.Run("growing a node with three children and error for the second", func(t *testing.T) {
		defer leaktest.Check(t)()
		a := new(Node)
		for i := 0; i < 3; i++ {
			node := new(Node)
			// Something to track which node it is, to correlate assertions
			node.refs = i
			a.add(node)
		}
		loadCounter := int32(0)
		assert.NotNil(t, oak.grow(a, func(node *Node) error {
			defer func() {
				atomic.AddInt32(&loadCounter, 1)
			}()
			switch node.refs {
			case 0:
				node.D.Name = "var"
				return nil
			case 1:
				return errors.New("something unexpected")
			default:
				node.D.Name = "usr"
				return nil
			}
		}))
		firstName := a.children[0].D.Name
		secondName := a.children[1].D.Name
		thirdName := a.children[2].D.Name
		assert.Equal(t, "var", firstName)
		assert.Equal(t, "", secondName)
		assert.Equal(t, "usr", thirdName)
		assert.Equal(t, int32(3), atomic.LoadInt32(&loadCounter))
	})
	t.Run("duplicate arising when first node is loaded and second is not", func(t *testing.T) {
		a := new(Node)
		a.add(&Node{D: p.Dir{Name: "home"}})
		a.add(new(Node))
		callCount := 0
		assert.Nil(t, oak.grow(a, func(node *Node) error {
			node.D.Name = "home"
			callCount++
			return nil
		}))
		assert.Equal(t, "home", a.children[0].D.Name)
		assert.Regexp(t, "home\\.dupe[0-9]+", a.children[1].D.Name)
		assert.False(t, a.children[0].dirty)
		assert.True(t, a.children[1].dirty)
		assert.True(t, a.dirty)
		assert.Equal(t, 1, callCount)
	})
}

func TestChildNamesAreMadeUnique(t *testing.T) {
	newTestNode := func(names []string) *Node {
		parent := &Node{}
		for _, name := range names {
			child := &Node{}
			child.D.Name = name
			parent.children = append(parent.children, child)
		}
		return parent
	}
	extractChildNames := func(parent *Node) (all []string, dirty []string) {
		for _, child := range parent.children {
			all = append(all, child.D.Name)
			if child.dirty {
				dirty = append(dirty, child.D.Name)
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
}
