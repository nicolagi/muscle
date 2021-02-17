package tree

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/nicolagi/muscle/internal/storage"
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
		a.flags = loaded
		b.flags = loaded
		b.info.Name = "usr"
		if err := a.addChild(&b); err != nil {
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
		a.flags = loaded
		b.flags = loaded
		b.info.Name = "usr"
		if err := a.addChild(&b); err != nil {
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
		if !errors.Is(err, ErrNotExist) {
			t.Errorf("got %v, want %w", err, ErrNotExist)
		}
	})
	t.Run("successfully walking two steps", func(t *testing.T) {
		var root Node
		root.info.Name = "root"
		root.pointer = storage.RandomPointer()
		usr := storage.RandomPointer()
		if err := root.addChildPointer(usr); err != nil {
			t.Fatalf("%+v", err)
		}
		bin := storage.RandomPointer()
		if err := root.children[0].addChildPointer(bin); err != nil {
			t.Fatalf("%+v", err)
		}
		visited, err := oak.walk(&root, func(node *Node) error {
			for _, child := range node.children {
				switch child.pointer.Hex() {
				case usr.Hex():
					child.info.Name = "usr"
				case bin.Hex():
					child.info.Name = "bin"
				}
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
		var root, usr, bin Node
		root.flags = loaded
		root.info.Name = "root"
		root.pointer = storage.RandomPointer()
		usr.flags = loaded
		usr.info.Name = "usr"
		usr.pointer = storage.RandomPointer()
		bin.flags = loaded
		bin.info.Name = "bin"
		bin.pointer = storage.RandomPointer()
		if err := root.addChild(&usr); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := usr.addChild(&bin); err != nil {
			t.Fatalf("%+v", err)
		}
		visited, err := oak.walk(&bin, func(node *Node) error {
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
	t.Run("growing a node with three children and error for the second", func(t *testing.T) {
		defer leaktest.Check(t)()
		a := new(Node)
		for i := 0; i < 3; i++ {
			if err := a.addChildPointer(storage.RandomPointer()); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		for i := 0; i < 3; i++ {
			// Something to track which node it is, to correlate assertions
			a.children[i].refs = i
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
