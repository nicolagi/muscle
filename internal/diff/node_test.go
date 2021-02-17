package diff_test

import (
	"testing"

	"github.com/nicolagi/muscle/internal/diff"
)

func TestByteNodeSameAs(t *testing.T) {
	a := diff.ByteNode("some text")
	b := diff.ByteNode("other text")
	assertNotSame(t, a, b)
	assertSame(t, a, a)
	assertSame(t, b, b)
	assertSame(t, a, diff.ByteNode("some text"))
	assertNotSame(t, a, (diff.ByteNode)(nil))
	assertNotSame(t, a, diff.StringNode("some text"))
}

func TestByteNodeContent(t *testing.T) {
	node := diff.ByteNode("some text")
	content, err := node.Content()
	if err != nil {
		t.Error(err)
	}
	if got, want := content, "some text"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestStringNodeSameAs(t *testing.T) {
	a := diff.StringNode("some text")
	b := diff.StringNode("other text")
	assertNotSame(t, a, b)
	assertSame(t, a, a)
	assertSame(t, b, b)
	assertSame(t, a, diff.StringNode("some text"))
	assertNotSame(t, a, (diff.ByteNode)(nil))
	assertNotSame(t, a, diff.ByteNode{})
}

func TestStringNodeContent(t *testing.T) {
	node := diff.StringNode("some text")
	content, err := node.Content()
	if err != nil {
		t.Error(err)
	}
	if got, want := content, "some text"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func assertSame(t *testing.T, a, b diff.Node) {
	t.Helper()
	assertComparison(t, a, b, true)
	assertComparison(t, b, a, true)
}

func assertNotSame(t *testing.T, a, b diff.Node) {
	t.Helper()
	assertComparison(t, a, b, false)
	assertComparison(t, b, a, false)
}

func assertComparison(t *testing.T, a, b diff.Node, want bool) {
	t.Helper()
	got, err := a.SameAs(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %t, want %t", got, want)
	}
}
