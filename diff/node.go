package diff

import (
	"bytes"
)

type Node interface {
	// SameAs is an optional shortcut to comparing nodes. This
	// could be implemented, for instance, if the nodes to compare
	// contain hashes of their contents. You'd quickly compare the
	// hashes before comparing contents. If no shortcuts are
	// possible, one should return false.
	SameAs(Node) bool

	// Content returns the content of the node.
	Content() (string, error)
}

type ByteNode []byte

func (b ByteNode) SameAs(node Node) bool {
	other, ok := node.(ByteNode)
	if !ok {
		return false
	}
	return bytes.Equal(b, other)
}

func (b ByteNode) Content() (string, error) {
	return string(b), nil
}

type StringNode string

func (s StringNode) SameAs(node Node) bool {
	other, ok := node.(StringNode)
	if !ok {
		return false
	}
	return string(s) == string(other)
}

func (s StringNode) Content() (string, error) {
	return string(s), nil
}
