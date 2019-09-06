package mergebase_test

import (
	"testing"

	"github.com/nicolagi/muscle/tree/mergebase"
	"github.com/stretchr/testify/assert"
)

func TestFind(t *testing.T) {
	t.Run("same node", func(t *testing.T) {
		node := simpleNode("node", "slackware")
		graph, base, err := mergebase.Find(node, node, nil)
		assert.Nil(t, err)
		assert.Equal(t, mergebase.NullGraph, graph)
		assert.Equal(t, node, base)
	})
	t.Run("different nodes with same parent", func(t *testing.T) {
		common := simpleNode("common", "slackware")
		a0 := simpleNode("a0", "slackware")
		b0 := simpleNode("b0", "debian")
		constantParentFunc := func(mergebase.Node) ([]mergebase.Node, error) {
			return []mergebase.Node{common}, nil
		}
		graph, base, err := mergebase.Find(a0, b0, constantParentFunc)
		assert.Nil(t, err)
		assert.Equal(t, common, base)
		assert.Equal(t, `digraph {
"a0" -> "common" [label="slackware" color="#ffffaa"];
"b0" -> "common" [label="debian" color="#aa5500"];
"common" [style="filled" fillcolor="#55ffff"];
}
`, graph.String())
	})
	t.Run("different nodes with same grand parent", func(t *testing.T) {
		parentFunc := func(node mergebase.Node) ([]mergebase.Node, error) {
			switch node.ID {
			case "a0":
				return []mergebase.Node{simpleNode("a1", "slackware")}, nil
			case "a1":
				return []mergebase.Node{simpleNode("common", "slackware")}, nil
			case "b0":
				return []mergebase.Node{simpleNode("b1", "debian")}, nil
			case "b1":
				return []mergebase.Node{simpleNode("common", "slackware")}, nil
			default:
				return nil, nil
			}
		}
		a0 := simpleNode("a0", "slackware")
		b0 := simpleNode("b0", "debian")
		graph, base, err := mergebase.Find(a0, b0, parentFunc)
		assert.Nil(t, err)
		assert.Equal(t, "common", base.ID)
		assert.Equal(t, `digraph {
"a0" -> "a1" [label="slackware" color="#ffffaa"];
"a1" -> "common" [label="slackware" color="#ffffaa"];
"b0" -> "b1" [label="debian" color="#aa5500"];
"b1" -> "common" [label="debian" color="#aa5500"];
"common" [style="filled" fillcolor="#55ffff"];
}
`, graph.String())
	})
	t.Run("branching more than once", func(t *testing.T) {
		parentFunc := func(node mergebase.Node) ([]mergebase.Node, error) {
			switch node.ID {
			case "a0":
				return []mergebase.Node{
					simpleNode("a1", "slackware"),
					simpleNode("a2", "slackware"),
				}, nil
			case "a1":
				return []mergebase.Node{
					simpleNode("a3", "slackware"),
				}, nil
			case "a2":
				return []mergebase.Node{
					simpleNode("a5", "slackware"),
					simpleNode("a6", "slackware"),
				}, nil
			case "a3":
				return []mergebase.Node{
					simpleNode("a4", "slackware"),
				}, nil
			case "a4":
				return []mergebase.Node{
					simpleNode("common", "debian"),
				}, nil
			case "b0":
				return []mergebase.Node{
					simpleNode("common", "debian"),
				}, nil
			default:
				return nil, nil
			}
		}
		a0 := simpleNode("a0", "slackware")
		b0 := simpleNode("b0", "debian")
		graph, base, err := mergebase.Find(a0, b0, parentFunc)
		assert.Nil(t, err)
		assert.Equal(t, "common", base.ID)
		assert.Equal(t, `digraph {
"a0" -> "a1" [label="slackware" color="#ffffaa"];
"a0" -> "a2" [label="slackware" color="#ffffaa"];
"a1" -> "a3" [label="slackware" color="#ffffaa"];
"a2" -> "a5" [label="slackware" color="#ffffaa"];
"a2" -> "a6" [label="slackware" color="#ffffaa"];
"a3" -> "a4" [label="slackware" color="#ffffaa"];
"a4" -> "common" [label="slackware" color="#ffffaa"];
"b0" -> "common" [label="debian" color="#aa5500"];
"common" [style="filled" fillcolor="#55ffff"];
}
`, graph.String())
	})
	t.Run("no common merge base", func(t *testing.T) {
		a0 := simpleNode("a0", "slackware")
		b0 := simpleNode("b0", "debian")
		graph, base, err := mergebase.Find(a0, b0, func(child mergebase.Node) (parents []mergebase.Node, err error) {
			return nil, nil
		})
		assert.Equal(t, mergebase.NullNode, base)
		assert.Equal(t, mergebase.NullGraph, graph)
		assert.Equal(t, mergebase.ErrNoMergeBase, err)
	})
}

func simpleNode(name string, instance string) mergebase.Node {
	return mergebase.Node{
		ID:      name,
		GraphID: instance,
	}
}
