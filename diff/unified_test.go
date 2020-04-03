package diff_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/nicolagi/muscle/diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type brokenNode struct{}

func (brokenNode) SameAs(diff.Node) bool {
	panic("broken")
}

func (brokenNode) Content() (string, error) {
	panic("broken")
}

type nodeSameAsAny struct {
	brokenNode
}

func (n nodeSameAsAny) SameAs(diff.Node) (bool, error) {
	return true, nil
}

type contentErrorNode struct {
	err error
}

func (contentErrorNode) SameAs(diff.Node) (bool, error) {
	return false, nil
}

func (node contentErrorNode) Content() (string, error) {
	return "", node.err
}

type sameAsErrorNode struct {
	err error
}

func (node sameAsErrorNode) SameAs(diff.Node) (bool, error) {
	return false, node.err
}

func (sameAsErrorNode) Content() (string, error) {
	panic("not implemented")
}

func TestUnifiedIfNodesSameNoDiff(t *testing.T) {
	var a, b nodeSameAsAny
	for _, right := range []diff.Node{a, b, nil} {
		diffOutput, err := diff.Unified(a, right, rand.Intn(100))
		assert.Empty(t, diffOutput)
		assert.Nil(t, err)
	}
}

func TestUnified(t *testing.T) {
	t.Run("when SameAs returns an error, Unified fails in turn", func(t *testing.T) {
		a := sameAsErrorNode{err: fmt.Errorf("an error")}
		b := nodeSameAsAny{}
		out, err := diff.Unified(a, b, 5)
		if got, want := out, ""; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
		if err == nil {
			t.Errorf("got nil, want non-nil error")
		}
	})
}

func TestUnifiedPassesContentError(t *testing.T) {
	a := contentErrorNode{err: errors.New("any error")}
	b := contentErrorNode{err: nil}
	for _, pair := range [][2]diff.Node{
		{a, a},
		{a, b},
		{b, a},
	} {
		diffOutput, err := diff.Unified(pair[0], pair[1], rand.Intn(100))
		assert.Equal(t, "", diffOutput)
		assert.True(t, errors.Is(err, a.err))
	}
}

// From https://www.gnu.org/software/diffutils/manual/html_node/Binary.html:
// diff determines whether a file is text or binary by checking the first few
// bytes in the file; the exact number of bytes is system dependent, but it is
// typically several thousand. If every byte in that part of the file is
// non-null, diff considers the file to be text; otherwise it considers the file
// to be binary.
func TestUnifiedRecognizesBinaryFiles(t *testing.T) {
	a := diff.ByteNode{0}
	b := diff.ByteNode{1}
	output, err := diff.Unified(a, b, 3)
	assert.Equal(t, "Binary files differ\n", output)
	assert.Nil(t, err)
	output, err = diff.Unified(a, a, 3)
	assert.Equal(t, "", output)
	assert.Nil(t, err)
}

func TestUnifiedCorrectnessAgainstGNUDiff(t *testing.T) {
	for i := 0; ; i++ {
		leftInputPath := fmt.Sprintf("testdata/%02d-left.in", i)
		// We keep looking for test data files. We assume that they are
		// numbered, in order. When the next input does not exist, the test
		// suite is done.
		_, err := os.Stat(leftInputPath)
		if os.IsNotExist(err) {
			break
		}
		require.Nil(t, err)
		leftInput, err := ioutil.ReadFile(fmt.Sprintf("testdata/%02d-left.in", i))
		require.Nil(t, err)
		rightInput, err := ioutil.ReadFile(fmt.Sprintf("testdata/%02d-right.in", i))
		require.Nil(t, err)
		for _, contextLines := range []int{1, 2, 3, 5, 8, 11} {
			diffOutputFile := fmt.Sprintf("testdata/%02d-diff-%02d.out", i, contextLines)
			diffOutput, err := ioutil.ReadFile(diffOutputFile)
			require.Nil(t, err)
			t.Run(diffOutputFile, func(t *testing.T) {
				left := diff.StringNode(leftInput)
				right := diff.StringNode(rightInput)
				got, err := diff.Unified(left, right, contextLines)
				want := string(diffOutput)
				assert.Equal(t, want, got)
				assert.Nil(t, err)
			})
		}
	}
}
