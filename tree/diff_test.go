package tree

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestExtractPrintable(t *testing.T) {
	readFile := func(p string) string {
		b, err := ioutil.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	// TODO use fixture file
	input := readFile("/etc/fstab")
	output, wasPrintable := extractPrintable(input)
	assert.True(t, wasPrintable)
	assert.Equal(t, input, output)

	// TODO use fixture file
	input = readFile("/bin/bash")
	output, wasPrintable = extractPrintable(input)
	assert.False(t, wasPrintable)
	// TODO Need more fine-grained tests on the output. Then can improve the function.
	assert.NotEqual(t, input, output)
}
