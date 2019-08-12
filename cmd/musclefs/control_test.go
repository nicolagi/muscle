package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestControlRead(t *testing.T) {
	const initialValue = 42
	testCases := []struct {
		name     string
		size     int // Control file contents
		count    int // Requested bytes
		offset   int // Requested offset
		expected int // Expected bytes read
	}{
		{name: "read nothing from beginning of empty file"},
		{name: "read nothing at non-zero offset of empty file", offset: 1},
		{name: "read a byte at beginning of empty file", count: 1},
		{name: "read a byte at non-zero offset of empty file", count: 1, offset: 1},
		{name: "read a byte right after end of file", size: 16, count: 1, offset: 16},
		{name: "read a byte after end of file", size: 16, count: 1, offset: 17},
		{name: "read last byte of file", size: 16, count: 1, offset: 15, expected: 1},
		{name: "partial read of 2 bytes", size: 16, count: 2, offset: 15, expected: 1},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			contents := make([]byte, tc.size)
			for i := range contents {
				contents[i] = initialValue
			}
			file := new(ctl)
			file.contents = contents
			dst := make([]byte, tc.count)
			n := file.read(dst, tc.offset)
			if n != tc.expected {
				t.Fatalf("got %d, want %d read bytes", n, tc.expected)
			}
			for i := 0; i < tc.expected; i++ {
				if dst[i] != initialValue {
					t.Fatalf("got %d non-zero bytes read, want %d", i, tc.expected)
				}
			}
			for i := tc.expected; i < len(dst); i++ {
				if dst[i] != 0 {
					t.Fatalf("got %d excess non-zero bytes, want 0", i)
				}
			}
		})
	}
}

func TestControlAppend(t *testing.T) {
	t.Run("append-nothing", func(t *testing.T) {
		c := new(ctl)
		assert.Len(t, c.append(nil), 0)
	})
	t.Run("append-incomplete-line", func(t *testing.T) {
		c := new(ctl)
		assert.Len(t, c.append([]byte("this line is")), 0)
	})
	t.Run("append-complete-line", func(t *testing.T) {
		c := new(ctl)
		lines := c.append([]byte("this line is complete\n"))
		assert.Len(t, lines, 1)
		assert.Equal(t, "this line is complete", lines[0])
	})
	t.Run("append-many-complete-lines", func(t *testing.T) {
		c := new(ctl)
		lines := c.append([]byte("this line is complete\nand another\n"))
		assert.Len(t, lines, 2)
		assert.Equal(t, "this line is complete", lines[0])
		assert.Equal(t, "and another", lines[1])
	})
	t.Run("append-complete-line-twice", func(t *testing.T) {
		c := new(ctl)
		lines := c.append([]byte("this line is complete\n"))
		assert.Len(t, lines, 1)
		assert.Equal(t, "this line is complete", lines[0])
		lines = c.append([]byte("and another\n"))
		assert.Len(t, lines, 1)
		assert.Equal(t, "and another", lines[0])
	})
	t.Run("append-incomplete-then-complete", func(t *testing.T) {
		c := new(ctl)
		lines := c.append([]byte("this line is "))
		assert.Len(t, lines, 0)
		lines = c.append([]byte("complete\n"))
		assert.Len(t, lines, 1)
		assert.Equal(t, "this line is complete", lines[0])
	})
	t.Run("append-many-times-then-read", func(t *testing.T) {
		c := new(ctl)
		assert.Len(t, c.append([]byte("this line ")), 0)
		assert.Equal(t, []string{"this line is complete"}, c.append([]byte("is complete\nbut this ")))
		assert.Equal(t, []string{"but this one is not."}, c.append([]byte("one is not.\n")))
		assert.Equal(t, "this line is complete\nbut this one is not.\n", string(c.contents))
	})
}
