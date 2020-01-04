package main

import (
	"testing"
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
