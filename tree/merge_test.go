package tree

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalOverrides(t *testing.T) {
	tdir, err := ioutil.TempDir("", "muscle-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("no overrides", func(t *testing.T) {
		f, err := ioutil.TempDir(tdir, "")
		if err != nil {
			t.Fatal(err)
		}
		rf, cleanup := MustKeepLocalFn(f)
		defer cleanup()
		assert.False(t, rf("revision", "path"))
	})
	t.Run("one override", func(t *testing.T) {
		f, err := ioutil.TempDir(tdir, "")
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, KeepLocalFor(f, "another revision", "another path"))
		rf, cleanup := MustKeepLocalFn(f)
		defer cleanup()
		assert.False(t, rf("revision", "path"))
		assert.False(t, rf("another revision", "path"))
		assert.False(t, rf("revision", "another path"))
		assert.True(t, rf("another revision", "another path"))
	})
	t.Run("overrides for distinct revisions", func(t *testing.T) {
		f, err := ioutil.TempDir(tdir, "")
		if err != nil {
			t.Fatal(err)
		}
		assert.Nil(t, KeepLocalFor(f, "revision", "path"))
		assert.Nil(t, KeepLocalFor(f, "another revision", "another path"))
		rf, cleanup := MustKeepLocalFn(f)
		defer cleanup()
		assert.True(t, rf("revision", "path"))
		assert.True(t, rf("another revision", "another path"))
		assert.False(t, rf("revision", "other path"))
		assert.False(t, rf("another revision", "path"))
		assert.False(t, rf("third revision", "third path"))
	})
}
