package main_test

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/clnt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	fsAddr string
)

func init() {
	flag.StringVar(&fsAddr, "fsaddr", "", "a reference file-system to test against")
}

// These tests are meant to explore how DMEXCL work in practice.
// They are meant to be pointed to an existing 9P server, e.g., ufs from go9p.
// Then they'll guide my implementation for musclefs.
//
// Sadly, with ufs I seem to be able to not only walk, but also open
// a DMEXCL file twice. Also, Tcreate with DMEXCL followed by Tstat
// shows DMEXCL is silently ignored by ufs.
//
// I switched to a fs exported from a 9front VM. Note to self of how to do this:
// In the VM, export the file system with "aux/listen1 -t 'tcp!*!564' exportfs -r /root'".
// Launch the QEMU VM with "-net user,hostfwd=tcp::1564-:564 -net nic" to be able to access the exported fs on localhost:1564.
// To see the 9P dialog, use "pine -l localhost:2564 -r localhost:1564" and run this test with "-fsaddr localhost:2564".
//
// DMEXCL behaves in the obvious way in Tcreate and Topen.
// But I was surprised to learn that Twstat always successfully sets DMEXCL,
// even if the file is already opened more than once.
// (This in CWFS. I haven't checked other file systems.)
func TestDMEXCL(t *testing.T) {
	if fsAddr == "" {
		t.Skip()
	}
	rand.Seed(time.Now().UnixNano())
	user := p.OsUsers.Uid2User(os.Getuid())
	c, err := clnt.Mount("tcp", fsAddr, "", 8192, user)
	require.NoError(t, err)
	defer c.Unmount()
	t.Run("it is okay to walk twice to a DMEXCL file, but it is not possible to open it twice", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)
		name := randomName()
		root, err := c.Attach(nil, user, "")
		r.NoError(err)
		defer func() {
			a.NoError(c.Clunk(root))
		}()

		// f: create file with DMEXCL flag.
		f := c.FidAlloc()
		qids, err := c.Walk(root, f, []string{"tmp"})
		r.NoError(err)
		r.Len(qids, 1)
		err = c.Create(f, name, 0777|p.DMEXCL, p.OWRITE, "")
		r.NoError(err)
		defer func() {
			a.NoError(c.Remove(f))
		}()

		// g: walk again to same file, try to open it (fail).
		g := c.FidAlloc()
		qids, err = c.Walk(root, g, []string{"tmp", name})
		if a.NoError(err) && a.Len(qids, 2) {
			a.Error(c.Open(g, p.OREAD))
		}
		a.NoError(c.Clunk(g))

		// h: walk again to same file, check its DMEXCL flag is set.
		h := c.FidAlloc()
		qids, err = c.Walk(root, h, []string{"tmp", name})
		if a.NoError(err) && a.Len(qids, 2) {
			dir, err := c.Stat(h)
			a.NoError(err)
			a.Equal(uint32(p.DMEXCL), dir.Mode&p.DMEXCL)
		}
		a.NoError(c.Clunk(h))
	})
	t.Run("it is okay to set the DMEXCL flag when the file is not opened", func(t *testing.T) {
		must := require.New(t)
		should := assert.New(t)

		name := randomName()
		root, err := c.Attach(nil, user, "")
		must.NoError(err)
		defer func() {
			should.NoError(c.Clunk(root))
		}()

		// f1: create and close file.
		f1 := c.FidAlloc()
		qids, err := c.Walk(root, f1, []string{"tmp"})
		must.NoError(err)
		must.Len(qids, 1)
		err = c.Create(f1, name, 0777, p.OWRITE, "")
		must.NoError(err)
		should.NoError(c.Clunk(f1))

		// f2: walk to same file, set it DMEXCL.
		f2 := c.FidAlloc()
		qids, err = c.Walk(root, f2, []string{"tmp", name})
		if should.NoError(err) && should.Len(qids, 2) {
			dir := p.NewWstatDir()
			dir.Mode = 0777 | p.DMEXCL
			should.NoError(c.Wstat(f2, dir))
		}
		should.NoError(c.Clunk(f2))

		// f3: walk again to same file, check DMEXCL is set.
		f3 := c.FidAlloc()
		qids, err = c.Walk(root, f3, []string{"tmp", name})
		if should.NoError(err) && should.Len(qids, 2) {
			dir, err := c.Stat(f3)
			should.NoError(err)
			should.Equal(uint32(p.DMEXCL), dir.Mode&p.DMEXCL)
		}
		should.NoError(c.Remove(f3))
	})
	t.Run("it is okay to set the DMEXCL flag when the file is opened once", func(t *testing.T) {
		must := require.New(t)
		should := assert.New(t)

		name := randomName()
		root, err := c.Attach(nil, user, "")
		must.NoError(err)
		defer func() {
			should.NoError(c.Clunk(root))
		}()

		// f1: create but do not close file.
		f1 := c.FidAlloc()
		qids, err := c.Walk(root, f1, []string{"tmp"})
		must.NoError(err)
		must.Len(qids, 1)
		err = c.Create(f1, name, 0777, p.OWRITE, "")
		must.NoError(err)
		defer func() {
			should.NoError(c.Clunk(f1))
		}()

		// f2: walk to same file, set it DMEXCL.
		f2 := c.FidAlloc()
		qids, err = c.Walk(root, f2, []string{"tmp", name})
		if should.NoError(err) && should.Len(qids, 2) {
			dir := p.NewWstatDir()
			dir.Mode = 0777 | p.DMEXCL
			should.NoError(c.Wstat(f2, dir))
		}
		should.NoError(c.Clunk(f2))

		// f3: walk again to same file, check DMEXCL is set.
		f3 := c.FidAlloc()
		qids, err = c.Walk(root, f3, []string{"tmp", name})
		if should.NoError(err) && should.Len(qids, 2) {
			dir, err := c.Stat(f3)
			should.NoError(err)
			should.Equal(uint32(p.DMEXCL), dir.Mode&p.DMEXCL)
		}
		should.NoError(c.Remove(f3))
	})
	t.Run("it is still okay to set the DMEXCL flag when the file is opened twice", func(t *testing.T) {
		must := require.New(t)
		should := assert.New(t)

		name := randomName()
		root, err := c.Attach(nil, user, "")
		must.NoError(err)
		defer func() {
			should.NoError(c.Clunk(root))
		}()

		// f1: create but do not close file.
		f1 := c.FidAlloc()
		qids, err := c.Walk(root, f1, []string{"tmp"})
		must.NoError(err)
		must.Len(qids, 1)
		err = c.Create(f1, name, 0777, p.OWRITE, "")
		must.NoError(err)
		defer func(f *clnt.Fid) {
			should.NoError(c.Clunk(f))
		}(f1)

		// f1 again: open file again.
		f1 = c.FidAlloc()
		qids, err = c.Walk(root, f1, []string{"tmp", name})
		must.NoError(err)
		must.Len(qids, 2)
		err = c.Open(f1, p.OREAD)
		must.NoError(err)
		defer func(f *clnt.Fid) {
			should.NoError(c.Clunk(f))
		}(f1)

		// f2: walk to same file, set it DMEXCL.
		f2 := c.FidAlloc()
		qids, err = c.Walk(root, f2, []string{"tmp", name})
		if should.NoError(err) && should.Len(qids, 2) {
			dir := p.NewWstatDir()
			dir.Mode = 0777 | p.DMEXCL
			should.NoError(c.Wstat(f2, dir))
		}
		should.NoError(c.Clunk(f2))

		// f3: walk again to same file, check DMEXCL is set.
		f3 := c.FidAlloc()
		qids, err = c.Walk(root, f3, []string{"tmp", name})
		if should.NoError(err) && should.Len(qids, 2) {
			dir, err := c.Stat(f3)
			should.NoError(err)
			should.Equal(uint32(p.DMEXCL), dir.Mode&p.DMEXCL)
		}
		should.NoError(c.Remove(f3))
	})
	t.Run("it is okay to open a file a second time after the DMEXCL flag has been removed", func(t *testing.T) {
		must := require.New(t)
		should := assert.New(t)

		name := randomName()
		root, err := c.Attach(nil, user, "")
		must.NoError(err)
		defer func() {
			should.NoError(c.Clunk(root))
		}()

		// f1: create but do not close a DMEXCL file.
		f1 := c.FidAlloc()
		qids, err := c.Walk(root, f1, []string{"tmp"})
		must.NoError(err)
		must.Len(qids, 1)
		err = c.Create(f1, name, 0777|p.DMEXCL, p.OWRITE, "")
		must.NoError(err)
		defer func() {
			should.NoError(c.Clunk(f1))
		}()

		// f2: walk to same file, unset its DMEXCL bit.
		f2 := c.FidAlloc()
		qids, err = c.Walk(root, f2, []string{"tmp", name})
		if should.NoError(err) && should.Len(qids, 2) {
			dir := p.NewWstatDir()
			dir.Mode = 0777
			should.NoError(c.Wstat(f2, dir))
		}
		should.NoError(c.Clunk(f2))

		// f3: walk again to the same file, open it a second time; should work.
		f3 := c.FidAlloc()
		qids, err = c.Walk(root, f3, []string{"tmp", name})
		must.NoError(err)
		must.Len(qids, 2)
		err = c.Open(f3, p.OREAD)
		must.NoError(err)
		defer func() {
			should.NoError(c.Clunk(f3))
		}()

		// f4: walk again to same file, check DMEXCL is NOT set.
		f4 := c.FidAlloc()
		qids, err = c.Walk(root, f4, []string{"tmp", name})
		if should.NoError(err) && should.Len(qids, 2) {
			dir, err := c.Stat(f4)
			should.NoError(err)
			should.Equal(uint32(0), dir.Mode&p.DMEXCL)
		}
		should.NoError(c.Remove(f4))
	})
}

func randomName() string {
	return fmt.Sprintf("testfile.%d", rand.Uint64())
}
