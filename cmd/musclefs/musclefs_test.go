package main_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/go9p/p/clnt"
	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/* TODO To read to get test cases from:
size[4] version(5) [2] msize[4] version[s]
size[4] auth(5) [2] afid[4] uname[s] aname[s]
size[4] error(5) [2] ename[s]
size[4] flush(5) [2] oldtag[2]
size[4] attach(5) [2] fid[4] afid[4] uname[s] aname[s]
size[4] walk(5) [2] fid[4] newfid[4] nwname[2] nwname*(wname[s])
size[4] open(5) [2] fid[4] mode[1]
size[4] create(5) [2] fid[4] name[s] perm[4] mode[1]
size[4] read(5) [2] fid[4] offset[8] count[4]
size[4] write(5) [2] fid[4] offset[8] count[4] data[count]
size[4] clunk(5) [2] fid[4]
size[4] remove(5) [2] fid[4]
size[4] stat(5) [2] fid[4]
size[4] wstat(5) [2] fid[4] stat[n]
*/
func TestComformsToManualPages(t *testing.T) {
	// From intro(5):
	// The thirteen-byte qid fields hold a one-byte type, specifying whether the file is a directory, append-only
	// file, etc., and two unsigned integers: first the four-byte qid version, then the eight-byte qid path.  The
	// path is an integer unique among all files in the hierarchy.  If a file is deleted and recreated with the same
	// name in the same directory, the old and new path components of the qids should be different.  The version is
	// a version number for a file; typically, it is incremented every time the file is modified.
	t.Run("check QID type for newly created directory", func(t *testing.T) {
	})
	t.Run("check QID type for newly created file", func(t *testing.T) {
	})
	// The above should check QT* values against other file types, but muscle does not support them, so
	// we'll check that indeed such files can't be created.
	t.Run("check creating DMAPPEND, DMEXCL, DMAUTH, DMTMP don't work", func(t *testing.T) {
	})
	// The following should check against all mutations of a file or directory, not just write to a file.
	// TODO: ensure the QID version increase for all operations via unit tests instead.
	t.Run("check QID version increases after write", func(t *testing.T) {
	})
	t.Run("check re-created file has new QID path", func(t *testing.T) {
	})
	// From intro(5):
	// All directories must support walks to the directory ..  (dot-dot) meaning parent directory, although by
	// convention directories contain no explicit entry for ..  or . (dot).  The parent of the root directory of a
	// server's tree is itself.
	t.Run("walk to .. from root gives root", func(t *testing.T) {
		client, _, tearDown := setUp(t)
		defer tearDown(t)
		newfid := client.FidAlloc()
		qids, err := client.Walk(client.Root, newfid, []string{".."})
		require.Nil(t, err)
		require.Len(t, qids, 1)

		dir, err := client.Stat(newfid)
		require.Nil(t, err)
		assert.Equal(t, "root", dir.Name)
	})
	t.Run("walk to .. from dir gives parent", func(t *testing.T) {
		client, _, tearDown := setUp(t)
		defer tearDown(t)
		must := &mustHelpers{t: t, c: client}

		fid := must.walk()
		must.create(fid, "dir", 0700|p.DMDIR, 0)
		must.clunk(fid)

		fid = must.walk("dir")
		newfid := client.FidAlloc()
		qids, err := client.Walk(fid, newfid, []string{".."})
		require.Nil(t, err)
		require.Len(t, qids, 1)

		dir, err := client.Stat(newfid)
		require.Nil(t, err)
		assert.Equal(t, "root", dir.Name)
	})
	t.Run("walk to . from dir gives dir", func(t *testing.T) {
		// TODO
	})
}

func Test(t *testing.T) {
	client, factory, tearDown := setUp(t)
	defer tearDown(t)
	t.Run("the root is a directory", func(t *testing.T) {
		must := &mustHelpers{t: t, c: client}
		fid := must.walk()
		dir := must.stat(fid)
		if dir.Mode&p.DMDIR == 0 {
			t.Error("the root is not a directory (mode)")
		}
		if dir.Qid.Type != p.QTDIR {
			t.Error("the root is not a directory (QID type)")
		}
		must.clunk(fid)
	})
	t.Run("the control file shouldn't be removable", func(t *testing.T) {
		must := &mustHelpers{t: t, c: client}
		fid := must.walk("ctl")
		if err := client.Remove(fid); err == nil {
			t.Fatal("was able to remove control file")
		}
		// Besides checking that removing returned an error,
		// let's check that we can still walk to the control file.
		must.clunk(must.walk("ctl"))
	})
	t.Run("try to change dir length and fail", func(t *testing.T) {
		must := &mustHelpers{t: t, c: client}

		fid := must.walk()
		must.create(fid, "dir", 0700|p.DMDIR, 0)
		must.clunk(fid)

		fid = must.walk("dir")
		dir := p.NewWstatDir()
		dir.Length = 1
		assert.NotNil(t, client.Wstat(fid, dir))
		must.clunk(fid)
	})
	t.Run("move directory via control file", func(t *testing.T) {
		must := &mustHelpers{t: t, c: client}

		// Setup
		fid := must.walk()
		must.create(fid, "old-name", 0700|p.DMDIR, 0)
		must.clunk(fid)
		fid = must.walk()
		must.create(fid, "new-parent", 0700|p.DMDIR, 0)
		must.clunk(fid)
		fid = must.walk("old-name")
		must.create(fid, "inner-node", 0600, p.OWRITE)
		must.write(fid, []byte("hello world"))
		must.clunk(fid)

		// Write rename command
		fid = must.walk("ctl")
		must.open(fid, p.OWRITE)
		cmd := []byte("rename old-name new-parent/new-name\n")
		must.write(fid, cmd)
		must.clunk(fid)

		// Verify
		must.notExist("old-name")
		got := must.readFile("new-parent", "new-name", "inner-node")
		if got != "hello world" {
			t.Fatalf("got %q, want %q", got, "hello world")
		}
	})
	// There used to be a bug where if you did a graft with a command like "graft revision:music music" and "music/song" is
	// currently open, any changes to the "music/song" are silently lost. This is how the scenario used to play out:
	// 1. root -> music -> song is the initial in-memory state
	// 2. music/song is opened
	// 3. graft revision:music music is run via the control file, hence root' -> music' is the new in-memory state
	// 4. the open song file is modified and then released, marking nodes song, music, and root ad dirty
	// 5. at some point the tree is flushed, but this walks root' -> music' rather than the dirty tree root -> music -> song.
	// The nodes root, music, and song, could have been even GC'd at this point.
	// The proper behavior is to only allow graft commands when there is no open file under the grafted path and
	// give a proper error otherwise.
	t.Run("graft /music while /music/song is open", func(t *testing.T) {
		// Outside of the file server, we must create a "donor" tree that will "donate"
		// a path that will be grafted onto the running tree.
		donor, err := factory.NewTree(storage.Null, false)
		require.Nil(t, err)
		_, donorRoot := donor.Root()
		donorMusic, err := donor.Add(donorRoot, "music", 0666)
		require.Nil(t, err)
		require.Nil(t, donor.Release(donorMusic))
		require.Nil(t, donor.Flush())
		donorRevision, _ := donor.Root()

		must := &mustHelpers{t: t, c: client}

		// Create root/music/song
		fid := must.walk()
		must.create(fid, "music", 0700|p.DMDIR, 0)
		must.clunk(fid)
		fid = must.walk("music")
		must.create(fid, "song", 0600, p.OWRITE)
		must.clunk(fid)

		// Walk to root/music/song and open it and write something to it so the underlying node is dirty.
		// But do not release it nor flush the tree.
		fid = must.walk("music", "song")
		must.open(fid, p.OWRITE)
		must.write(fid, []byte("all along the watchtower"))

		// Graft root/music from the donor tree.
		ctl := must.walk("ctl")
		must.open(ctl, p.OWRITE)
		cmd := []byte("flush\n")
		must.write(ctl, cmd)
		cmd = []byte(fmt.Sprintf("graft %s/music music\n", donorRevision.Hex()))
		_, err = must.c.Write(ctl, cmd, 0)
		assert.Nil(t, err) // TODO converse
		must.clunk(ctl)

		// Now release the song file and then flush the tree.
		must.clunk(fid)
		ctl = must.walk("ctl")
		must.open(ctl, p.OWRITE)
		cmd = []byte("flush\n")
		must.write(ctl, cmd)
		must.clunk(ctl)

		// Finally verify that the song was NOT lost.
		//must.walk("music", "song") // TODO enable
	})
	t.Run("creating or removing a file updates the directory timestamp", func(t *testing.T) {
		must := &mustHelpers{t: t, c: client}

		// Create test dir.
		fid := must.walk()
		must.create(fid, "dirmtime", 0700|p.DMDIR, 0)
		must.clunk(fid)

		// Set its mtime to 0 and check it was set to 0.
		fid = must.walk("dirmtime")
		dir := p.NewWstatDir()
		dir.Mtime = 0
		must.wstat(fid, dir)
		must.clunk(fid)
		fid = must.walk("dirmtime")
		dir = must.stat(fid)
		require.Equal(t, uint32(0), dir.Mtime)
		must.clunk(fid)

		// Create file within directory.
		fid = must.walk("dirmtime")
		must.create(fid, "filemtime", 0600, 0)
		must.clunk(fid)

		// Check mtime of file and dir match and are non-zero.
		fid = must.walk("dirmtime")
		ddir := must.stat(fid)
		must.clunk(fid)
		fid = must.walk("dirmtime", "filemtime")
		fdir := must.stat(fid)
		must.clunk(fid)
		require.NotEqual(t, 0, fdir.Mtime)
		require.Equal(t, fdir.Mtime, ddir.Mtime)

		// Reset the dir's mtime to 0 and check it was set to 0.
		fid = must.walk("dirmtime")
		dir = p.NewWstatDir()
		dir.Mtime = 0
		must.wstat(fid, dir)
		must.clunk(fid)
		fid = must.walk("dirmtime")
		dir = must.stat(fid)
		require.Equal(t, uint32(0), dir.Mtime)
		must.clunk(fid)

		// Remove the file.
		fid = must.walk("dirmtime", "filemtime")
		must.remove(fid)

		// Check mtime of dir is non-zero after the removal.
		fid = must.walk("dirmtime")
		ddir = must.stat(fid)
		must.clunk(fid)
		require.NotEqual(t, 0, ddir.Mtime)
	})
}

// The returned client is associated with an ephemeral musclefs process.
// The tree factory is configured to write to the same storage as the musclefs process,
// therefore it can be used to build fixture data that the musclefs process can use, e.g.,
// for the graft command.
func setUp(t *testing.T) (client *clnt.Clnt, factory *tree.Factory, tearDown func(*testing.T)) {
	// dir will store what is usually in $HOME/lib/musclefs.
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("The temporary directory is at %q", dir)
	f, err := os.OpenFile(path.Join(dir, "config"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// The encryption key is shared between the ephemeral musclefs configuration
	// and the tree store. Otherwise any fixture created via the factory will be unreadable
	// by the ephemeral musclefs.
	sharedKey := make([]byte, 16)
	rand.Seed(time.Now().Unix())
	rand.Read(sharedKey)
	port := 5000 + rand.Intn(5000)
	err = json.NewEncoder(f).Encode(config.C{
		ListenIP:          "127.0.0.1",
		ListenPort:        port,
		Instance:          "test",
		ReadOnlyInstances: []string{"test"},
		Storage:           "null",
		EncryptionKey:     fmt.Sprintf("%x", sharedKey),
	})
	if err != nil {
		t.Fatal(err)
	}

	testAddress := fmt.Sprintf("127.0.0.1:%d", port)

	ready := make(chan struct{})

	// Start process asynchronously, creating a process group id, so we can later
	// kill this process and all its children.
	command := exec.Command("go", "run", "-race", ".", "-config", dir)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Attach the ephemeral musclefs stdin and stdout to a file for debugging.
	serverLog, err := os.Create(path.Join(dir, "combined-output"))
	require.Nil(t, err)
	command.Stdout = serverLog
	command.Stderr = serverLog
	require.Nil(t, command.Start())

	// Probe test port.
	go func() {
		for {
			if conn, err := net.Dial("tcp", testAddress); err == nil {
				_ = conn.Close()
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		close(ready)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Initialization timed out")
	case <-ready:
	}

	user := p.OsUsers.Uid2User(os.Geteuid())
	client, err = clnt.Mount("tcp", testAddress, user.Name(), 8192, user)
	require.Nil(t, err)

	rootFile := path.Join(dir, "other-root") // Avoid conflicts with the ephemeral musclefs root.
	cacheDir := path.Join(dir, "cache")      // Share the storage.
	diskStore := storage.NewDiskStore(cacheDir)
	// Use the same backing store for the staging area and the cache.
	// This way we don't have to (in fact, we must not) snapshot the trees created via the factory
	// to populate the cache from the staging area, which makes the fixtures easier to set up.
	// Of course this means that the cache directory will contain extraneous intermediate data,
	// but it's fine for tests.
	blobStore := storage.NewMartino(diskStore, diskStore)
	store, err := tree.NewStore(blobStore, nil, rootFile, "remote.root.other", sharedKey)
	require.Nil(t, err)
	factory = tree.NewFactory(store)

	// TODO: Should do the cleean up only if the test is successful, leave other wise
	// process and temporary files around for debugging.
	return client, factory, func(t *testing.T) {
		// Can't use command.Process.Kill() because that would kill go run, not its child.
		//ng        9368  9353  0 18:52 ?        00:00:00 go run -race . -config ./config.test
		//ng        9477  9368  0 18:52 ?        00:00:00 /tmp/go-build420765022/b001/exe/musclefs -config ./config.test
		if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
			t.Errorf("Could not kill test server: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Could not remove test server dir %q: %v", dir, err)
		}
		_ = serverLog.Close()
	}
}

type mustHelpers struct {
	t *testing.T
	c *clnt.Clnt
}

func (m *mustHelpers) walk(names ...string) *clnt.Fid {
	fid := m.c.FidAlloc()
	qids, err := m.c.Walk(m.c.Root, fid, names)
	if err != nil {
		m.t.Fatal(err)
	}
	if got, want := len(qids), len(names); got != want {
		m.t.Fatalf("got %d qids, want %d", got, want)
	}
	return fid
}

func (m *mustHelpers) wstat(fid *clnt.Fid, dir *p.Dir) {
	err := m.c.Wstat(fid, dir)
	if err != nil {
		m.t.Fatal(err)
	}
}

func (m *mustHelpers) stat(fid *clnt.Fid) *p.Dir {
	dir, err := m.c.Stat(fid)
	if err != nil {
		m.t.Fatal(err)
	}
	return dir
}

func (m *mustHelpers) open(fid *clnt.Fid, mode uint8) {
	err := m.c.Open(fid, mode)
	if err != nil {
		m.t.Fatalf("could not open: %v", err)
	}
}

func (m *mustHelpers) create(fid *clnt.Fid, name string, perm uint32, mode uint8) {
	err := m.c.Create(fid, name, perm, mode, "")
	if err != nil {
		m.t.Fatalf("could not open: %v", err)
	}
}

func (m *mustHelpers) remove(fid *clnt.Fid) {
	err := m.c.Remove(fid)
	if err != nil {
		m.t.Fatalf("could not open: %v", err)
	}
}

func (m *mustHelpers) read(fid *clnt.Fid, off uint64, count uint32) []byte {
	b, err := m.c.Read(fid, off, count)
	if err != nil {
		m.t.Fatalf("could not read: %v", err)
	}
	return b
}

func (m *mustHelpers) write(fid *clnt.Fid, contents []byte) {
	n, err := m.c.Write(fid, contents, 0)
	if err != nil {
		m.t.Fatalf(`could not write: %v`, err)
	}
	if n != len(contents) {
		m.t.Fatalf("got %d, want %d bytes written", n, len(contents))
	}
}

func (m *mustHelpers) clunk(fid *clnt.Fid) {
	err := m.c.Clunk(fid)
	if err != nil {
		m.t.Fatalf("could not clunk: %v", err)
	}
}

func (m *mustHelpers) notExist(name string) {
	fid := m.c.FidAlloc()
	_, err := m.c.Walk(m.c.Root, fid, []string{name})
	if err == nil {
		m.t.Fatalf("no error walking to %q", name)
	}
}

func (m *mustHelpers) readFile(names ...string) string {
	fid := m.walk(names...)
	m.open(fid, p.OREAD)
	b := m.read(fid, 0, 8192)
	m.clunk(fid)
	return string(b)
}
