package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	mathrand "math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"9fans.net/go/plan9/client"
	"github.com/pkg/errors"
)

var (
	// DefaultBaseDirectoryPath is where all muscle commands store configuration and data.
	// It defaults to $MUSCLE_BASE if it is set, otherwise it defaults to $HOME/lib/muscle.
	// Commands override this via the -base flag.
	DefaultBaseDirectoryPath string

	defaultBlockSize uint32 = 1024 * 1024
)

func init() {
	if base := os.Getenv("MUSCLE_BASE"); base != "" {
		DefaultBaseDirectoryPath = base
	} else {
		// The portable way of doing this is by using the os/user package,
		// but I only intend to run this on Linux or NetBSD.
		DefaultBaseDirectoryPath = os.ExpandEnv("$HOME/lib/muscle")
	}
}

type C struct {
	// BlockSize is the capacity for blocks of new nodes. Existing nodes
	// have their block size encoded within them (which is the value of this
	// variable at the time the nodes were created).
	BlockSize uint32 `json:"block-size,omitempty"`

	// Listen on localhost or a local-only network, e.g., one for
	// containers hosted on your computer.  There is no
	// authentication nor TLS so the file server must not be exposed on a
	// public address.
	ListenNet           string `json:"listen-net,omitempty"`
	ListenAddr          string `json:"listen-addr,omitempty"`
	SnapshotsListenNet  string `json:"snapshots-listen-net,omitempty"`
	SnapshotsListenAddr string `json:"snapshots-listen-addr,omitempty"`

	MuscleFSMount    string `json:"musclefs-mount,omitempty"`
	SnapshotsFSMount string `json:"snapshotsfs-mount,omitempty"`

	// 64 hex digits - do not lose this or you lose access to all
	// data.
	EncryptionKey string `json:"encryption-key,omitempty"`

	// Path to cache. Defaults to $HOME/lib/muscle/cache.
	CacheDirectory string `json:"cache-directory,omitempty"`

	// Permanent storage type - can be "s3" or "null" at present.
	Storage string `json:"storage,omitempty"`

	// These only make sense if the storage type is "s3".  The AWS
	// profile is used for credentials.
	S3Profile string `json:"s3-profile,omitempty"`
	S3Region  string `json:"s3-region,omitempty"`
	S3Bucket  string `json:"s3-bucket,omitempty"`

	// These only make sense if the storage type is "disk".
	// If the path is relative, it will be assumed relative to the base dir.
	DiskStoreDir string `json:"disk-store-dir,omitempty"`

	// Directory holding muscle config file and other files.
	// Other directories and files are derived from this.
	base string

	// Computed from the corresponding string at load time.
	encryptionKey []byte
}

// Load loads the configuration from the file called "config" in the provided base
// directory.
func Load(base string) (*C, error) {
	filename := path.Join(base, "config")
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	fi, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	if fi.Mode()&0077 != 0 {
		return nil, fmt.Errorf("reading config: %s mode is %#o, want %#o", filename, fi.Mode()&0777, fi.Mode()&0700)
	}
	c, err := load(f)
	if err == nil {
		c.base = base
	}
	c.encryptionKey, err = hex.DecodeString(c.EncryptionKey)
	if err != nil {
		err = fmt.Errorf("%q: %w", c.EncryptionKey, err)
	}
	if c.DiskStoreDir != "" && !filepath.IsAbs(c.DiskStoreDir) {
		c.DiskStoreDir = filepath.Clean(filepath.Join(c.base, c.DiskStoreDir))
	}
	if c.BlockSize == 0 {
		c.BlockSize = defaultBlockSize
	}
	if c.ListenNet == "" && c.ListenAddr == "" {
		c.ListenNet = "unix"
	}
	if c.SnapshotsListenNet == "" && c.SnapshotsListenAddr == "" {
		c.SnapshotsListenNet = "unix"
	}
	if c.ListenNet == "unix" && c.ListenAddr == "" {
		c.ListenAddr = fmt.Sprintf("%s/muscle", client.Namespace())
	}
	if c.SnapshotsListenNet == "unix" && c.SnapshotsListenAddr == "" {
		c.SnapshotsListenAddr = fmt.Sprintf("%s/snapshots", client.Namespace())
	}
	return c, err
}

func load(r io.Reader) (c *C, err error) {
	err = json.NewDecoder(r).Decode(&c)
	return
}

func (c *C) CacheDirectoryPath() string {
	if c.CacheDirectory != "" {
		return c.CacheDirectory
	}
	return path.Join(c.base, "cache")
}

// An instance of *storage.Paired will log keys to propagate from the
// fast store to the slow store to this append-only log.  This will
// ensure all data is eventually copied to the slow store, even if
// musclefs restarts.
func (c *C) PropagationLogFilePath() string {
	return path.Join(c.base, "propagation.log")
}

func (c *C) StagingDirectoryPath() string {
	return path.Join(c.base, "staging")
}

func (c *C) EncryptionKeyBytes() []byte {
	return c.encryptionKey
}

// See https://www.kernel.org/doc/Documentation/filesystems/9p.txt.
func linuxMountCommand(net string, addr string, mountpoint string) (string, error) {
	uid, gid := os.Getuid(), os.Getgid()
	switch net {
	case "unix":
		return fmt.Sprintf("sudo mount -t 9p %v %v -o trans=unix,dfltuid=%d,dfltgid=%d", addr, mountpoint, uid, gid), nil
	case "tcp":
		if parts := strings.Split(addr, ":"); len(parts) != 2 {
			return "", errors.Errorf("mailformed host-port pair: %q", addr)
		} else {
			return fmt.Sprintf("sudo mount -t 9p %v %v -o trans=tcp,port=%v,dfltuid=%d,dfltgid=%d", parts[0], mountpoint, parts[1], uid, gid), nil
		}
	default:
		return "", errors.Errorf("unhandled network type: %v", net)
	}
}

// See mount_9p(8).
func netbsdMountCommand(net string, addr string, mountpoint string) (string, error) {
	if net != "tcp" {
		return "", errors.Errorf("unsupported network: %q", net)
	}
	if parts := strings.Split(addr, ":"); len(parts) != 2 {
		return "", errors.Errorf("mailformed host-port pair: %q", addr)
	} else {
		return fmt.Sprintf("sudo mount_9p -p %v %v %v", parts[1], parts[0], mountpoint), nil
	}
}

func (c *C) MountCommands() ([]string, error) {
	switch runtime.GOOS {
	case "linux":
		cmd1, err := linuxMountCommand(c.ListenNet, c.ListenAddr, c.MuscleFSMount)
		if err != nil {
			return nil, err
		}
		cmd2, err := linuxMountCommand(c.SnapshotsListenNet, c.SnapshotsListenAddr, c.SnapshotsFSMount)
		if err != nil {
			return nil, err
		}
		return []string{cmd1, cmd2}, nil
	case "netbsd":
		cmd1, err := netbsdMountCommand(c.ListenNet, c.ListenAddr, c.MuscleFSMount)
		if err != nil {
			return nil, err
		}
		cmd2, err := netbsdMountCommand(c.SnapshotsListenNet, c.SnapshotsListenAddr, c.SnapshotsFSMount)
		if err != nil {
			return nil, err
		}
		return []string{cmd1, cmd2}, nil
	default:
		return nil, fmt.Errorf("don't know now to mount on %v", runtime.GOOS)
	}
}

func (c *C) UmountCommands() ([]string, error) {
	switch runtime.GOOS {
	case "linux", "netbsd":
		return []string{
			fmt.Sprintf("sudo umount %s", c.MuscleFSMount),
			fmt.Sprintf("sudo umount %s", c.SnapshotsFSMount),
		}, nil
	default:
		return nil, fmt.Errorf("don't know now to umount on %v", runtime.GOOS)
	}
}

// Initialize generates an initial configuration at the given directory.
func Initialize(baseDir string) error {
	if err := os.MkdirAll(baseDir, 0700); err != nil {
		return fmt.Errorf("%q: could not mkdir: %w", baseDir, err)
	}
	path := filepath.Join(baseDir, "config")
	_, err := os.Stat(path)
	if err == nil {
		return fmt.Errorf("%q: already exists", path)
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("%q: could not determine if it exists: %w", path, err)
	}
	var c C
	c.BlockSize = defaultBlockSize
	mathrand.Seed(time.Now().UnixNano())
	port := 49152 + mathrand.Intn(65535-49152)
	c.ListenNet = "tcp"
	c.ListenAddr = fmt.Sprintf("127.0.0.1:%d", port)
	c.SnapshotsListenNet = "tcp"
	c.SnapshotsListenAddr = fmt.Sprintf("127.0.0.1:%d", port+1)
	c.MuscleFSMount = "/mnt/muscle"
	c.SnapshotsFSMount = "/mnt/snapshots"
	b := make([]byte, 32)
	n, err := rand.Read(b)
	if err != nil {
		return fmt.Errorf("could not read 32 random bytes: %w", err)
	}
	if n != 32 {
		return fmt.Errorf("could not read 32 random bytes, got only %d", n)
	}
	c.EncryptionKey = hex.EncodeToString(b)
	c.Storage = "disk"
	c.DiskStoreDir = "permanent"
	b, err = json.MarshalIndent(c, "", "	")
	if err != nil {
		return fmt.Errorf("could not marshal generated configuration: %w", err)
	}
	err = ioutil.WriteFile(path, b, 0600)
	if err != nil {
		return fmt.Errorf("could not write generated configuration to %q: %w", path, err)
	}
	return nil
}
