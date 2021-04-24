package config

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	mathrand "math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

var (
	// DefaultBaseDirectoryPath is where all muscle commands store configuration and data.
	// It defaults to $MUSCLE_BASE if it is set, otherwise it defaults to $HOME/lib/muscle.
	// Commands override this via the -base flag.
	DefaultBaseDirectoryPath string

	// DO NOT CHANGE.
	//
	// I had to back out changes to make the block size configurable. That
	// knob turned out to be a problem: If a file system has blocks of a
	// variety of different sizes, contents can't be compared for equality
	// just by looking at the list of block hashes. I don't want musclefs
	// to read up large files to determine if they're equal or not, as that
	// would make the merge operation slow.
	//
	// The block size could still be configurable per file system, but that
	// configuration should be written _once_ and never changed (in
	// conventional file systems, in the superblocks). Since we've no
	// superblocks (for now?) I decided to remove the configuration knob
	// entirely.
	BlockSize uint32 = 1024 * 1024
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
	// Listen on localhost or a local-only network, e.g., one for
	// containers hosted on your computer.  There is no
	// authentication nor TLS so the file server must not be exposed on a
	// public address.
	ListenNet  string
	ListenAddr string

	MuscleFSMount string

	// 64 hex digits - do not lose this or you lose access to all
	// data.
	EncryptionKey string

	// Path to cache. Defaults to $HOME/lib/muscle/cache.
	CacheDirectory string

	// Permanent storage type - can be "s3" or "null" at present.
	Storage string

	// These only make sense if the storage type is "s3".
	S3Region    string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string

	// These only make sense if the storage type is "disk".
	// If the path is relative, it will be assumed relative to the base dir.
	DiskStoreDir string

	// Directory holding muscle config file and other files.
	// Other directories and files are derived from this.
	base string

	// Computed from the corresponding string at load time.
	encryptionKey []byte
}

// Load loads the configuration from the file called "config" in the provided base
// directory.
func Load(base string) (*C, error) {
	filename := filepath.Join(base, "config")
	if fi, err := os.Stat(filename); err != nil {
		return nil, fmt.Errorf("config.Load: %w", err)
	} else if fi.Mode()&0077 != 0 {
		return nil, fmt.Errorf("config.Load %q: mode is %#o, want at most %#o",
			filename, fi.Mode()&0777, fi.Mode()&0700)
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Ignore error closing file opened only for reading.
		_ = f.Close()
	}()
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
	if c.ListenNet == "" && c.ListenAddr == "" {
		c.ListenNet = "unix"
	}
	if c.ListenNet == "unix" && c.ListenAddr == "" {
		c.ListenAddr = fmt.Sprintf("%s/muscle", clientNamespace())
	}
	return c, err
}

func load(f io.Reader) (*C, error) {
	c := C{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		i := strings.IndexAny(line, " 	")
		if i == -1 {
			return nil, fmt.Errorf("load: no separator in %q", line)
		}
		switch key, val := line[:i], strings.TrimSpace(line[i:]); key {
		case "cache-directory":
			c.CacheDirectory = val
		case "disk-store-dir":
			c.DiskStoreDir = val
		case "encryption-key":
			c.EncryptionKey = val
		case "listen-addr":
			c.ListenAddr = val
		case "listen-net":
			c.ListenNet = val
		case "musclefs-mount":
			c.MuscleFSMount = val
		case "s3-bucket":
			c.S3Bucket = val
		case "s3-access-key":
			c.S3AccessKey = val
		case "s3-secret-key":
			c.S3SecretKey = val
		case "s3-region":
			c.S3Region = val
		case "storage":
			c.Storage = val
		default:
			return nil, fmt.Errorf("load: unknown key %q", key)
		}
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}
	return &c, nil
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
	const method = "linuxMountCommand"
	uid, gid := os.Getuid(), os.Getgid()
	switch net {
	case "unix":
		return fmt.Sprintf("sudo mount -t 9p %v %v -o trans=unix,dfltuid=%d,dfltgid=%d,cache=none,noextend,msize=131072", addr, mountpoint, uid, gid), nil
	case "tcp":
		if parts := strings.Split(addr, ":"); len(parts) != 2 {
			return "", errorf(method, "mailformed host-port pair: %q", addr)
		} else {
			return fmt.Sprintf("sudo mount -t 9p %v %v -o trans=tcp,port=%v,dfltuid=%d,dfltgid=%d,cache=none,noextend,msize=131072", parts[0], mountpoint, parts[1], uid, gid), nil
		}
	default:
		return "", errorf(method, "unhandled network type: %v", net)
	}
}

// See mount_9p(8).
func netbsdMountCommand(net string, addr string, mountpoint string) (string, error) {
	const method = "linuxMountCommand"
	if net != "tcp" {
		return "", errorf(method, "unsupported network: %q", net)
	}
	if parts := strings.Split(addr, ":"); len(parts) != 2 {
		return "", errorf(method, "mailformed host-port pair: %q", addr)
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
		return []string{cmd1}, nil
	case "netbsd":
		cmd1, err := netbsdMountCommand(c.ListenNet, c.ListenAddr, c.MuscleFSMount)
		if err != nil {
			return nil, err
		}
		return []string{cmd1}, nil
	default:
		return nil, fmt.Errorf("don't know now to mount on %v", runtime.GOOS)
	}
}

func (c *C) UmountCommands() ([]string, error) {
	switch runtime.GOOS {
	case "linux", "netbsd":
		return []string{
			fmt.Sprintf("sudo umount %s", c.MuscleFSMount),
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

	var buf bytes.Buffer
	mathrand.Seed(time.Now().UnixNano())
	port := 49152 + mathrand.Intn(65535-49152)
	buf.WriteString("listen-net tcp\n")
	fmt.Fprintf(&buf, "listen-addr 127.0.0.1:%d\n", port)
	buf.WriteString("musclefs-mount /mnt/muscle\n")
	b := make([]byte, 32)
	n, err := rand.Read(b)
	if err != nil {
		return fmt.Errorf("could not read 32 random bytes: %w", err)
	}
	if n != 32 {
		return fmt.Errorf("could not read 32 random bytes, got only %d", n)
	}
	fmt.Fprintf(&buf, "encryption-key %02x\n", b)
	buf.WriteString("storage disk\n")
	buf.WriteString("disk-store-dir permanent\n")
	err = ioutil.WriteFile(path, buf.Bytes(), 0600)
	if err != nil {
		return fmt.Errorf("config.Initialize %q: %w", path, err)
	}
	return nil
}

var dotZero = regexp.MustCompile(`\A(.*:\d+)\.0\z`)

// clientNamespace returns the path to the name space directory.
func clientNamespace() string {
	ns := os.Getenv("NAMESPACE")
	if ns != "" {
		return ns
	}

	disp := os.Getenv("DISPLAY")
	if disp == "" {
		// No $DISPLAY? Use :0.0 for non-X11 GUI (OS X).
		disp = ":0.0"
	}

	// Canonicalize: xxx:0.0 => xxx:0.
	if m := dotZero.FindStringSubmatch(disp); m != nil {
		disp = m[1]
	}

	// Turn /tmp/launch/:0 into _tmp_launch_:0 (OS X 10.5).
	disp = strings.Replace(disp, "/", "_", -1)

	// NOTE: plan9port creates this directory on demand.
	// Maybe someday we'll need to do that.

	return fmt.Sprintf("/tmp/ns.%s.%s", os.Getenv("USER"), disp)
}
