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
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	AWSCredentialsPath string

	// DefaultBaseDirectoryPath is where all muscle commands store configuration and data.
	// It defaults to $MUSCLE_BASE if it is set, otherwise it defaults to $HOME/lib/muscle.
	// Commands override this via the -base flag.
	DefaultBaseDirectoryPath string
)

func init() {
	u, err := user.Current()
	if err != nil {
		log.Fatalf("Could not get current user: %v", err)
	}
	DefaultBaseDirectoryPath = os.Getenv("MUSCLE_BASE")
	if DefaultBaseDirectoryPath == "" {
		DefaultBaseDirectoryPath = path.Join(u.HomeDir, "lib/muscle")
	}
	// The AWS Go packages could be updated to use $home instead of $HOME for Plan 9,
	// but I'd hate the location $home/.aws/credentials on Plan 9.
	AWSCredentialsPath = ""
	if runtime.GOOS == "plan9" {
		AWSCredentialsPath = path.Join(u.HomeDir, "lib/aws/credentials")
	}
}

type C struct {
	// Listen on localhost or a local-only network, e.g., one for
	// containers hosted on your computer.  There is no
	// authentication so the file server must not be exposed on a
	// public address.
	ListenIP   string `json:"listen-ip"`
	ListenPort int    `json:"listen-port"`

	SnapshotsFSListenIP   string `json:"snapshotsfs-listen-ip"`
	SnapshotsFSListenPort int    `json:"snapshotsfs-listen-port"`

	MuscleFSMount    string `json:"musclefs-mount"`
	SnapshotsFSMount string `json:"snapshotsfs-mount"`

	// 64 hex digits - do not lose this or you lose access to all
	// data.
	EncryptionKey string `json:"encryption-key"`

	// Identifies the filesystem, I use the hostname.
	Instance string `json:"instance"`

	// Permanent storage type - can be "s3" or "null" at present.
	Storage string `json:"storage"`

	// These only make sense if the storage type is "s3".  The AWS
	// profile is used for credentials.
	S3Profile string `json:"s3-profile"`
	S3Region  string `json:"s3-region"`
	S3Bucket  string `json:"s3-bucket"`

	// These only make sense if the storage type is "disk".
	// If the path is relative, it will be assumed relative to the base dir.
	DiskStoreDir string `json:"disk-store-dir"`

	// Other instances to expose read-only trees for (see snapshotsfs).
	ReadOnlyInstances []string `json:"read-only-instances"`

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
	return c, err
}

func load(r io.Reader) (c *C, err error) {
	err = json.NewDecoder(r).Decode(&c)
	return
}

// ListenAddress is the ip:port pair for musclefs to listen on.
func (c *C) ListenAddress() string {
	return fmt.Sprintf("%s:%d", c.ListenIP, c.ListenPort)
}

// SnapshotsFSListenAddr is the ip:port pair for snapshotsfs to listen on.
func (c *C) SnapshotsFSListenAddr() string {
	return fmt.Sprintf("%s:%d", c.SnapshotsFSListenIP, c.SnapshotsFSListenPort)
}

func (c *C) CacheDirectoryPath() string {
	return path.Join(c.base, "cache")
}

func (c *C) ConflictResolutionDirectoryPath() string {
	return path.Join(c.base, "conflicts")
}

func (c *C) MuscleLogFilePath() string {
	return path.Join(c.base, "muscle.log")
}

func (c *C) MuscleFSLogFilePath() string {
	return path.Join(c.base, "musclefs.log")
}

func (c *C) SnapshotsFSLogFilePath() string {
	return path.Join(c.base, "snapshotsfs.log")
}

// An instance of *storage.Paired will log keys to propagate from the
// fast store to the slow store to this append-only log.  This will
// ensure all data is eventually copied to the slow store, even if
// musclefs restarts.
func (c *C) PropagationLogFilePath() string {
	return path.Join(c.base, "propagation.log")
}

// RootKeyFilePath is the path to the file that holds the hash pointer
// to the root of the tree and to the previous root.  Do not lose this
// pointer, or you lose all your current data and past snapshots of
// it.  TODO: Turn it into a tamper-evident log so you keep references
// to past snapshots in case the most recent root gets corrupted or
// lost because of some regression. In the meantime, pointers to past
// revisions can be obtained from the musclefs log file.
func (c *C) RootKeyFilePath() string {
	return path.Join(c.base, "root")
}

func (c *C) StagingDirectoryPath() string {
	return path.Join(c.base, "staging")
}

func (c *C) EncryptionKeyBytes() []byte {
	return c.encryptionKey
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
	c.ListenIP = "127.0.0.1"
	c.SnapshotsFSListenIP = "127.0.0.1"
	mathrand.Seed(time.Now().UnixNano())
	c.ListenPort = 49152 + mathrand.Intn(65535-49152)
	c.SnapshotsFSListenPort = 49152 + mathrand.Intn(65535-49152)
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
	name, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("could not get host name: %w", err)
	}
	c.Instance = name
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
