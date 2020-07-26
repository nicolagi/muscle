package tree

import (
	"errors"
	"os"
	"time"

	"github.com/lionkov/go9p/p"
)

const (
	SnapshotFrequency = 3 * time.Minute
)

var (
	RemoteRootKeyPrefix = "remote.root."
	ErrReadOnly         = errors.New("entity is read only")

	nodeUID = p.OsUsers.Uid2User(os.Geteuid()).Name()
	nodeGID = p.OsUsers.Gid2Group(os.Getegid()).Name()
)
