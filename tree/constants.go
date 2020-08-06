package tree

import (
	"errors"
	"log"
	"os/user"
	"time"
)

const (
	SnapshotFrequency = 3 * time.Minute
)

var (
	RemoteRootKeyPrefix = "remote.root."
	ErrReadOnly         = errors.New("entity is read only")

	nodeUID string
	nodeGID string
)

func init() {
	u, err := user.Current()
	if err != nil {
		log.Fatalf("could not get current user: %v", err)
	}
	nodeUID = u.Username
	g, err := user.LookupGroupId(u.Gid)
	if err != nil {
		log.Fatalf("could not get group %v: %v", u.Gid, err)
	}
	nodeGID = g.Name
}
