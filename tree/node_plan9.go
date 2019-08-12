// +build plan9

package tree

import (
	"os/user"

	log "github.com/sirupsen/logrus"
)

var (
	nodeUID string
	nodeGID string
)

func init() {
	u, err := user.Current()
	if err != nil {
		log.Fatalf("Could not get current user: %w", err)
	}
	nodeUID = u.Uid
	nodeGID = u.Gid
}
