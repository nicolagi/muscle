// +build linux

package tree

import (
	"os"

	"github.com/nicolagi/go9p/p"
)

var (
	nodeUID = p.OsUsers.Uid2User(os.Geteuid()).Name()
	nodeGID = p.OsUsers.Gid2Group(os.Getegid()).Name()
)
