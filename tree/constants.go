package tree

import (
	"errors"
	"time"
)

const (
	SnapshotFrequency = 3 * time.Minute
)

var (
	RemoteRootKeyPrefix = "remote.root."
	ErrReadOnly         = errors.New("entity is read only")
)
