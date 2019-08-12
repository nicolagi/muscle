package tree

import (
	"errors"
	"time"
)

const (
	maxBlobSizeForDiff = 1024 * 1024
	SnapshotFrequency  = 3 * time.Minute
)

var (
	RemoteRootKeyPrefix = "remote.root."
	ErrReadOnly         = errors.New("entity is read only")
)
