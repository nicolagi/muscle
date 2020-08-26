package tree

import (
	"time"

	"github.com/pkg/errors"
)

const (
	SnapshotFrequency = 3 * time.Minute
)

var (
	RemoteRootKeyPrefix = "remote.root."
	ErrReadOnly         = errors.New("entity is read only")

	// ErrPhase indicates that the caller didn't use this package as
	// intended, e.g., remove a node that's already been removed. At the
	// time of writing, not all preconditions are checked.
	ErrPhase = errors.New("phase error")

	// ErrInvariant is like ErrPhase but the responsibility for the
	// misuse lies within this package itself. At the time of writing,
	// not all preconditions are checked.
	ErrInvariant = errors.New("invariant error")
)
