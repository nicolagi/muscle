package tree

import (
	"time"
)

// baseError is an error type to be used when a stack trace is not required or would be confusing.
type baseErr string

// Error implements error.
func (e baseErr) Error() string {
	return string(e)
}

const (
	SnapshotFrequency = 3 * time.Minute
)

var (
	RemoteRootKeyPrefix = "remote.root."
	ErrReadOnly         = baseErr("read-only")

	ErrExist      = baseErr("exists")
	ErrNotEmpty   = baseErr("not empty")
	ErrNotExist   = baseErr("does not exist")
	ErrPermission = baseErr("permission denied")

	// ErrPhase indicates that the caller didn't use this package as
	// intended, e.g., remove a node that's already been removed. At the
	// time of writing, not all preconditions are checked.
	ErrPhase = baseErr("phase error")

	// ErrInvariant is like ErrPhase but the responsibility for the
	// misuse lies within this package itself. At the time of writing,
	// not all preconditions are checked.
	ErrInvariant = baseErr("invariant error")
)
