package main

import (
	"sync"
	"time"

	"github.com/lionkov/go9p/p/srv"
	"github.com/nicolagi/muscle/internal/tree"
)

const (
	numNodeLocks        = 1024
	lockDurationSeconds = 300
)

type nodeLock struct {
	owner   *srv.Fid
	node    *tree.Node
	expires uint32
}

var nodelocks struct {
	sync.Mutex
	locks [numNodeLocks]nodeLock
}

// Returns nil if the node is already locked or node locks were exhausted.
func lockNode(owner *srv.Fid, node *tree.Node) *nodeLock {
	now := uint32(time.Now().Unix())
	nodelocks.Lock()
	defer nodelocks.Unlock()
	for i := range nodelocks.locks {
		if l := &nodelocks.locks[i]; l.expires <= now { // Is lock expired?
			l.expires = now + lockDurationSeconds
			l.owner = owner
			l.node = node
			return l
		} else if l.node == node {
			return nil // Already locked by a non-expired lock.
		}
	}
	return nil
}

// Marks the lock free.
func unlockNode(l *nodeLock) {
	nodelocks.Lock()
	l.expires = 0
	// Prevent loitering:
	l.owner = nil
	l.node = nil
	nodelocks.Unlock()
}
