package main

import (
	"sync"
	"time"

	"github.com/lionkov/go9p/p/srv"
	"github.com/nicolagi/muscle/tree"
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

// Maps node IDs to 9P mode flags that are maintained in musclefs but
// do not have a counter part in the fs-agnostic tree package.
var moreModes struct {
	sync.Mutex
	m map[uint64]uint32
}

func init() {
	moreModes.m = make(map[uint64]uint32)
}

func moreMode(nodeID uint64) uint32 {
	moreModes.Lock()
	m := moreModes.m[nodeID]
	moreModes.Unlock()
	return m
}

func setMoreMode(nodeID uint64, mode uint32) {
	moreModes.Lock()
	moreModes.m[nodeID] = mode
	moreModes.Unlock()
}
