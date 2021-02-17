package tree

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/nicolagi/muscle/internal/storage"
)

// Revision is the analogue of a git commit.
type Revision struct {
	key storage.Pointer // Hash of the fields below

	parent  storage.Pointer
	rootKey storage.Pointer
	host    string // From where the snapshot was taken.
	when    int64  // When the snapshot was taken (in seconds).
}

func NewRevision(root *Node, parent storage.Pointer) *Revision {
	host, err := os.Hostname()
	if err != nil {
		host = "(unknown)"
	}
	return &Revision{
		parent:  parent,
		rootKey: root.pointer,
		host:    host,
		when:    time.Now().Unix(),
	}
}

func (r *Revision) Key() storage.Pointer { return r.key }

func (r *Revision) RootKey() storage.Pointer { return r.rootKey }

func (r *Revision) Time() time.Time {
	return time.Unix(r.when, 0)
}

func (r *Revision) ShortString() string {
	return fmt.Sprintf(
		"timestamp=%d host=%s key=%v parent=%v root=%v",
		r.when,
		r.host,
		r.key,
		r.parent,
		r.rootKey,
	)
}

func (r *Revision) String() string {
	when := time.Unix(r.when, 0)
	ago := time.Since(when).Truncate(time.Second).String()
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "timestamp %s (%s ago)\n", when, ago)
	fmt.Fprintf(&buf, "host %s\n", r.host)
	fmt.Fprintf(&buf, "key %v\n", r.key)
	fmt.Fprintf(&buf, "parent %v\n", r.parent)
	fmt.Fprintf(&buf, "root %v\n", r.rootKey)
	return buf.String()
}

func (r *Revision) RootIs(key storage.Pointer) bool {
	return r.rootKey.Equals(key)
}
