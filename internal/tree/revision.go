package tree

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/nicolagi/muscle/internal/storage"
)

type Tag struct {
	Name    string
	Pointer storage.Pointer // Needs to point to a Revision.
}

// Revision is the analogue of a git commit.
type Revision struct {
	key storage.Pointer // Hash of the fields below

	parents []Tag
	rootKey storage.Pointer
	host    string // From where the snapshot was taken.
	when    int64  // When the snapshot was taken (in seconds).
}

func NewRevision(root *Node, parents []Tag) *Revision {
	host, err := os.Hostname()
	if err != nil {
		host = "(unknown)"
	}
	return &Revision{
		parents: parents,
		rootKey: root.pointer,
		host:    host,
		when:    time.Now().Unix(),
	}
}

func (r *Revision) Key() storage.Pointer { return r.key }

func (r *Revision) Parent(tagName string) (Tag, bool) {
	for _, tag := range r.parents {
		if tag.Name == tagName {
			return tag, true
		}
	}
	return Tag{}, false
}

func (r *Revision) RootKey() storage.Pointer { return r.rootKey }

func (r *Revision) Time() time.Time {
	return time.Unix(r.when, 0)
}

func (r *Revision) ShortString() string {
	var b bytes.Buffer
	_, _ = fmt.Fprintf(
		&b,
		"timestamp=%d host=%s key=%v root=%v",
		r.when,
		r.host,
		r.key,
		r.rootKey,
	)
	for _, p := range r.parents {
		_, _ = fmt.Fprintf(&b, " parent-%s=%v", p.Name, p.Pointer)
	}
	return b.String()
}

func (r *Revision) String() string {
	when := time.Unix(r.when, 0)
	ago := time.Since(when).Truncate(time.Second).String()
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "timestamp %s (%s ago)\n", when, ago)
	fmt.Fprintf(&buf, "host %s\n", r.host)
	fmt.Fprintf(&buf, "key %v\n", r.key)
	for _, p := range r.parents {
		fmt.Fprintf(&buf, "parent-%s %v\n", p.Name, p.Pointer)
	}
	fmt.Fprintf(&buf, "root %v\n", r.rootKey)
	return buf.String()
}

func (r *Revision) RootIs(key storage.Pointer) bool {
	return r.rootKey.Equals(key)
}
