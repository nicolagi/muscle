package tree

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/nicolagi/muscle/storage"
	log "github.com/sirupsen/logrus"
)

// Revision is the analogue of a git commit.
type Revision struct {
	key storage.Pointer // Hash of the fields below

	parents  []storage.Pointer
	rootKey  storage.Pointer
	instance string
	hostname string // From where the snapshot was taken.
	when     int64  // When the snapshot was taken (in seconds).
}

func NewRevision(rootKey storage.Pointer, parents []storage.Pointer) *Revision {
	hostname, err := os.Hostname()
	if err != nil {
		log.WithField("err", err.Error()).Error("Could not get hostname")
		hostname = "(unknown)"
	}
	return &Revision{
		parents:  parents,
		rootKey:  rootKey,
		hostname: hostname,
		when:     time.Now().Unix(),
	}
}

func (r *Revision) Key() storage.Pointer { return r.key }

func (r *Revision) Time() time.Time {
	return time.Unix(r.when, 0)
}

func (r *Revision) String() string {
	when := time.Unix(r.when, 0)
	ago := time.Since(when).Truncate(time.Second).String()
	buf := bytes.NewBuffer(nil)
	fmt.Fprintf(buf, "revision taken %s ago, precisely %s\n", ago, when)
	fmt.Fprintf(buf, "instance %s\n", r.instance)
	fmt.Fprintf(buf, "host %s\n", r.hostname)
	fmt.Fprintf(buf, "key %s\n", r.key.Hex())
	fmt.Fprint(buf, "parents")
	for _, pk := range r.parents {
		fmt.Fprintf(buf, " %s", pk.Hex())
	}
	fmt.Fprintln(buf)
	fmt.Fprintf(buf, "root %s\n", r.rootKey.Hex())
	return buf.String()
}

func (r *Revision) RootIs(key storage.Pointer) bool {
	return r.rootKey.Equals(key)
}
