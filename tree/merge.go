package tree

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nicolagi/muscle/config"
	log "github.com/sirupsen/logrus"
)

type KeepLocalFn func(string, string) bool

func MustKeepLocalFn(dir string) (rf KeepLocalFn, cleanup func()) {
	var mu sync.Mutex
	pathnamesByRevision := make(map[string]map[string]struct{})
	return func(revision, pathname string) bool {
		mu.Lock()
		defer mu.Unlock()
		pathnames := pathnamesByRevision[revision]
		if pathnames == nil {
			f, _ := os.OpenFile(keepPathName(dir, revision), os.O_RDONLY|os.O_CREATE, 0640)
			defer func() {
				_ = f.Close()
			}()
			s := bufio.NewScanner(f)
			pathnames = make(map[string]struct{})
			for s.Scan() {
				pathnames[s.Text()] = struct{}{}
			}
			_ = s.Err()
			pathnamesByRevision[revision] = pathnames
		}
		_, ok := pathnames[pathname]
		return ok
	}, func() {}
}

func KeepLocalFor(dir, revision, pathname string) error {
	f, err := os.OpenFile(keepPathName(dir, revision), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	_, err = fmt.Fprintln(f, pathname)
	return err
}

func keepPathName(dir string, revision string) string {
	return filepath.Join(dir, revision+".keep")
}

func sameKeyOrBothNil(a, b *Node) bool {
	if a != nil && b != nil {
		return a.pointer.Equals(b.pointer)
	}
	return a == nil && b == nil
}

// Returns proposed commands to execute via the ctl file.
// If empty, and no error, it means there's nothing to pull.
func (tree *Tree) PullWorklog(keep KeepLocalFn, cfg *config.C, baseTree *Tree, remoteTree *Tree) (output string, err error) {
	var buf bytes.Buffer
	err = merge3way(
		keep,
		tree,       // tree to merge into
		baseTree,   // merge base
		remoteTree, // tree to merge
		tree.root,
		baseTree.root,
		remoteTree.root,
		baseTree.revision.Hex(),
		remoteTree.revision.Hex(),
		cfg,
		&buf,
	)
	output = buf.String()
	return
}

// TODO Some commands are output in comments so one can't just pipe to rc. (One shouldn't without checking, anyway...)
func merge3way(keepLocalFn KeepLocalFn, localTree, baseTree, remoteTree *Tree, local, base, remote *Node, baseRev, remoteRev string, cfg *config.C, output io.Writer) error {
	if sameKeyOrBothNil(local, remote) {
		log.Printf("Same key (or both nil): %v", local)
		return nil
	}

	if same, err := sameContents(local, remote); err != nil {
		return err
	} else if same {
		log.Printf("Same contents (ignoring metadata differences): %v", local)
		return nil
	}

	if sameKeyOrBothNil(remote, base) {
		// The remote has not changed since the common point in history.
		// We keep the local changes.
		log.Printf("Only locally changed, remote is equal to common ancestor: %v", local)
		return nil
	}

	if sameKeyOrBothNil(local, base) && (local == nil || !local.IsRoot()) {
		// If we are here, we need to take the remote changes. There are many cases:
		// - local copy does not exist, only added in remote
		// - local copy exists, changed in remote
		// - local copy exists, removed in remote
		var p string
		if local != nil {
			p = local.Path()
		} else {
			p = remote.Path()
		}
		p = strings.TrimPrefix(p, "root/")
		if remote != nil {
			_, _ = fmt.Fprintf(output, "graft %s/%s %s\n", remoteRev, p, p)
		} else {
			_, _ = fmt.Fprintf(output, "unlink %s\n", p)
		}
		return nil
	}

	// If we are here, local, base and remote are all different.
	// If local and remote have different type, we have a conflict (unless marked resolved).
	// Otherwise, we can try recursion (losing metadata diffs for the directories, but it's something I can stand at the moment).

	if remote != nil {
		resolved := keepLocalFn(remoteRev, strings.TrimPrefix(remote.Path(), "root/"))
		if resolved {
			_, _ = fmt.Fprintf(output, "# There was a conflict at path %q but it is marked as locally resolved\n", remote.Path())
			return nil
		}
	}

	if !(local != nil && remote != nil && local.IsDir()) || !remote.IsDir() {
		_, _ = fmt.Fprintln(output, "---")
		if local != nil {
			_, _ = fmt.Fprintln(output, local.Path())
		}
		if remote != nil {
			p := remote.Path()
			_, _ = fmt.Fprintf(output, "%s/%s\n", remoteRev, p)
			p = strings.TrimPrefix(p, "root/")
			_, _ = fmt.Fprintf(output, "# graft %s/%s %s\n", remoteRev, p, p+".merge-conflict")
			_, _ = fmt.Fprintf(output, "# graft %s/%s %s\n", remoteRev, p, p)
			localVersion := filepath.Join(
				cfg.MuscleFSMount,
				p,
			)
			baseVersion := filepath.Join(
				cfg.SnapshotsFSMount,
				baseRev,
				p,
			)
			remoteVersion := filepath.Join(
				cfg.SnapshotsFSMount,
				remoteRev,
				p,
			)
			_, _ = fmt.Fprintf(output, "meld %s %s %s\n", localVersion, baseVersion, remoteVersion)
			_, _ = fmt.Fprintf(output, "# keep-local-for %s/%s\n", remoteRev, p)
		}
		_, _ = fmt.Fprintln(output, "EOE")
		return nil
	}

	// Prepare children maps and the names of children to merge (union of local and remote)
	mergeNames := make(map[string]struct{})
	var localChildren, baseChildren, remoteChildren map[string]*Node
	if err := localTree.Grow(local); err != nil {
		return fmt.Errorf("tree.merge3way: %w", err)
	}
	localChildren = local.childrenMap()
	for name := range localChildren {
		mergeNames[name] = struct{}{}
	}
	if base != nil && base.IsDir() {
		if err := baseTree.Grow(base); err != nil {
			return fmt.Errorf("tree.merge3way: %w", err)
		}
		baseChildren = base.childrenMap()
	}
	if err := remoteTree.Grow(remote); err != nil {
		return fmt.Errorf("tree.merge3way: %w", err)
	}
	remoteChildren = remote.childrenMap()
	for name := range remoteChildren {
		mergeNames[name] = struct{}{}
	}

	for name := range mergeNames {
		if err := merge3way(keepLocalFn, localTree, baseTree, remoteTree, getChild(localChildren, name), getChild(baseChildren, name), getChild(remoteChildren, name), baseRev, remoteRev, cfg, output); err != nil {
			return err
		}
	}

	return nil
}

func sameContents(a *Node, b *Node) (bool, error) {
	if a == nil || b == nil || a.IsDir() || b.IsDir() {
		return false, nil
	}
	return a.hasEqualBlocks(b)
}

func getChild(nodes map[string]*Node, s string) *Node {
	if nodes == nil {
		return nil
	}
	return nodes[s]
}
