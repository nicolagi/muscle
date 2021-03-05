package tree

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/nicolagi/muscle/internal/config"
	log "github.com/sirupsen/logrus"
)

func (tree *Tree) isIgnored(revision string, pathname string) bool {
	if tree.ignored == nil {
		return false
	}
	m := tree.ignored[revision]
	if m == nil {
		return false
	}
	_, ok := m[pathname]
	return ok
}

// Ignore marks the given pathname within the given revision as
// ignored for the purpose of pull (merge) operations. In other
// words, a conflict for pathname when merging the revision will
// result in the local version to be kept.
func (tree *Tree) Ignore(revision string, pathname string) {
	if tree.ignored == nil {
		tree.ignored = make(map[string]map[string]struct{})
	}
	m := tree.ignored[revision]
	if m == nil {
		m = make(map[string]struct{})
		tree.ignored[revision] = m
	}
	m[pathname] = struct{}{}
}

func sameKeyOrBothNil(a, b *Node) bool {
	if a != nil && b != nil {
		return a.pointer.Equals(b.pointer)
	}
	return a == nil && b == nil
}

// Returns proposed commands to execute via the ctl file.
// If empty, and no error, it means there's nothing to pull.
func (tree *Tree) PullWorklog(cfg *config.C, baseTree *Tree, remoteTree *Tree) (output string, err error) {
	var buf bytes.Buffer
	err = merge3way(
		tree,       // tree to merge into
		baseTree,   // merge base
		remoteTree, // tree to merge
		tree.root,
		baseTree.root,
		remoteTree.root,
		baseTree.revision.Hex(),
		remoteTree.revision.Hex(),
		remoteTree.root.pointer.Hex(),
		cfg,
		&buf,
	)
	if buf.Len() > 0 {
		_, _ = fmt.Fprintln(&buf, "flush")
		_, _ = fmt.Fprintln(&buf, "pull")
	}
	output = buf.String()
	return
}

func merge3way(localTree, baseTree, remoteTree *Tree, local, base, remote *Node, baseRev, remoteRev string, remoteRoot string, cfg *config.C, output io.Writer) error {
	if sameKeyOrBothNil(local, remote) {
		return nil
	}

	if same, err := sameContents(local, remote); err != nil {
		return err
	} else if same {
		return nil
	}

	if sameKeyOrBothNil(remote, base) {
		// The remote has not changed since the common point in history.
		// We keep the local changes.
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
		p = strings.TrimPrefix(p, "/")
		if remote != nil {
			_, _ = fmt.Fprintf(output, "graft2 %s/%s %s\n", remoteRoot, p, p)
		} else {
			_, _ = fmt.Fprintf(output, "unlink %s\n", p)
		}
		return nil
	}

	// If we are here, local, base and remote are all different.
	// If local and remote have different type, we have a conflict (unless marked resolved).
	// Otherwise, we can try recursion (losing metadata diffs for the directories, but it's something I can stand at the moment).

	if remote != nil {
		resolved := localTree.isIgnored(remoteRoot, strings.TrimPrefix(remote.Path(), "/"))
		if resolved {
			log.Printf("There was a conflict at path %q but it is marked as locally resolved\n", remote.Path())
			return nil
		}
	}

	if !(local != nil && remote != nil && local.IsDir()) || !remote.IsDir() {
		if remote != nil {
			p := remote.Path()
			p = strings.TrimPrefix(p, "/")
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
			_, _ = fmt.Fprintf(output, "# meld %s %s %s\n", localVersion, baseVersion, remoteVersion)
			_, _ = fmt.Fprintf(output, "# meld %s %s\n", localVersion, remoteVersion)
			_, _ = fmt.Fprintf(output, "# diff3 %s %s %s\n", localVersion, baseVersion, remoteVersion)
			_, _ = fmt.Fprintf(output, "# diff %s %s\n", localVersion, remoteVersion)
			_, _ = fmt.Fprintf(output, "# graft2 %s/%s %s\n", remoteRoot, p, p)
			_, _ = fmt.Fprintf(output, "# keep-local-for %s/%s\n", remoteRoot, p)
		}
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
		if err := merge3way(localTree, baseTree, remoteTree, getChild(localChildren, name), getChild(baseChildren, name), getChild(remoteChildren, name), baseRev, remoteRev, remoteRoot, cfg, output); err != nil {
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
