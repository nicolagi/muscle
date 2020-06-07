package tree

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/storage"
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

// Merge logs diagnostic messages and command to be run to merge changes from another revision.
// It will not modify the tree.
func Merge(keepLocalFn KeepLocalFn, dst *Tree, srcInstance string, factory *Factory, cfg *config.C) error {
	remote, _, err := factory.store.RemoteRevision(srcInstance)
	if err != nil {
		return fmt.Errorf("could not get remote revision for %q: %v", srcInstance, err)
	}
	// dst.revision is going to be empty because the local revision (the destination)
	// doesn't really exist. We make it up just as in ../cmd/muscle/muscle.go:/parseRevision/.
	parentKey, err := factory.store.RemoteRevisionKey(dst.instance)
	if err != nil {
		return err
	}
	dstrev := NewRevision(dst.instance, dst.root.pointer, []storage.Pointer{parentKey})
	ancestor, err := factory.store.MergeBase(dstrev, remote)
	if err != nil {
		return err
	}
	remoteTree, err := factory.NewTree(factory.WithRevisionKey(remote.key))
	if err != nil {
		return fmt.Errorf("could not build tree for %q (remote): %v", remote.key, err)
	}
	ancestorTree, err := factory.NewTree(factory.WithRevisionKey(ancestor))
	if err != nil {
		return fmt.Errorf("could not build tree for %q (ancestor): %v", ancestor.Hex(), err)
	}
	defer func() {
		fmt.Println("flush")
		fmt.Println("# If all is merged fine, run the following to create a merge commit.")
		fmt.Printf("# snapshot %s\n", remote.key.Hex())
	}()
	return merge3way(keepLocalFn, dst, ancestorTree, remoteTree, dst.root, ancestorTree.root, remoteTree.root, ancestor.Hex(), remote.key.Hex(), cfg)
}

func sameKeyOrBothNil(a, b *Node) bool {
	if a != nil && b != nil {
		return a.pointer.Equals(b.pointer)
	}
	return a == nil && b == nil
}

// TODO Some commands are output in comments so one can't just pipe to rc. (One shouldn't without checking, anyway...)
func merge3way(keepLocalFn KeepLocalFn, localTree, baseTree, remoteTree *Tree, local, base, remote *Node, baseRev, remoteRev string, cfg *config.C) error {
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

	if local != nil && !local.IsRoot() && local.refs != 0 {
		return fmt.Errorf("node %q has %d references", local.Path(), local.refs)
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
			fmt.Printf("graft %s/%s %s\n", remoteRev, p, p)
		} else {
			fmt.Printf("unlink %s\n", p)
		}
		return nil
	}

	// If we are here, local, base and remote are all different.
	// If local and remote have different type, we have a conflict (unless marked resolved).
	// Otherwise, we can try recursion (losing metadata diffs for the directories, but it's something I can stand at the moment).

	if remote != nil {
		resolved := keepLocalFn(remoteRev, strings.TrimPrefix(remote.Path(), "root/"))
		if resolved {
			fmt.Printf("# There was a conflict at path %q but it is marked as locally resolved\n", remote.Path())
			return nil
		}
	}

	if !(local != nil && remote != nil && local.IsDir()) || !remote.IsDir() {
		fmt.Println("---")
		if local != nil {
			fmt.Println(local.Path())
		}
		if remote != nil {
			p := remote.Path()
			fmt.Printf("%s/%s\n", remoteRev, p)
			p = strings.TrimPrefix(p, "root/")
			fmt.Printf("# graft %s/%s %s\n", remoteRev, p, p+".merge-conflict")
			fmt.Printf("# graft %s/%s %s\n", remoteRev, p, p)
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
			fmt.Printf("meld %s %s %s\n", localVersion, baseVersion, remoteVersion)
			fmt.Printf("# keep-local-for %s/%s\n", remoteRev, p)
		}
		fmt.Println("EOE")
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
		if err := merge3way(keepLocalFn, localTree, baseTree, remoteTree, getChild(localChildren, name), getChild(baseChildren, name), getChild(remoteChildren, name), baseRev, remoteRev, cfg); err != nil {
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
