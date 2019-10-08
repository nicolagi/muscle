package tree

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nicolagi/muscle/config"
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
	remote, err := factory.store.RemoteRevisionKey(srcInstance)
	if err != nil {
		return fmt.Errorf("could not get remote revision for %q: %v", srcInstance, err)
	}
	ancestor, err := factory.store.MergeBase(dst.revision, remote)
	if err != nil {
		return fmt.Errorf("could not find ancestor for %q (destination, local) and %q (source, remote): %v", dst.revision.Hex(), remote.Hex(), err)
	}
	remoteTree, err := factory.NewTree(remote, true)
	if err != nil {
		return fmt.Errorf("could not build tree for %q (remote): %v", remote.Hex(), err)
	}
	ancestorTree, err := factory.NewTree(ancestor, true)
	if err != nil {
		return fmt.Errorf("could not build tree for %q (ancestor): %v", ancestor.Hex(), err)
	}
	defer func() {
		fmt.Printf("echo flush > %s/ctl\n", cfg.MuscleFSMount)
		fmt.Println("# If all is merged fine, run the following to create a merge commit.")
		fmt.Printf("# echo snapshot %s > %s/ctl\n", remote.Hex(), cfg.MuscleFSMount)
	}()
	return merge3way(keepLocalFn, dst, ancestorTree, remoteTree, dst.root, ancestorTree.root, remoteTree.root, ancestor.Hex(), remote.Hex(), cfg)
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
		// Local is equal to remote, nothing to do
		return nil
	}

	if sameContents(local, remote) {
		// Ignore metadata differences
		return nil
	}

	if sameKeyOrBothNil(remote, base) {
		// The remote has not changed since the common point in history.
		// We keep the local changes.
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
			fmt.Printf("echo graft %s/%s %s > %s/ctl\n", remoteRev, p, p, cfg.MuscleFSMount)
		} else {
			fmt.Printf("# echo rm -rf %s/%s\n", cfg.MuscleFSMount, p)
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
			fmt.Printf("# echo graft %s/%s %s > %s/ctl\n", remoteRev, p, p+".merge-conflict", cfg.MuscleFSMount)
			fmt.Printf("# echo graft %s/%s %s > %s/ctl\n", remoteRev, p, p, cfg.MuscleFSMount)
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
			fmt.Printf("# echo keep-local-for %s/%s > %s/ctl\n", remoteRev, p, cfg.MuscleFSMount)
		}
		fmt.Println("EOE")
		return nil
	}

	// Prepare children maps and the names of children to merge (union of local and remote)
	mergeNames := make(map[string]struct{})
	var localChildren, baseChildren, remoteChildren map[string]*Node
	if local != nil {
		localTree.Grow(local)
		localChildren = local.childrenMap()
		for name := range localChildren {
			mergeNames[name] = struct{}{}
		}
	}
	if base != nil && base.IsDir() {
		baseTree.Grow(base)
		baseChildren = base.childrenMap()
	}
	if remote != nil {
		remoteTree.Grow(remote)
		remoteChildren = remote.childrenMap()
		for name := range remoteChildren {
			mergeNames[name] = struct{}{}
		}
	}

	for name := range mergeNames {
		if err := merge3way(keepLocalFn, localTree, baseTree, remoteTree, getChild(localChildren, name), getChild(baseChildren, name), getChild(remoteChildren, name), baseRev, remoteRev, cfg); err != nil {
			return err
		}
	}

	return nil
}

func sameContents(a *Node, b *Node) bool {
	if a == nil || b == nil || a.IsDir() || b.IsDir() {
		return false
	}
	return a.hasEqualBlocks(b)
}

func getChild(nodes map[string]*Node, s string) *Node {
	if nodes == nil {
		return nil
	}
	return nodes[s]
}
