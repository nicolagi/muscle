package main

import (
	"flag"
	"io"

	"github.com/lionkov/go9p/p"
	"github.com/nicolagi/muscle/internal/tree"
	"github.com/pkg/errors"
)

type ctl struct {
	D        p.Dir
	contents []byte
}

func (f *ctl) read(target []byte, offset int) int {
	if offset > len(f.contents) {
		return 0
	}
	return copy(target, f.contents[offset:])
}

func doDiff(w io.Writer, localTree *tree.Tree, treeStore *tree.Store, muscleFSMount string, snapshotsFSMount string, args []string) error {
	var diffContext struct {
		prefix  string
		names   bool
		verbose bool
	}
	flags := flag.NewFlagSet("diff", flag.ContinueOnError)
	flags.BoolVar(&diffContext.verbose, "v", false, "include metadata changes")
	flags.BoolVar(&diffContext.names, "N", false, "only output paths that changed, not context diffs")
	flags.StringVar(&diffContext.prefix, "prefix", "", "omit diffs outside of `path`, e.g., project/name")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if n := flags.NArg(); n != 0 {
		return errors.Errorf("no positional arguments expected, got %d", n)
	}
	remoteRevisionKey, err := treeStore.RemoteBasePointer()
	if err != nil {
		return err
	}
	remoteTree, err := tree.NewTree(treeStore, tree.WithRevision(remoteRevisionKey))
	if err != nil {
		return err
	}
	err = tree.DiffTrees(
		remoteTree,
		localTree,
		snapshotsFSMount,
		muscleFSMount,
		tree.DiffTreesOutput(w),
		tree.DiffTreesInitialPath(diffContext.prefix),
		tree.DiffTreesNamesOnly(diffContext.names),
		tree.DiffTreesVerbose(diffContext.verbose),
	)
	return err
}
