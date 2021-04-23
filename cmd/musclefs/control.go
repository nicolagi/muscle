package main

import (
	"flag"
	"io"

	"github.com/nicolagi/muscle/internal/tree"
)

func doDiff(w io.Writer, localTree *tree.Tree, treeStore *tree.Store, muscleFSMount string, snapshotsFSMount string, args []string) error {
	const method = "doDiff"
	var tagName string
	var diffContext struct {
		prefix  string
		names   bool
		verbose bool
	}
	flags := flag.NewFlagSet("diff", flag.ContinueOnError)
	flags.StringVar(&tagName, "b", "base", "tag `name`")
	flags.BoolVar(&diffContext.verbose, "v", false, "include metadata changes")
	flags.BoolVar(&diffContext.names, "N", false, "only output paths that changed, not context diffs")
	flags.StringVar(&diffContext.prefix, "prefix", "", "omit diffs outside of `path`, e.g., project/name")
	if err := flags.Parse(args); err != nil {
		return errorv(method, err)
	}
	if n := flags.NArg(); n != 0 {
		return errorf(method, "no positional argument expected, got %d", n)
	}
	tag, err := treeStore.RemoteTag(tagName)
	if err != nil {
		return errorv(method, err)
	}
	remoteTree, err := tree.NewTree(treeStore, tree.WithRevision(tag.Pointer))
	if err != nil {
		return errorv(method, err)
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
	if err != nil {
		return errorv(method, err)
	}
	return nil
}
