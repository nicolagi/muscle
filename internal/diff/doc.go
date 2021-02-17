// Package diff implements unified diffs for a certain type of node objects that
// occur in muscle (https://github.com/nicolagi/muscle/internal) but can be applied in
// particular to strings. The output format is unified diff with a configurable
// number of unified context lines.
//
// The code in this package builds on top of https://github.com/andreyvit/diff,
// which generates line diffs (with unlimited context lines) on top of the word
// diffs produced by the diffmatchpatch package
// (https://github.com/sergi/go-diff).
//
// I've compared this diff to GNU diff (that's where the test cases in testdata/
// come from) and it seems that a limitation of this diff is that it's not smart
// about reordered lines.
package diff
