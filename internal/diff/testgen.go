// +build ignore

// Command testgen takes two arguments, the files to diff, and an optional
// argument -U to specify the number of unified context lines. It runs the
// system's diff executable on those two files, muscle's own diff code, and then
// diffs the results using the system diff. If there is a mismatch, a test case
// is easily constructed from the files left around by this command.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"

	"github.com/nicolagi/muscle/internal/diff"
)

func main() {
	contextLines := flag.Int("U", 3, "unified context lines")
	verbose := flag.Bool("v", false, "show output when system diff and muscle diff match")
	flag.Parse()
	if flag.NArg() != 2 {
		log.Fatalf("want 2 args, got %d", flag.NArg())
	}

	args := flag.Args()
	left, right := args[0], args[1]

	// Exit status will be 1, ignoring error. It might generate a bad test case but it'll be trivial to notice.
	want, _ := exec.Command("diff", "-U", strconv.Itoa(*contextLines), left, right).CombinedOutput()
	// Skip lines with "--- $path" and "+++ $path".
	want = skipLine(skipLine(want))

	lb, err := ioutil.ReadFile(left)
	if err != nil {
		log.Fatal(err)
	}
	rb, err := ioutil.ReadFile(right)
	if err != nil {
		log.Fatal(err)
	}

	got, err := diff.Unified(diff.ByteNode(lb), diff.ByteNode(rb), *contextLines)
	if err != nil {
		log.Fatal(err)
	}

	wantFile, err := ioutil.TempFile("", "testgen-want-")
	if err != nil {
		log.Fatal(err)
	}
	gotFile, err := ioutil.TempFile("", "testgen-got-")
	if err != nil {
		log.Fatal(err)
	}
	_, _ = fmt.Fprint(wantFile, string(want))
	_, _ = fmt.Fprint(gotFile, got)
	_ = wantFile.Close()
	_ = gotFile.Close()

	mismatch, _ := exec.Command("diff", "-U", strconv.Itoa(*contextLines), wantFile.Name(), gotFile.Name()).CombinedOutput()

	if m := string(mismatch); m != "" {
		fmt.Fprintln(os.Stderr, m)
		os.Exit(1)
	} else {
		log.Printf("Output of size %d bytes matched.", len(got))
		if *verbose {
			fmt.Fprintln(os.Stderr, got)
		}
		_ = os.Remove(wantFile.Name())
		_ = os.Remove(gotFile.Name())
	}
}

func skipLine(b []byte) []byte {
	if i := bytes.IndexByte(b, 10); i != -1 {
		return b[i+1:]
	}
	return b
}
