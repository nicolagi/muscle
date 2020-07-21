package main

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nicolagi/muscle/config"
)

/*
// run9P is a wrapper to run the plan9port program 9p in the test
// below. The run() method will mark the test failed if the command is
// unsuccessful, and will be a no-op for subsequent calls.
type run9P struct {
	t    *testing.T
	args []string
}

func newRun9P(t *testing.T, addr string) run9P {
	return run9P{
		t:    t,
		args: []string{"-a", addr},
	}
}

func (p run9P) run(input string, args ...string) {
	if p.t.Failed() {
		return
	}
	args = append(p.args, args...)
	cmd := exec.Command("9p", args...)
	cmd.Stdin = bytes.NewBufferString(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		p.t.Logf("Command args: %v", args)
		p.t.Logf("Command combined output: %v", string(output))
		p.t.Logf("Command failed: %v", err)
		p.t.Fail()
	}
}
*/

func TestInit(t *testing.T) {
	tryRemoveAll := func(dir string) {
		_ = os.RemoveAll(dir)
	}
	t.Run("if config already exists, its contents untouched and error returned", func(t *testing.T) {
		base, err := ioutil.TempDir("", "muscle")
		if err != nil {
			t.Fatal(err)
		}
		defer tryRemoveAll(base)
		path := filepath.Join(base, "config")
		contents := make([]byte, 32)
		n, err := rand.Read(contents)
		if n != 32 {
			t.Fatalf("could not generate 32 random bytes, got only %d", n)
		}
		if err != nil {
			t.Fatalf("could not generate 32 random bytes: %v", err)
		}
		if err := ioutil.WriteFile(path, contents, 0600); err != nil {
			t.Fatalf("could not write random config file at %q: %v", path, err)
		}
		if err := config.Initialize(base); err == nil {
			t.Error("expected an error, got nil")
		}
		got, err := ioutil.ReadFile(path)
		if err != nil {
			t.Fatalf("could not read back random config file at %q: %v", path, err)
		}
		if diff := cmp.Diff(contents, got); diff != "" {
			t.Errorf("config file has changed (-want +got):\n%s", diff)
		}
	})
	t.Run("creates working configuration", func(t *testing.T) {
		t.Skip("TODO rewrite using push/pull")
		/*
			base, err := ioutil.TempDir("", "muscle")
			if err != nil {
				t.Fatal(err)
			}
			defer tryRemoveAll(base)
			base = filepath.Join(base, "muscle") // Ensures init creates dirs if necessary.
			if err := config.Initialize(base); err != nil {
				t.Fatal(err)
			}
			c, err := config.Load(base)
			if err != nil {
				t.Fatal(err)
			}
			output, err := exec.Command("go", "run", ".", "history", "-base", base, "local").CombinedOutput()
			if err != nil {
				t.Errorf("could not print (empty) history for generated config: %v", err)
			}
			if diff := cmp.Diff([]byte{}, output); diff != "" {
				t.Errorf("unexpected output for muscle history (-want +got):\n%s", diff)
			}

			srvc := exec.Command("go", "run", "../musclefs/", "-base", base)
			// Create a new process group for go run, so that killing its process group will also kill musclefs.
			srvc.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			srverr, err := srvc.StderrPipe()
			if err != nil {
				t.Fatalf("could not get stderr pipe for musclefs: %v", err)
			}
			go func(t *testing.T, r io.Reader) {
				s := bufio.NewScanner(r)
				for s.Scan() {
					t.Logf("musclefs: %s", s.Text())
				}
				// On successful execution, we might get something like "read |0: file already closed".
				if err := s.Err(); err != nil && !strings.Contains(err.Error(), "file already closed") {
					t.Errorf("could not get all stderr from musclefs: %v", err)
				}
			}(t, srverr)
			if err := srvc.Start(); err != nil {
				t.Fatalf("could not start musclefs: %v", err)
			}
			defer func(t *testing.T, cmd *exec.Cmd) {
				// Kill (with no possibility of trapping the signal) the process group (go run, and musclefs).
				if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
					t.Fatalf("could not kill musclefs: %v", err)
				}
				// Check that musclefs exits; and error because of the above kill is expected.
				if err := cmd.Wait(); err != nil && !strings.Contains(err.Error(), "killed") {
					t.Fatalf("musclefs execution error: %v", err)
				}
			}(t, srvc)

			if err := netutil.WaitForListener(c.ListenAddress(), 10*time.Second); err != nil {
				t.Fatalf("failed to connect to musclefs within 10 seconds: %v", err)
			}

			// Next part depends on having the 9p program from plan9port.
			// I want to use an external program to act on the filesystem.
			// This will create a file and flush the changes to create a new revision.
			p := newRun9P(t, c.ListenAddress())
			p.run("", "create", "test_file")
			p.run("test content\n", "write", "test_file")
			p.run("push\n", "write", "ctl")

			// Check the history again.
			output, err = exec.Command("go", "run", ".", "history", "-base", base, "local").CombinedOutput()
			if err != nil {
				t.Errorf("could not print (non-empty) history for generated config: %v", err)
			}
			t.Log(string(output))
			lines := strings.Split(string(output), "\n")
			if got, want := len(lines), 15; got != want {
				t.Fatalf("got %d lines, want %d", got, want)
			}
			if !strings.HasPrefix(lines[4], "parents ") {
				t.Errorf(`got %q, want something prefixed by "parents "`, lines[4])
			}
			if got, want := lines[11], "parents"; got != want {
				t.Errorf("got %q as parents line, want %q", got, want)
			}
		*/
	})
}
