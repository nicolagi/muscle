package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/clnt"
	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	// To set this at build time, use go build -ldflags '-X main.version=something'. The release script in the
	// repository toplevel directory uses this to create binaries and a versioned tarball.
	version = "unknown"

	// Flag sets are associated with the fields of a corresponding context struct, perhaps not aptly named. Sometimes
	// the properties are bound to positional arguments. The global context is for flags that are part of all flag sets,
	// that is, all sub-commands.
	globalContext struct {
		base     string
		logLevel string
	}

	cleanContext struct {
		storedKeys string
		neededKeys string
	}

	diffContext struct {
		context int
		prefix  string
		names   bool
		verbose bool
		maxSize int
	}

	historyContext struct {
		prefix string
		count  int
		diff   bool

		// These apply only if diff is true.
		context int
		names   bool
		verbose bool
		maxSize int
	}
)

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.StringVar(&globalContext.base, "base", config.DefaultBaseDirectoryPath, "`directory` for caches, configuration, logs, etc.")
	var levels []string
	for _, l := range logrus.AllLevels {
		levels = append(levels, l.String())
	}
	fs.StringVar(&globalContext.logLevel, "verbosity", "warning", "sets the log `level`, among "+strings.Join(levels, ", "))
	return fs
}

func exitUsage(msg string) {
	_, _ = fmt.Fprintln(os.Stderr, msg)
	_, _ = fmt.Fprintf(os.Stderr, `Usage: %s COMMAND [ARGS]

Commands:

	clean: remove unneeded items from the persistent store - use with caution

		At some point you might want to trim your history to reduce your S3 bill. This is a dangerous way to achieve
		that and I haven't done it in ages. I say it's dangerous because it involves deleting stuff, and that's never
		safe. I won't give cut and paste commands so you'll be force to understand the ins and outs and take
		responsibility. The process is the following.

		- Use the list command to list what's in the remote store at present.
		- Use the history command to extract the range of revisions you want to keep. Mind you, I say "range", because
		if you omit an intermediate revision in the history, the parent chain will be broken and you'll have no access
		to revisions prior to that one, unless you store the revision key somewhere. Also, no instance of musclefs
		should be running, potentially changing the store contents and root pointers. Especially considering you want to
		keep the local root!
		- Feed those revision keys to the reachable command. This will get you all the keys to keep.
		- Use this command (clean) with the two lists of keys (stored, to keep) to prune the remote storage.

		How do you know all is well?

		- Start another musclefs with same config but empty cache.
		- Mount it.
		- ls -lR.
		- Compare with ls -lR of main musclefs (which whould also use cache files that might have been erroneuously
		removed from the remote).

* control

Reads commands line by line from standard input and sends them to
musclefs via its contro file /ctl. It prints responses to standard
output. An example usage is "muco pull | muco" where "muco" is
defined as "fn muco { muscle control $* ; }".

	diff: compare local tree to the remote tree
	history: shows the history of the tree
	init: initializes configuration given the base directory
	list: list all keys in remote store
	reachable: reads a list of line-separated revision keys from standard input and lists all keys reachable from them to standard output

* upload

The “upload” command reads a list of 64-digit hexadecimal keys
from standard input and propagates them from the cache to the
permanent store using many goroutines. This command helps in case
blocks are wrongly deleted from the permanent store, e.g., by
improper use of the “reachable” and “clean” commands. Missing
blocks, which you'd want to propagate from *another* host's cache,
are reported in error messages (but you'll need to front muscle with
a logging 9P proxy such as https://github.com/nicolagi/pine to see
error messages in Linux).

	version: show version information
`, os.Args[0])
	os.Exit(2)
}

func main() {
	cleanFlags := newFlagSet("clean")
	cleanFlags.StringVar(&cleanContext.storedKeys, "stored", "", "`file` listing stored keys - output from muscle list")
	cleanFlags.StringVar(&cleanContext.neededKeys, "needed", "", "`file` listing needed keys - output from muscle reachable")

	diffFlags := newFlagSet("diff")
	diffFlags.BoolVar(&diffContext.verbose, "v", false, "include metadata changes")
	diffFlags.IntVar(&diffContext.context, "U", 3, "number of unified context `lines`")
	diffFlags.BoolVar(&diffContext.names, "N", false, "only output paths that changed, not context diffs")
	diffFlags.StringVar(&diffContext.prefix, "prefix", "", "omit diffs outside of `path`, e.g., project/name")
	diffFlags.IntVar(&diffContext.maxSize, "S", 256*1024, "do not diff nodes larger than `count` bytes")

	// For all commands that don't take flags.
	emptyFlags := newFlagSet("empty")

	// TODO I think instance should be renamed to tree for all these - how to view local vs remote history?
	// TODO I need a glossary

	historyFlags := newFlagSet("history")
	historyFlags.IntVar(&historyContext.context, "U", 3, "number of unified context `lines` (requires -d)")
	historyFlags.BoolVar(&historyContext.diff, "d", false, "show diff between revisions")
	historyFlags.StringVar(&historyContext.prefix, "prefix", "", "omit diffs outside of `path`, e.g., project/name")
	historyFlags.BoolVar(&historyContext.names, "N", false, "Only output paths that changed, not context diffs (requires -d)")
	historyFlags.IntVar(&historyContext.count, "n", 3, "Number of `revisions` to show")
	historyFlags.BoolVar(&historyContext.verbose, "v", false, "include metadata changes (requires -d)")
	historyFlags.IntVar(&historyContext.maxSize, "S", 256*1024, "do not diff nodes larger than `count` bytes")

	// TODO does update encoding work?

	if len(os.Args) < 2 {
		exitUsage("Command name required")
	}

	switch cmd := os.Args[1]; cmd {
	case "clean":
		// Ignoring error - here and in all other cases below - because we configure flag sets to exit on error.
		_ = cleanFlags.Parse(os.Args[2:])
		// Arguments to clean could be positional. It's always going to be 2 required positional arguments. But I'm
		// keeping them as flags so one has to be explicit about what keys need to be preserved and which ones are
		// stored.
		if narg := cleanFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("clean: no args expected, got %d", narg))
		}
		if cleanContext.neededKeys == "" || cleanContext.storedKeys == "" {
			cleanFlags.Usage()
			os.Exit(2)
		}
	case "control":
		_ = emptyFlags.Parse(os.Args[2:])
	case "diff":
		_ = diffFlags.Parse(os.Args[2:])
		if narg := diffFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("diff: no args expected, got %d\n", narg))
		}
	case "history":
		_ = historyFlags.Parse(os.Args[2:])
		if narg := historyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("history: no args expected, got %d\n", narg))
		}
	case "init":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("init: no args expected, got %d", narg))
		}
	case "list":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("list: no args expected, got %d", narg))
		}
	case "mount":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("mount: no args expected, got %d", narg))
		}
	case "reachable":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("reachable: no args expected, got %d", narg))
		}
	case "umount":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("umount: no args expected, got %d", narg))
		}
	case "upload":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("upload: no args expected, got %d", narg))
		}
	case "version":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("version: no args expected, got %d", narg))
		}
	default:
		exitUsage(fmt.Sprintf("%q: command not recognized", cmd))
	}

	logrus.SetFormatter(&logrus.JSONFormatter{})
	ll, err := logrus.ParseLevel(globalContext.logLevel)
	if err != nil {
		log.Fatalf("Could not parse log level %q: %v", globalContext.logLevel, err)
	}
	logrus.SetLevel(ll)

	// The init subcommand is special, because it must create configuration, not use it.
	// Therefore it is handled outside of the big switch statement below.
	if os.Args[1] == "init" {
		if err := config.Initialize(globalContext.base); err != nil {
			log.Fatalf("Could not initialize config in %q: %v", globalContext.base, err)
		}
		return
	}

	cfg, err := config.Load(globalContext.base)
	if err != nil {
		log.Fatalf("Could not load config from %q: %v", globalContext.base, err)
	}

	if os.Args[1] == "mount" || os.Args[1] == "umount" {
		var cmds []string
		if os.Args[1] == "mount" {
			cmds, err = cfg.MountCommands()
		} else {
			cmds, err = cfg.UmountCommands()
		}
		if err != nil {
			log.Fatalf("While getting %s commands: %+v", os.Args[1], err)
		}
		for _, c := range cmds {
			fmt.Println(c)
		}
		os.Exit(0)
	}

	if os.Args[1] == "control" {
		if err := doControl(cfg, os.Args[2:]); err != nil {
			log.Printf("control: %+v", err)
			os.Exit(1)
		} else {
			os.Exit(0)
		}
	}

	stagingStore := storage.NewDiskStore(cfg.StagingDirectoryPath())
	cacheStore := storage.NewDiskStore(cfg.CacheDirectoryPath())
	remoteStore, err := storage.NewStore(cfg)
	if err != nil {
		log.Fatalf("Could not create remote store: %v", err)
	}
	f, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("Could not create temporary file for bugs propagation log: %v", err)
	}
	paired, err := storage.NewPaired(cacheStore, remoteStore, f.Name())
	if err != nil {
		log.Fatalf("Could not start new paired store with log %q: %v", f.Name(), err)
	}
	blockFactory, err := block.NewFactory(stagingStore, paired, cfg.EncryptionKeyBytes())
	if err != nil {
		log.Fatalf("Could not build block factory: %v", err)
	}
	treeStore, err := tree.NewStore(blockFactory, remoteStore, globalContext.base)
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}

	rootKey, err := treeStore.LocalRootKey()
	if os.IsNotExist(err) {
		rootKey = storage.Null
		err = nil
	}
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	localTree, err := tree.NewTree(treeStore, tree.WithRoot(rootKey))
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}

	switch cmd := os.Args[1]; cmd {

	case "clean":
		// TODO enable versioning for bucket containing remote roots
		m := make(map[string]struct{})
		f, err := os.Open(cleanContext.storedKeys)
		if err != nil {
			log.Fatalf("Could not open file containing stored keys %q: %v", cleanContext.storedKeys, err)
		}
		s := bufio.NewScanner(f)
		for s.Scan() {
			m[s.Text()] = struct{}{}
		}
		if err := s.Err(); err != nil {
			log.Fatalf("Error scanning file %q: %v", f.Name(), err)
		}
		_ = f.Close()
		logrus.WithField("count", len(m)).Info("Found stored keys")
		f, err = os.Open(cleanContext.neededKeys)
		if err != nil {
			log.Fatalf("Could not open file containing still needed keys %q: %v", cleanContext.neededKeys, err)
		}
		s = bufio.NewScanner(f)
		for s.Scan() {
			delete(m, s.Text())
		}
		if err := s.Err(); err != nil {
			log.Fatalf("Error scanning file %q: %v", f.Name(), err)
		}
		logrus.WithField("count", len(m)).Info("Found stored keys that are no longer needed")
		i := 0
		for keyHex := range m {
			if keyHex == "base" || strings.HasPrefix(keyHex, tree.RemoteRootKeyPrefix) {
				continue
			}
			key, _ := storage.NewPointerFromHex(keyHex) // TODO handle rror
			// TODO log a warning
			// TODO rethink output
			_ = cacheStore.Delete(key.Key()) // Best effort
			err := remoteStore.Delete(key.Key())
			if err != nil {
				fmt.Print("O")
				logrus.Error(err.Error())
			} else {
				fmt.Print(".")
			}
			i++
			if i%80 == 0 {
				fmt.Print("\n")
			}
		}

	case "diff":
		cmdlog := logrus.WithFields(logrus.Fields{})
		remoteRevisionKey, err := treeStore.RemoteBasePointer()
		if err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not load remote revision key")
		}
		remoteTree, err := tree.NewTree(treeStore, tree.WithRevision(remoteRevisionKey))
		if err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not load remote tree")
		}
		err = tree.DiffTrees(localTree, remoteTree, tree.DiffTreesOutput(os.Stdout),
			tree.DiffTreesInitialPath(diffContext.prefix),
			tree.DiffTreesContext(diffContext.context),
			tree.DiffTreesNamesOnly(diffContext.names),
			tree.DiffTreesVerbose(diffContext.verbose),
			tree.DiffTreesMaxSize(diffContext.maxSize),
		)
		if err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not diff against remote tree")
		}

	case "history":
		pointer, err := treeStore.RemoteBasePointer()
		if err != nil {
			log.Fatalf("could not read base pointer: %+v", err)
		}
		rev, err := treeStore.LoadRevisionByKey(pointer)
		if err != nil {
			log.Fatalf("could not load revision %v: %+v", pointer, err)
		}
		rr, err := treeStore.History(historyContext.count, rev)
		if err != nil {
			log.Printf("history may be truncated: %+v", err)
		}
		for i := 0; i < len(rr); i++ {
			this := rr[i]
			fmt.Println(this)
			if historyContext.diff && i < len(rr)-1 {
				var a, b *tree.Tree
				a, _ = tree.NewTree(treeStore, tree.WithRevision(rr[i+1].Key()))
				if i == 0 && this.Key().IsNull() {
					b, _ = tree.NewTree(treeStore, tree.WithRoot(rev.RootKey()))
				} else {
					b, _ = tree.NewTree(treeStore, tree.WithRevision(this.Key()))
				}
				err := tree.DiffTrees(a, b, tree.DiffTreesOutput(os.Stdout),
					tree.DiffTreesInitialPath(historyContext.prefix),
					tree.DiffTreesContext(historyContext.context),
					tree.DiffTreesNamesOnly(historyContext.names),
					tree.DiffTreesVerbose(historyContext.verbose),
					tree.DiffTreesMaxSize(historyContext.maxSize),
				)
				if err != nil {
					log.Printf("could not diff against remote tree: %+v", err)
				}
				fmt.Println()
			}
		}

	case "list":
		// TODO how does this work with clean and reachable?
		// TODO note about encryption and that it's probably bad
		store, ok := remoteStore.(storage.Lister)
		if !ok {
			log.Fatal("Store does not implement github.com/nicolagi/muscle/storage.Lister.")
		}
		keys, err := store.List()
		if err != nil {
			log.Fatalf("Could not list keys in store: %v", err)
		}
		for key := range keys {
			// Do not print keys that are not hash pointers, e.g., "remote.root.myhost", "extraneous-key", ...
			if _, err := storage.NewPointerFromHex(key); err == nil {
				fmt.Println(key)
			}
		}

	case "reachable":
		cmdlog := logrus.WithField("op", cmd) // TODO adopt pattern for all commands
		m := make(map[string]struct{})
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			cmdlog = cmdlog.WithField("key", s.Text())
			key, err := storage.NewPointerFromHex(s.Text())
			if err != nil {
				cmdlog.WithField("cause", err).Fatal("Could not parse key")
			}
			log.Printf("reachable: examining revision %q", key)
			t, err := tree.NewTree(treeStore, tree.WithRevision(key))
			if err != nil {
				cmdlog.WithField("cause", err).Fatal("Could not construct tree")
			}
			if _, err := t.ReachableKeys(m); err != nil {
				cmdlog.WithField("cause", err).Fatal("Could not find reachable keys")
			}
		}
		if err := s.Err(); err != nil {
			cmdlog.WithField("cause", err).Fatal("Scan error")
		}
		for k := range m {
			fmt.Println(k)
		}

	case "upload":
		doUpload(cacheStore, remoteStore)

	case "version":
		fmt.Println(version)

	default:
		panic("not reached")
	}
}

func doControl(c *config.C, args []string) error {
	user := p.OsUsers.Uid2User(os.Getuid())
	fs, err := clnt.Mount(c.ListenNet, c.ListenAddr, "", 8192, user)
	if err != nil {
		return errors.Wrapf(err, "connecting to %s", c.ListenAddr)
	}
	defer fs.Unmount()
	ctl, err := fs.FOpen("ctl", p.ORDWR)
	if err != nil {
		return errors.Wrap(err, "opening control file")
	}
	defer func() {
		if err := ctl.Close(); err != nil {
			log.Printf("warning: closing control file: %v", err)
		}
	}()

	var s *bufio.Scanner
	if len(args) > 0 {
		s = bufio.NewScanner(strings.NewReader(strings.Join(args, " ")))
	} else {
		s = bufio.NewScanner(os.Stdin)
	}
	for s.Scan() {
		if _, err := ctl.Write(s.Bytes()); err != nil {
			return errors.Wrapf(err, "writing command %q", s.Bytes())
		}
		if _, err := ctl.Seek(0, 0); err != nil {
			return errors.Wrapf(err, "seeking to beginning of control file")
		}
		if response, err := ioutil.ReadAll(ctl); err != nil {
			return errors.Wrapf(err, "reading response for command %q", s.Bytes())
		} else if _, err := os.Stdout.Write(response); err != nil {
			return errors.Wrapf(err, "writing response to standard output for command %q", s.Bytes())
		}
	}
	if err := s.Err(); err != nil {
		return errors.Wrap(err, "scanning input")
	}
	return nil
}

func doUpload(fromStore, toStore storage.Store) {
	completed := uint32(0)
	pending := make(chan storage.Key, 4096)
	uploaders := sync.WaitGroup{}
	// upload runs in a goroutine and uses the three variables above.
	upload := func() {
		for key := range pending {
		get:
			value, err := fromStore.Get(key)
			if err != nil {
				log.Printf("upload: error: Get: %v", err)
				time.Sleep(time.Second)
				goto get
			}
		put:
			err = toStore.Put(key, value)
			if err != nil {
				log.Printf("upload: error: Put: %+v", err)
				time.Sleep(time.Second)
				goto put
			}
			if completedNew := atomic.AddUint32(&completed, 1); completedNew%100 == 0 {
				log.Printf("upload: uploaded %d keys", completedNew)
			}
		}
		uploaders.Done()
	}
	for i := 0; i < 64; i++ {
		uploaders.Add(1)
		go upload()
	}
	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		pending <- storage.Key(s.Text())
	}
	if err := s.Err(); err != nil {
		log.Fatalf("upload: error: could not scan keys from standard input: %v", err)
	}
	close(pending)
	uploaders.Wait()
	log.Printf("upload: uploaded %d keys", completed)
}
