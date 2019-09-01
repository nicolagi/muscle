package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/user"
	"strings"

	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
	log "github.com/sirupsen/logrus"
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
		tree    string
		names   bool
		verbose bool
	}

	historyContext struct {
		rev    string
		prefix string
		count  int
		diff   bool

		// These apply only if diff is true.
		context int
		names   bool
		verbose bool
	}

	mergeContext struct {
		tree string
	}

	mergeBaseContext struct {
		rev1 string
		rev2 string
	}
)

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.StringVar(&globalContext.base, "base", config.DefaultBaseDirectoryPath, "`directory` for caches, configuration, logs, etc.")
	var levels []string
	for _, l := range log.AllLevels {
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

	diff: compare local tree to a remote tree
	history: shows the history of a tree
	init: initializes configuration given the base directory
	list: list all keys in remote store
	merge: generate commands to merge a remote tree into the local one
	merge-base: find the merge base for two revisions

		Unless you're debugging some problem with the merge command, you don't need this.

	reachable: reads a list of line-separated revision keys from standard input and lists all keys reachable from them to standard output
	update-encoding: migrate all nodes to the latest metadata encoding

		Unless you're developing a new metadata encoding, you don't need this. This command must run with no musclefs
		running as it must have exclusive access to the root key file.

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
	case "diff":
		_ = diffFlags.Parse(os.Args[2:])
		if narg := diffFlags.NArg(); narg != 1 {
			exitUsage(fmt.Sprintf("Expected 1 positional argument for the remote tree to diff against, got %d\n", narg))
		}
		diffContext.tree = diffFlags.Arg(0)
	case "history":
		_ = historyFlags.Parse(os.Args[2:])
		if narg := historyFlags.NArg(); narg != 1 {
			exitUsage(fmt.Sprintf("Expected 1 positional argument for the revision to display history for, got %d\n", narg))
		}
		historyContext.rev = historyFlags.Arg(0)
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
	case "merge":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 1 {
			exitUsage(fmt.Sprintf("merge: expected 1 positional argument for the remote tree to merge into the local one, got %d\n", narg))
		}
		mergeContext.tree = emptyFlags.Arg(0)
	case "merge-base":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 2 {
			exitUsage(fmt.Sprintf("merge-base: expected 2 positional arguments (for revisions), got %d\n", narg))
		}
		mergeBaseContext.rev1 = emptyFlags.Arg(0)
		mergeBaseContext.rev2 = emptyFlags.Arg(1)
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
	case "update-encoding":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("update-encoding: no args expected, got %d", narg))
		}
	case "version":
		_ = emptyFlags.Parse(os.Args[2:])
		if narg := emptyFlags.NArg(); narg != 0 {
			exitUsage(fmt.Sprintf("version: no args expected, got %d", narg))
		}
	default:
		exitUsage(fmt.Sprintf("%q: command not recognized", cmd))
	}

	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})
	ll, err := log.ParseLevel(globalContext.logLevel)
	if err != nil {
		log.Fatalf("Could not parse log level %q: %v", globalContext.logLevel, err)
	}
	log.SetLevel(ll)

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

	// Sub-commands mount and umount only require the configuration.
	if os.Args[1] == "mount" {
		u, err := user.Current()
		if err != nil {
			log.Fatalf("Could not get current user: %v", err)
		}
		ofmt := fmt.Sprintf(
			"'trans=tcp,port=%%d,dfltuid=%s,dfltgid=%s,uname=%s,access=any'",
			u.Uid,
			u.Gid,
			u.Username,
		)
		fmt.Println("# Sweep and send as appropriate:")
		fmt.Println("sudo", "mount", cfg.ListenIP, cfg.MuscleFSMount, "-t", "9p", "-o", fmt.Sprintf(ofmt, cfg.ListenPort))
		fmt.Println("sudo", "mount", cfg.SnapshotsFSListenIP, cfg.SnapshotsFSMount, "-t", "9p", "-o", fmt.Sprintf(ofmt, cfg.SnapshotsFSListenPort))
		fmt.Println("9pfuse", cfg.ListenAddress(), cfg.MuscleFSMount)
		fmt.Println("9pfuse", cfg.SnapshotsFSListenAddr(), cfg.SnapshotsFSMount)
		return
	} else if os.Args[1] == "umount" {
		fmt.Println("# Sweep and send as appropriate:")
		fmt.Println("sudo", "umount", cfg.MuscleFSMount)
		fmt.Println("sudo", "umount", cfg.SnapshotsFSMount)
		fmt.Println("fusermount", "-u", cfg.MuscleFSMount)
		fmt.Println("fusermount", "-u", cfg.SnapshotsFSMount)
		return
	}

	stagingStore := storage.NewDiskStore(cfg.StagingDirectoryPath())
	cacheStore := storage.NewDiskStore(cfg.CacheDirectoryPath())
	remoteStore, err := storage.NewStore(cfg)
	if err != nil {
		log.Fatalf("Could not create remote store: %v", err)
	}
	paired, err := storage.NewPaired(cacheStore, remoteStore, cfg.PropagationLogFilePath())
	if err != nil {
		log.Fatalf("Could not start new paired store with log %q: %v", cfg.PropagationLogFilePath(), err)
	}
	// TODO martino is a terrible name. what does this store do? how does it differ from the paired store?
	martino := storage.NewMartino(stagingStore, paired)
	treeStore, err := tree.NewStore(martino, remoteStore, cfg.RootKeyFilePath(), tree.RemoteRootKeyPrefix+cfg.Instance, cfg.EncryptionKeyBytes())
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	revisionKey, err := treeStore.LocalRevisionKey()
	// TODO use new errors.Is()
	if os.IsNotExist(err) {
		revisionKey = storage.Null
		err = nil
	}
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	treeFactory := tree.NewFactory(treeStore)
	localTree, err := treeFactory.NewTree(revisionKey, true)
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
		log.WithField("count", len(m)).Info("Found stored keys")
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
		log.WithField("count", len(m)).Info("Found stored keys that are no longer needed")
		i := 0
		for keyHex := range m {
			key, _ := storage.NewPointerFromHex(keyHex) // TODO handle rror
			// TODO log a warning
			// TODO rethink output
			_ = cacheStore.Delete(key.Key()) // Best effort
			err := remoteStore.Delete(key.Key())
			if err != nil {
				fmt.Print("O")
				log.Error(err.Error())
			} else {
				fmt.Print(".")
			}
			i++
			if i%80 == 0 {
				fmt.Print("\n")
			}
		}

	case "diff":
		cmdlog := log.WithFields(log.Fields{
			"local":  cfg.Instance,
			"remote": diffContext.tree,
		})
		remoteRevisionKey, err := treeStore.RemoteRevisionKey(diffContext.tree)
		if err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not load remote revision key")
		}
		remoteTree, err := treeFactory.NewTree(remoteRevisionKey, true)
		if err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not load remote tree")
		}
		err = tree.DiffTrees(localTree, remoteTree, tree.DiffTreesOutput(os.Stdout),
			tree.DiffTreesInitialPath(diffContext.prefix),
			tree.DiffTreesContext(diffContext.context),
			tree.DiffTreesNamesOnly(diffContext.names),
			tree.DiffTreesVerbose(diffContext.verbose),
		)
		if err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not diff against remote tree")
		}

	case "history":
		cmdlog := log.WithField("op", cmd)
		rev := mustParseRevision(cmdlog, treeStore, historyContext.rev)
		rr, err := treeStore.History(historyContext.count, rev)
		if err != nil {
			cmdlog.WithField("cause", err).Warn("History possibly truncated")
		}
		for i := 0; i < len(rr); i++ {
			this := rr[i]
			fmt.Println(this) // TODO rething output
			if historyContext.diff && i < len(rr)-1 {
				a, _ := treeFactory.NewTree(rr[i+1].Key(), true)
				b, _ := treeFactory.NewTree(this.Key(), true)
				tree.DiffTrees(a, b, tree.DiffTreesOutput(os.Stdout),
					tree.DiffTreesInitialPath(historyContext.prefix),
					tree.DiffTreesContext(historyContext.context),
					tree.DiffTreesNamesOnly(historyContext.names),
					tree.DiffTreesVerbose(historyContext.verbose),
				)
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

	case "merge":
		if mergeContext.tree == cfg.Instance {
			log.Fatalf("Refusing to merge %q onto itself", mergeContext.tree)
		}
		localRevisionKey, err := treeStore.LocalRevisionKey()
		if err != nil {
			log.Fatal(err)
		}
		localTree, err := treeFactory.NewTree(localRevisionKey, true)
		if err != nil {
			log.Fatal(err)
		}
		keepLocalFn, cleanup := tree.MustKeepLocalFn(cfg.ConflictResolutionDirectoryPath())
		defer cleanup()
		err = tree.Merge(keepLocalFn, localTree, mergeContext.tree, treeFactory)
		if err != nil {
			fmt.Printf("There was a fatal error: %v\n", err)
		}

	case "merge-base":
		cmdlog := log.WithField("op", cmd)

		rev1 := mustParseRevision(cmdlog, treeStore, mergeBaseContext.rev1)
		rev2 := mustParseRevision(cmdlog, treeStore, mergeBaseContext.rev2)

		// TODO it would be nice to have the distance too
		mb, err := treeStore.MergeBase(rev1, rev2)
		if err != nil {
			log.WithFields(log.Fields{
				"rev1":  rev1.Hex(),
				"rev2":  rev2.Hex(),
				"cause": err,
			}).Fatal("could not find merge base")
		}
		fmt.Printf("%s:%s\n%s:%s\n%s\n", mergeBaseContext.rev1, rev1.Hex(), mergeBaseContext.rev2, rev2.Hex(), mb.Hex())

	case "reachable":
		cmdlog := log.WithField("op", cmd) // TODO adopt pattern for all commands
		m := make(map[string]struct{})
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			cmdlog = cmdlog.WithField("key", s.Text())
			key, err := storage.NewPointerFromHex(s.Text())
			if err != nil {
				cmdlog.WithField("cause", err).Fatal("Could not parse key")
			}
			t, err := treeFactory.NewTree(key, true)
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

	case "update-encoding":
		cmdlog := log.WithField("op", cmd)
		key, err := treeStore.LocalRevisionKey()
		if err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not get the local revision key")
		}
		cmdlog = cmdlog.WithField("key", key.Hex())
		t, err := treeFactory.NewTree(key, false)
		if err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not load tree")
		}
		if err := t.UpdateEncoding(); err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not update encoding")
		}
		if err := t.CreateRevision(); err != nil {
			cmdlog.WithField("cause", err).Fatal("Could not create new revision pointing to updated metadata")
		}

	case "version":
		fmt.Println(version)

	default:
		panic("not reached")
	}
}

func parseRevision(store *tree.Store, r string) (storage.Pointer, error) {
	if r == "local" {
		return store.LocalRevisionKey()
	}
	key, err := storage.NewPointerFromHex(r)
	if errors.Is(err, storage.ErrNotHashPointer) {
		return store.RemoteRevisionKey(r)
	}
	return key, err
}

func mustParseRevision(logger *log.Entry, store *tree.Store, r string) storage.Pointer {
	key, err := parseRevision(store, r)
	if err != nil {
		logger.WithFields(log.Fields{
			"revision": r,
			"cause":    err,
		}).Fatal("could not parse revision")
	}
	return key
}
