package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/srv"
	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type ops struct {
	factory   *tree.Factory
	treeStore *tree.Store

	// Serializes access to the tree.
	mu   sync.Mutex
	tree *tree.Tree

	// Control node
	c *ctl

	mergeConflictsPath string
	cfg                *config.C
}

var (
	_ srv.ReqOps = (*ops)(nil)

	Eunlinked error = &p.Error{Err: "fid points to unlinked node", Errornum: p.EINVAL}
)

func (ops *ops) Attach(r *srv.Req) {
	if r.Afid != nil {
		r.RespondError(srv.Enoauth)
	} else {
		ops.mu.Lock()
		defer ops.mu.Unlock()
		root := ops.tree.Attach()
		r.Fid.Aux = root
		r.RespondRattach(&root.D.Qid)
	}
}

func (ops *ops) Walk(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		if len(r.Tc.Wname) == 0 {
			r.Newfid.Aux = ops.c
			r.RespondRwalk(nil)
		} else {
			r.RespondError(srv.Eperm)
		}
	default:
		node := r.Fid.Aux.(*tree.Node)
		if node.Unlinked() {
			r.RespondError(Eunlinked)
			return
		}
		if len(r.Tc.Wname) == 0 {
			node.Ref("clone")
			r.Newfid.Aux = node
			r.RespondRwalk(nil)
			return
		}
		if node.IsRoot() && len(r.Tc.Wname) == 1 && r.Tc.Wname[0] == "ctl" {
			r.Newfid.Aux = ops.c
			r.RespondRwalk([]p.Qid{ops.c.D.Qid})
			return
		}
		// TODO test scenario: nwqids != 0 but < nwname
		nodes, err := ops.tree.Walk(node, r.Tc.Wname...)
		if errors.Is(err, tree.ErrNotFound) {
			if len(nodes) == 0 {
				r.RespondError(srv.Enoent)
				return
			}
			// Clear the error if it was of type "not found" and we could walk at least a node.
			err = nil
		}
		if err != nil {
			log.WithFields(log.Fields{
				"path":  node.Path(),
				"cause": err.Error(),
			}).Error("Could not walk")
			r.RespondError(srv.Eperm)
			return
		}
		var qids []p.Qid
		for _, n := range nodes {
			qids = append(qids, n.D.Qid)
		}
		if len(qids) == len(r.Tc.Wname) {
			targetNode := nodes[len(nodes)-1]
			r.Newfid.Aux = targetNode
			targetNode.Ref("successful walk")
		}
		r.RespondRwalk(qids)
	}
}

func (ops *ops) Open(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		r.RespondRopen(&ops.c.D.Qid, 0)
	default:
		node := r.Fid.Aux.(*tree.Node)
		if node.Unlinked() {
			r.RespondError(Eunlinked)
			return
		}
		switch {
		case node.IsDir():
			if err := ops.tree.Grow(node); err != nil {
				r.RespondError(err)
				return
			}
			node.PrepareForReads()
		default:
			if r.Tc.Mode&p.OTRUNC != 0 {
				if err := node.Truncate(0); err != nil {
					r.RespondError(err)
					return
				}
			}
		}
		r.RespondRopen(&node.D.Qid, 0)
	}
}

func (ops *ops) Create(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		r.RespondError(srv.Eperm)
	default:
		parent := r.Fid.Aux.(*tree.Node)
		if parent.Unlinked() {
			r.RespondError(Eunlinked)
			return
		}
		var node *tree.Node
		var err error
		node, err = ops.tree.Add(parent, r.Tc.Name, r.Tc.Perm)
		if err != nil {
			r.RespondError(err)
			return
		}
		node.Ref("create")
		parent.Unref("created child")
		r.Fid.Aux = node
		r.RespondRcreate(&node.D.Qid, 0)
	}
}

func (ops *ops) Read(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if err := p.InitRread(r.Rc, r.Tc.Count); err != nil {
		r.RespondError(err)
		return
	}
	switch {
	case r.Fid.Aux == ops.c:
		ops.c.D.Atime = uint32(time.Now().Unix())
		count := ops.c.read(r.Rc.Data[:r.Tc.Count], int(r.Tc.Offset))
		p.SetRreadCount(r.Rc, uint32(count))
	default:
		node := r.Fid.Aux.(*tree.Node)
		if node.Unlinked() {
			r.RespondError(Eunlinked)
			return
		}
		var count int
		var err error
		if node.IsDir() {
			count, err = node.DirReadAt(r.Rc.Data[:r.Tc.Count], int64(r.Tc.Offset))
		} else {
			count, err = node.ReadAt(r.Rc.Data[:r.Tc.Count], int64(r.Tc.Offset))
		}
		if err != nil {
			log.WithFields(log.Fields{
				"path":  node.Path(),
				"cause": err.Error(),
			}).Error("Could not read")
			r.RespondError(srv.Eperm)
			return
		}
		p.SetRreadCount(r.Rc, uint32(count))
	}
	r.Respond()
}

func runCommand(ops *ops, cmd string) error {
	args := strings.Fields(cmd)
	if len(args) == 0 {
		return nil
	}
	cmd = args[0]
	args = args[1:]

	outputBuffer := bytes.NewBuffer(nil)

	// A helper function to return an error, and also add it to the output.
	output := func(err error) error {
		_, _ = fmt.Fprintf(outputBuffer, "%+v", err)
		return err
	}

	// Ensure the output is available even in the case of an early error return.
	defer func() {
		ops.c.contents = outputBuffer.Bytes()
		ops.c.D.Length = uint64(len(ops.c.contents))
	}()

	switch cmd {
	case "level":
		if err := setLevel(args[0]); err != nil {
			return err
		}
	case "lsof":
		paths := ops.tree.ListNodesInUse()
		sort.Strings(paths)
		for _, path := range paths {
			outputBuffer.WriteString(path)
			outputBuffer.WriteByte(10)
		}
	case "dump":
		ops.tree.DumpNodes()
	case "keep-local-for":
		parts := strings.SplitN(args[0], "/", 2)
		return tree.KeepLocalFor(ops.mergeConflictsPath, parts[0], parts[1])
	case "rename":
		err := ops.tree.Rename(args[0], args[1])
		if err != nil {
			return fmt.Errorf("could not rename %q to %q: %v", args[0], args[1], err)
		}
	case "unlink":
		if len(args) == 0 {
			return errors.New("missing argument to unlink")
		}
		elems := strings.Split(args[0], "/")
		if len(elems) == 0 {
			return errors.Errorf("not enough elements in path: %v", args[0])
		}
		_, r := ops.tree.Root()
		nn, err := ops.tree.Walk(r, elems...)
		if err != nil {
			return errors.Wrapf(err, "could not walk the local tree along %v", elems)
		}
		if len(nn) != len(elems) {
			return errors.Errorf("walked %d path elements, required %d", len(nn), len(elems))
		}
		return ops.tree.RemoveForMerge(nn[len(nn)-1])
	case "graft":
		parts := strings.Split(args[0], "/")
		revision := parts[0]
		historicalPath := parts[1:]
		localPath := strings.Split(args[1], "/")
		localBaseName := localPath[len(localPath)-1]
		localPath = localPath[:len(localPath)-1]
		_, _ = fmt.Fprintf(outputBuffer, "Grafting the node identified by the path elements %v from the revision %q into the local tree by walking the path elements %v\n",
			historicalPath, revision, localPath)
		key, err := storage.NewPointerFromHex(revision)
		if err != nil {
			return fmt.Errorf("%q: %w", revision, err)
		}
		historicalTree, err := ops.factory.NewTree(ops.factory.WithRevisionKey(key))
		if err != nil {
			return fmt.Errorf("could not load tree %q: %v", revision, err)
		}
		historicalRoot := historicalTree.Attach()
		hNodes, err := historicalTree.Walk(historicalRoot, historicalPath...)
		if err != nil {
			return fmt.Errorf("could not walk tree %q along nodes %v: %v", revision, historicalPath, err)
		}
		_, localRoot := ops.tree.Root()
		lNodes, err := ops.tree.Walk(localRoot, localPath...)
		if err != nil {
			return fmt.Errorf("could not walk the local tree along %v: %v", localPath, err)
		}
		// TODO also check lnodes is all the names
		localParent := localRoot
		if len(lNodes) > 0 {
			localParent = lNodes[len(lNodes)-1]
		}
		historicalChild := hNodes[len(hNodes)-1]

		fmt.Printf("Attempting graft of %s into %s\n", historicalChild, localParent)
		historicalChild.D.Name = localBaseName
		err = ops.tree.Graft(localParent, historicalChild)
		if err != nil {
			log.WithFields(log.Fields{
				"receiver": localParent,
				"donor":    historicalChild,
				"cause":    err,
			}).Error("Graft failed")
			return srv.Eperm
		}
	case "trim":
		_, root := ops.tree.Root()
		root.Trim()
	case "flush":
		if err := ops.tree.Flush(); err != nil {
			return fmt.Errorf("could not flush: %v", err)
		}
		_, _ = fmt.Fprintln(outputBuffer, "flushed")
	case "pull":
		localbase, err := tree.LocalBasePointer()
		if err != nil {
			return output(err)
		}
		remotebase, err := ops.treeStore.RemoteBasePointer()
		if err != nil {
			return output(err)
		}
		if localbase.Equals(remotebase) {
			_, _ = fmt.Fprintln(outputBuffer, "local base matches remote base, pull is a no-op")
			return nil
		}
		localbasetree, err := ops.factory.NewTree(ops.factory.WithRevisionKey(localbase))
		if err != nil {
			return output(err)
		}
		remotebasetree, err := ops.factory.NewTree(ops.factory.WithRevisionKey(remotebase))
		if err != nil {
			return output(err)
		}
		keep, cleanup := tree.MustKeepLocalFn(ops.mergeConflictsPath)
		defer cleanup()
		commands, err := ops.tree.PullWorklog(keep, ops.cfg, localbasetree, remotebasetree)
		if err != nil {
			return output(err)
		}
		if len(commands) == 0 {
			_, _ = fmt.Fprintln(outputBuffer, "no commands to run, pull is a no-op")
			if err := tree.SetLocalBasePointer(remotebase); err != nil {
				return output(err)
			}
			return nil
		}
		outputBuffer.WriteString(commands)
		return nil
	case "push":
		localbase, err := tree.LocalBasePointer()
		if err != nil {
			return output(err)
		}
		remotebase, err := ops.treeStore.RemoteBasePointer()
		if err != nil {
			return output(err)
		}
		if !localbase.Equals(remotebase) {
			return output(errors.Errorf("local base %v does not match remote base %v, pull first", localbase, remotebase))
		}
		_, _ = fmt.Fprintln(outputBuffer, "local base matches remote base, push allowed")

		if err := ops.tree.Flush(); err != nil {
			return output(err)
		}
		_, _ = fmt.Fprintln(outputBuffer, "push: flushed")

		if err := ops.tree.Seal(); err != nil {
			return output(err)
		}
		_, _ = fmt.Fprintln(outputBuffer, "push: sealed")

		_, localroot := ops.tree.Root()
		revision := tree.NewRevision(localroot.Key(), remotebase)
		if err := ops.treeStore.StoreRevision(revision); err != nil {
			return output(err)
		}
		ops.tree.SetRevision(revision)
		_, _ = fmt.Fprintf(outputBuffer, "push: revision created: %s\n", revision.ShortString())

		if err := ops.treeStore.SetRemoteBasePointer(revision.Key()); err != nil {
			return output(err)
		}
		_, _ = fmt.Fprintf(outputBuffer, "push: updated remote base pointer: %v\n", revision.Key())
		if err := tree.SetLocalBasePointer(revision.Key()); err != nil {
			return output(err)
		}
		_, _ = fmt.Fprintf(outputBuffer, "push: updated local base pointer: %v\n", revision.Key())
		return nil
	default:
		return fmt.Errorf("command not recognized: %q", cmd)
	}

	return nil
}

func (ops *ops) Write(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		ops.c.D.Mtime = uint32(time.Now().Unix())
		// Assumption: One Twrite per command.
		if err := runCommand(ops, string(r.Tc.Data)); err != nil {
			r.RespondError(err)
			return
		}
		r.RespondRwrite(uint32(len(r.Tc.Data)))
	default:
		node := r.Fid.Aux.(*tree.Node)
		if node.Unlinked() {
			r.RespondError(Eunlinked)
			return
		}
		if err := node.WriteAt(r.Tc.Data, int64(r.Tc.Offset)); err != nil {
			r.RespondError(err)
			return
		}
		r.RespondRwrite(uint32(len(r.Tc.Data)))
	}
}

func (ops *ops) Clunk(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
	default:
		node := r.Fid.Aux.(*tree.Node)
		/*  Respond with Rclunk even if unlinked. Caller won't care. */
		defer node.Unref("clunk")
	}
	r.RespondRclunk()
}

func (ops *ops) Remove(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		r.RespondError(srv.Eperm)
	default:
		node := r.Fid.Aux.(*tree.Node)
		if node.Unlinked() {
			r.RespondError(Eunlinked)
			return
		}
		node.Unref("remove")
		err := ops.tree.Remove(node)
		if err != nil {
			log.WithFields(log.Fields{
				"path":  node.Path(),
				"cause": err.Error(),
			}).Warning("Could not remove")
			r.RespondError(srv.Eperm)
		} else {
			r.RespondRremove()
		}
	}
}

func (ops *ops) Stat(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		r.RespondRstat(&ops.c.D)
	default:
		node := r.Fid.Aux.(*tree.Node)
		if node.Unlinked() {
			r.RespondError(Eunlinked)
			return
		}
		r.RespondRstat(&node.D)
	}
}

func (ops *ops) Wstat(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		r.RespondError(srv.Eperm)
	default:
		node := r.Fid.Aux.(*tree.Node)
		if node.Unlinked() {
			r.RespondError(Eunlinked)
			return
		}
		dir := r.Tc.Dir
		if dir.ChangeLength() {
			if node.IsDir() {
				r.RespondError(srv.Eperm)
				return
			}
			if err := node.Truncate(dir.Length); err != nil {
				log.WithFields(log.Fields{
					"cause": err,
				}).Error("Could not truncate")
				r.RespondError(srv.Eperm)
				return
			}
		}

		// From the documentation: "ChangeIllegalFields returns true
		// if Dir contains values that would request illegal fields to
		// be changed; these are type, dev, and qid. The size field is
		// ignored because it's not kept intact.  Any 9p server should
		// return error to a Wstat request when this method returns true."
		// Unfortunately (at least mounting on Linux using the 9p module)
		// rename operations issue Wstat calls with non-empty muid.
		// Since we need renames to work, let's just discard the muid
		// if present. Also, Linux tries to set the atime.  In order not to
		// fail commands such as touch, we'll ignore those also.
		dir.Atime = ^uint32(0)
		dir.Muid = ""
		if dir.ChangeIllegalFields() {
			log.WithFields(log.Fields{
				"path": node.Path(),
				"dir":  dir,
				"qid":  dir.Qid,
			}).Warning("Trying to change illegal fields")
			r.RespondError(srv.Eperm)
			return
		}

		if dir.ChangeName() {
			node.Rename(dir.Name)
		}
		if dir.ChangeMtime() {
			node.SetMTime(dir.Mtime)
		}

		if dir.ChangeMode() {
			node.SetMode(dir.Mode)
		}

		// Owner and group for files stored in a muscle tree are those
		// of the user/group that started the file server.  We allow to
		// change the GID but such change won't be persisted.
		if dir.ChangeGID() {
			node.D.Gid = dir.Gid
		}

		r.RespondRwstat()
	}
}

func setLevel(level string) error {
	ll, err := log.ParseLevel(level)
	if err != nil {
		return err
	}
	log.SetLevel(ll)
	return nil
}

func main() {
	// Dummy in Plan 9. The gops agent can be made to listen on
	// "$sysname:0", but there's no point since the executable gops
	// can't be built.
	gopsListen()

	base := flag.String("base", config.DefaultBaseDirectoryPath, "Base directory for configuration, logs and cache files")
	flag.Parse()
	cfg, err := config.Load(*base)
	if err != nil {
		log.Fatalf("Could not load config from %q: %v", *base, err)
	}

	f, err := os.OpenFile(cfg.MuscleFSLogFilePath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Fatalf("Could not open log file %q: %v", cfg.MuscleFSLogFilePath(), err)
	}
	log.SetOutput(f)
	log.SetFormatter(&log.JSONFormatter{})
	defer func() { _ = f.Close() }()

	remoteBasicStore, err := storage.NewStore(cfg)
	if err != nil {
		log.Fatalf("Could not create remote store: %v", err)
	}

	stagingStore := storage.NewDiskStore(cfg.StagingDirectoryPath())
	cacheStore := storage.NewDiskStore(cfg.CacheDirectoryPath())
	pairedStore, err := storage.NewPaired(cacheStore, remoteBasicStore, cfg.PropagationLogFilePath())
	if err != nil {
		log.Fatalf("Could not start new paired store with log %q: %v", cfg.PropagationLogFilePath(), err)
	}

	// The paired store starts propagation of blocks from the local to
	// the remote store on the first put operation.  which happens when
	// taking a snapshot (at that time, data moves from the staging area
	// to the paired store, which then propagates from local to remote
	// asynchronously). Since there may be still blocks to propagate
	// from a previous run (i.e., musclefs was killed before all blocks
	// were copied to the remote store) we need to start the background
	// propagation immediately.
	pairedStore.EnsureBackgroundPuts()

	blockFactory, err := block.NewFactory(stagingStore, pairedStore, cfg.EncryptionKeyBytes())
	if err != nil {
		log.Fatalf("Could not build block factory: %v", err)
	}
	treeStore, err := tree.NewStore(blockFactory, remoteBasicStore, cfg.RootKeyFilePath())
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	rootKey, err := treeStore.LocalRootKey()
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	factory := tree.NewFactory(blockFactory, treeStore, cfg)
	tt, err := factory.NewTree(factory.WithRootKey(rootKey), factory.Mutable())
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}

	ops := &ops{
		factory:            factory,
		treeStore:          treeStore,
		tree:               tt,
		c:                  new(ctl),
		mergeConflictsPath: cfg.ConflictResolutionDirectoryPath(),
		cfg:                cfg,
	}

	_, root := tt.Root()
	now := time.Now()
	ops.c.D.Qid.Path = uint64(now.UnixNano())
	ops.c.D.Mode = 0644
	ops.c.D.Mtime = uint32(now.Unix())
	ops.c.D.Atime = ops.c.D.Mtime
	ops.c.D.Name = "ctl"
	ops.c.D.Uid = root.D.Uid
	ops.c.D.Gid = root.D.Gid

	/* Best-effort clean-up, for when the control file used to be part of the tree. */
	if nodes, err := ops.tree.Walk(root, "ctl"); err == nil && len(nodes) == 1 {
		_ = ops.tree.Remove(nodes[0])
	}

	if err := os.MkdirAll(ops.mergeConflictsPath, 0755); err != nil {
		log.WithField("cause", err).Fatal("could not ensure conflicts directory exists")
	}

	fs := &srv.Srv{}
	fs.Upool = usersPool() // Platform dependent
	fs.Dotu = false
	fs.Id = "muscle"
	if !fs.Start(ops) {
		log.Fatal("Can't start file server")
	}

	go func() {
		if err := fs.StartNetListener("tcp", cfg.ListenAddress()); err != nil {
			log.Fatal(err)
		}
	}()

	// need to be flushed to the disk cache.
	go func() {
		for {
			time.Sleep(tree.SnapshotFrequency)
			ops.mu.Lock()
			// TODO handle all errors - add errcheck to precommit?
			if err := ops.tree.FlushIfNotDoneRecently(); err != nil {
				log.Printf("Could not flush: %v", err)
			}
			ops.mu.Unlock()
		}
	}()

	// Now just wait for a signal to do the clean-up.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	for range c {
		log.Info("Final clean-up")
		ops.mu.Lock()
		if err := tt.Flush(); err != nil {
			log.Printf("Could not flush: %v", err)
			ops.mu.Unlock()
			continue
		}
		ops.mu.Unlock()
		break
	}
}
