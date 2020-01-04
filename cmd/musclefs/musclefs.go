package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/go9p/p/srv"
	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
	log "github.com/sirupsen/logrus"
)

type ops struct {
	instanceID string

	factory   *tree.Factory
	treeStore *tree.Store
	martino   *storage.Martino

	// Serializes access to the tree.
	mu   sync.Mutex
	tree *tree.Tree

	// Control node
	c *ctl

	mergeConflictsPath string
}

var (
	_ srv.ReqOps = (*ops)(nil)
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
	node := r.Fid.Aux.(*tree.Node)
	if len(r.Tc.Wname) == 0 {
		node.Ref("clone", reqfid(r.Newfid))
		r.Newfid.Aux = node
		r.RespondRwalk(nil)
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
		targetNode.Ref("successful walk", reqfid(r.Newfid))
	}
	r.RespondRwalk(qids)
}

func (ops *ops) Open(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*tree.Node)
	switch {
	case node.IsDir():
		if err := ops.tree.Grow(node); err != nil {
			r.RespondError(err)
			return
		}
		node.PrepareForReads()
	default:
		if r.Tc.Mode&p.OTRUNC != 0 {
			ops.tree.Truncate(node, 0)
		}
	}
	r.RespondRopen(&node.D.Qid, 0)
}

func (ops *ops) Create(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	parent := r.Fid.Aux.(*tree.Node)
	var node *tree.Node
	var err error
	node, err = ops.tree.Add(parent, r.Tc.Name, r.Tc.Perm)
	if err != nil {
		r.RespondError(err)
		return
	}
	node.Ref("create", reqfid(r.Fid))
	parent.Unref("created child", reqfid(r.Fid))
	r.Fid.Aux = node
	r.RespondRcreate(&node.D.Qid, 0)
}

func (ops *ops) Read(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*tree.Node)
	p.InitRread(r.Rc, r.Tc.Count)
	var count int
	var err error
	if node.IsDir() {
		count, err = node.DirReadAt(r.Rc.Data[:r.Tc.Count], int64(r.Tc.Offset))
	} else if node.IsController() {
		count = ops.c.read(r.Rc.Data[:r.Tc.Count], int(r.Tc.Offset))
	} else {
		count, err = ops.tree.ReadAt(node, r.Rc.Data[:r.Tc.Count], int64(r.Tc.Offset))
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
	r.Respond()
}

func runCommand(ops *ops, cmd string) error {
	if cmd == "" {
		return nil
	}

	args := strings.Fields(cmd)
	cmd = args[0]
	args = args[1:]

	outputBuffer := bytes.NewBuffer(nil)
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
		historicalTree, err := ops.factory.NewTree(key, true)
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
	case "snapshot":
		// Any additional parents for the snapshot. This is used for merges.
		var parents []storage.Pointer
		for _, arg := range args {
			key, err := storage.NewPointerFromHex(arg)
			if err != nil {
				return fmt.Errorf("%q: %w", arg, err)
			}
			parents = append(parents, key)
		}

		// I. Flush.
		if err := ops.tree.Flush(); err != nil {
			return fmt.Errorf("could not flush: %v", err)
		}
		_, _ = fmt.Fprintln(outputBuffer, "flushed")

		// II. Make the parent the remote root (forgets intermediate local revisions).
		ts := ops.treeStore
		_, localRoot, err := ts.LocalRevision()
		if err != nil {
			return fmt.Errorf("could not get local revision: %v", err)
		}
		revisionKey, err := ts.RemoteRevisionKey("")
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("could not get remote revision: %v", err)
		}
		// DO NOT change order of revision keys or you'll break history.
		// The history code assumes the parent to follow to reconstruct the history of an instance is the last one.
		if revisionKey != nil {
			parents = append(parents, revisionKey)
		}
		revision := tree.NewRevision(ops.instanceID, localRoot.Key(), parents)
		// Need this or reachable keys below won't find the right things...
		if e := ts.PushRevisionLocally(revision); e != nil {
			return e
		}
		ops.tree.SetRevision(revision)

		// III. Mark keys that need to be persisted.
		toPersist, err := ops.tree.ReachableKeysInTheStagingArea()
		if err != nil {
			return fmt.Errorf("could not mark keys to persist: %v", err)
		}
		fmt.Fprintf(outputBuffer, "have to persist %d keys\n", len(toPersist))

		// IV. Commit.
		err = ops.martino.Commit(func(k storage.Key) bool {
			_, found := toPersist[string(k)]
			return found
		})
		if err != nil {
			return fmt.Errorf("could not persist: %v", err)
		}

		// Save remote root pointer.
		if e := ts.PushRevisionRemotely(revision); e != nil {
			return e
		}

		// At this point the revision.key has changed due to
		// re-encryption.
		//
		// (r1,d1) -> (r0,d0) (local)
		// (s1,d1) -> (r0,d0) (remote)
		//
		// If we don't do anything else here, when new revisions are
		// published locally (e.g., flushing via the control file, or
		// automatically every couple minutes), we'll end up with
		//
		// (rn,dn) -> ... -> (r1,d1) -> (r0,d0) (local)
		// (s1,d1) -> (r0,d0) (remote)
		//
		// so the merge base of local and remote would be (r0,d0)
		// rather than (s1,d1).
		//
		// Therefore we update the revision key in the tree to be the one
		// just pushed remotely, which will be the parent for the next
		// local revision. (Note though that only at the next flush will
		// the local revision appear as a child of the remote revision.)
		ops.tree.SetRevision(revision)

	default:
		return fmt.Errorf("command not recognized: %q", cmd)
	}

	ops.c.contents = outputBuffer.Bytes()

	return nil
}

func (ops *ops) Write(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*tree.Node)
	if node.IsController() {
		// Assumption: One Twrite per command.
		if err := runCommand(ops, string(r.Tc.Data)); err != nil {
			r.RespondError(err)
			return
		}
		r.RespondRwrite(uint32(len(r.Tc.Data)))
		return
	}
	if err := ops.tree.WriteAt(node, r.Tc.Data, int64(r.Tc.Offset)); err != nil {
		r.RespondError(err)
		return
	}
	r.RespondRwrite(uint32(len(r.Tc.Data)))
}

func (ops *ops) Clunk(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*tree.Node)
	defer node.Unref("clunk", reqfid(r.Fid))
	if !node.IsDir() {
		err := ops.tree.Release(node)
		if err != nil {
			log.WithFields(log.Fields{
				"path":  node.Path(),
				"cause": err.Error(),
			}).Error("Could not save")
			r.RespondError(srv.Eperm)
			return
		}
	}
	r.RespondRclunk()
}

// TODO I'm done debugging, I should remove this.
func reqfid(*srv.Fid) uint32 {
	return 0xffffffff
	// This can be used when replacing go9p with ../go9p in go.mod
	// return fid.F()
}

func (ops *ops) Remove(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*tree.Node)
	node.Unref("remove", reqfid(r.Fid))
	if node.IsController() {
		r.RespondError(srv.Eperm)
		return
	}
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

func (ops *ops) Stat(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*tree.Node)
	r.RespondRstat(&node.D)
}

func (ops *ops) Wstat(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*tree.Node)
	dir := r.Tc.Dir
	if dir.ChangeLength() {
		if node.IsDir() {
			r.RespondError(srv.Eperm)
			return
		}
		if err := ops.tree.Truncate(node, dir.Length); err != nil {
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
	defer f.Close()

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

	// The paired store starts propagation of blobs from the local to
	// the remote store on the first put operation.  which happens when
	// taking a snapshot (at that time, data moves from the staging area
	// to the paired store, which then propagates from local to remote
	// asynchronously). Since there may be still blobs to propagate
	// from a previous run (i.e., musclefs was killed before all blobs
	// were copied to the remote store) we need to start the background
	// propagation immediately.
	pairedStore.EnsureBackgroundPuts()

	martino := storage.NewMartino(stagingStore, pairedStore)
	treeStore, err := tree.NewStore(martino, remoteBasicStore, cfg.RootKeyFilePath(), tree.RemoteRootKeyPrefix+cfg.Instance, cfg.EncryptionKeyBytes())
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	revisionKey, err := treeStore.LocalRevisionKey()
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	factory := tree.NewFactory(treeStore)
	tt, err := factory.NewTreeForInstance(cfg.Instance, revisionKey)
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}

	ops := &ops{
		factory:            factory,
		treeStore:          treeStore,
		martino:            martino,
		tree:               tt,
		c:                  new(ctl),
		mergeConflictsPath: cfg.ConflictResolutionDirectoryPath(),
	}
	ops.instanceID = cfg.Instance

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
			ops.tree.CreateRevision()
			ops.mu.Unlock()
		}
	}()

	// Now just wait for a signal to do the clean-up.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	for range c {
		log.Info("Final clean-up")
		ops.mu.Lock()
		tt.Flush()
		ops.mu.Unlock()
		break // Allow to exit
	}
}
