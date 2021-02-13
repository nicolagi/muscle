package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/gops/agent"
	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/srv"
	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/linuxerr"
	"github.com/nicolagi/muscle/internal/p9util"
	"github.com/nicolagi/muscle/netutil"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	unsupportedModes = map[uint32]error{
		p.DMAPPEND:    fmt.Errorf("append-only files are not supported"),
		p.DMMOUNT:     fmt.Errorf("mounted channels are not supported"),
		p.DMAUTH:      fmt.Errorf("authentication files are not supported"),
		p.DMTMP:       fmt.Errorf("temporary files are not supported"),
		p.DMSYMLINK:   fmt.Errorf("symbolic links are not supported"),
		p.DMLINK:      fmt.Errorf("hard links are not supported"),
		p.DMDEVICE:    fmt.Errorf("device files are not supported"),
		p.DMNAMEDPIPE: fmt.Errorf("named pipes are not supported"),
		p.DMSOCKET:    fmt.Errorf("sockets are not supported"),
		p.DMSETUID:    fmt.Errorf("setuid files are not supported"),
		p.DMSETGID:    fmt.Errorf("setgid files are not supported"),
	}
	knownModes uint32
)

func init() {
	knownModes = 0777 | p.DMDIR | p.DMEXCL
	for mode := range unsupportedModes {
		knownModes |= mode
	}
}

func checkMode(node *tree.Node, mode uint32) error {
	if node != nil {
		if node.IsDir() && mode&p.DMDIR == 0 {
			return fmt.Errorf("a directory cannot become a regular file")
		}
		if !node.IsDir() && mode&p.DMDIR != 0 {
			return fmt.Errorf("a regular file cannot become a directory")
		}
	}
	for bit, err := range unsupportedModes {
		if mode&bit != 0 {
			return err
		}
	}
	if extra := mode &^ knownModes; extra != 0 {
		return fmt.Errorf("unrecognized mode bits: %b", extra)
	}
	return nil
}

type fsNode struct {
	*tree.Node

	dirb p9util.DirBuffer
	lock *nodeLock // Only meaningful for DMEXCL files.
}

func (node *fsNode) prepareForReads() {
	node.dirb.Reset()
	var dir p.Dir
	for _, child := range node.Children() {
		p9util.NodeDirVar(child, &dir)
		node.dirb.Write(&dir)
	}
}

type ops struct {
	pairedStore *storage.Paired
	treeStore   *tree.Store

	// Serializes access to the tree.
	mu      sync.Mutex
	tree    *tree.Tree
	trimmed time.Time

	// Control node
	c *ctl

	cfg *config.C
}

var (
	_ srv.ReqOps = (*ops)(nil)
	_ srv.FidOps = (*ops)(nil)

	Eperm     = "permission denied"
	Eunlinked = "fid points to unlinked node"
)

func logRespondError(r *srv.Req, err string) {
	log.Printf("Rerror: %s", err)
	r.RespondError(err)
}

// ReqProcess implements srv.ReqProcessOps.
// It delegates to the default processing right away, but could be used to add instrumentation for debugging when needed.
func (ops *ops) ReqProcess(r *srv.Req) {
	r.Process()
}

// ReqRespond implements srv.ReqProcessOps.
// It delegates to the default processing right away, but could be used to add instrumentation for debugging when needed.
func (ops *ops) ReqRespond(r *srv.Req) {
	r.PostProcess()
}

func (ops *ops) FidDestroy(fid *srv.Fid) {
	if fid.Aux == nil || fid.Aux == ops.c {
		return
	}
	node := fid.Aux.(*fsNode)
	refs := node.Unref()
	if node.lock != nil {
		unlockNode(node.lock)
		node.lock = nil
	}
	if refs == 0 && node.Unlinked() {
		ops.tree.Discard(node.Node)
	}
}

func (ops *ops) Attach(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	root := ops.tree.Attach()
	root.Ref()
	r.Fid.Aux = &fsNode{Node: root}
	qid := p9util.NodeQID(root)
	r.RespondRattach(&qid)
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
			logRespondError(r, Eperm)
		}
	default:
		node := r.Fid.Aux.(*fsNode)
		if node.Unlinked() {
			logRespondError(r, Eunlinked)
			return
		}
		if len(r.Tc.Wname) == 0 {
			node.Ref()
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
		nodes, err := ops.tree.Walk(node.Node, r.Tc.Wname...)
		if errors.Is(err, tree.ErrNotExist) {
			if len(nodes) == 0 {
				logRespondError(r, "file not found")
				return
			}
			// Clear the error if it was of type "not found" and we could walk at least a node.
			err = nil
		}
		if err != nil {
			logRespondError(r, err.Error())
			return
		}
		var qids []p.Qid
		for _, n := range nodes {
			qids = append(qids, p9util.NodeQID(n))
		}
		if len(qids) == len(r.Tc.Wname) {
			targetNode := nodes[len(nodes)-1]
			r.Newfid.Aux = &fsNode{Node: targetNode}
			targetNode.Ref()
		}
		r.RespondRwalk(qids)
	}
}

func (ops *ops) Open(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if r.Tc.Mode&p.ORCLOSE != 0 {
		logRespondError(r, Eperm)
	}
	switch {
	case r.Fid.Aux == ops.c:
		r.RespondRopen(&ops.c.D.Qid, 0)
	default:
		node := r.Fid.Aux.(*fsNode)
		if node.Unlinked() {
			logRespondError(r, Eunlinked)
			return
		}
		qid := p9util.NodeQID(node.Node)
		if m := moreMode(qid.Path); m&p.DMEXCL != 0 {
			node.lock = lockNode(r.Fid, node.Node)
			if node.lock == nil {
				logRespondError(r, "file already locked")
				return
			}
			qid.Type |= p.QTEXCL
		}
		switch {
		case node.IsDir():
			if err := ops.tree.Grow(node.Node); err != nil {
				logRespondError(r, err.Error())
				return
			}
			node.prepareForReads()
		default:
			if r.Tc.Mode&p.OTRUNC != 0 {
				if err := node.Truncate(0); err != nil {
					logRespondError(r, err.Error())
					return
				}
			}
		}
		r.RespondRopen(&qid, 0)
	}
}

func (ops *ops) Create(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		logRespondError(r, Eperm)
	default:
		parent := r.Fid.Aux.(*fsNode)
		if parent.Unlinked() {
			logRespondError(r, Eunlinked)
			return
		}
		if err := checkMode(nil, r.Tc.Perm); err != nil {
			logRespondError(r, err.Error())
			return
		}
		node, err := ops.tree.Add(parent.Node, r.Tc.Name, r.Tc.Perm)
		if err != nil {
			logRespondError(r, err.Error())
			return
		}
		node.Ref()
		parent.Unref()
		child := &fsNode{Node: node}
		r.Fid.Aux = child
		qid := p9util.NodeQID(node)
		if r.Tc.Perm&p.DMEXCL != 0 {
			setMoreMode(qid.Path, p.DMEXCL)
			child.lock = lockNode(r.Fid, child.Node)
			if child.lock == nil {
				logRespondError(r, "out of locks")
				return
			}
			qid.Type |= p.QTEXCL
		}
		r.RespondRcreate(&qid, 0)
	}
}

func (ops *ops) Read(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if err := p.InitRread(r.Rc, r.Tc.Count); err != nil {
		logRespondError(r, err.Error())
		return
	}
	switch {
	case r.Fid.Aux == ops.c:
		ops.c.D.Atime = uint32(time.Now().Unix())
		count := ops.c.read(r.Rc.Data[:r.Tc.Count], int(r.Tc.Offset))
		p.SetRreadCount(r.Rc, uint32(count))
	default:
		node := r.Fid.Aux.(*fsNode)
		var count int
		var err error
		if node.IsDir() {
			count, err = node.dirb.Read(r.Rc.Data[:r.Tc.Count], int(r.Tc.Offset))
		} else {
			count, err = node.ReadAt(r.Rc.Data[:r.Tc.Count], int64(r.Tc.Offset))
		}
		if err != nil {
			logRespondError(r, err.Error())
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
	case "diff":
		return doDiff(outputBuffer, ops.tree, ops.treeStore, ops.cfg.MuscleFSMount, args)
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
		ops.tree.Ignore(parts[0], parts[1])
		return nil
	case "rename":
		if len(args) != 2 {
			_, _ = fmt.Fprintln(outputBuffer, "Usage: rename SOURCE TARGET")
			return linuxerr.EINVAL
		}
		err := ops.tree.Rename(args[0], args[1])
		if err != nil {
			_, _ = fmt.Fprintf(outputBuffer, "rename: %v\n", err)
			var e linuxerr.E
			if errors.As(err, &e) {
				return e
			}
			return err
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
	case "graft2":
		{
			// Usage: graft2 srcNodeHex/src/path dst/path
			// e.g. graft2 50f6060602543d6825a84ed5b6bd215df6944cf1a41f283a9329d41c2c70c956 tmp/test
			// or graft2 50f6060602543d6825a84ed5b6bd215df6944cf1a41f283a9329d41c2c70c956/foo/bar baz
			// The srcNodeHex can refer to _any_ node, not necessarily a tree root node!
			parts := strings.Split(args[0], "/")
			srcNodeHex := parts[0]
			srcPathElems := parts[1:]
			dstPathElems := strings.Split(args[1], "/")
			dstLeafNodeName := dstPathElems[len(dstPathElems)-1]
			dstReceiverPathElems := dstPathElems[:len(dstPathElems)-1]
			srcNodeKey, err := storage.NewPointerFromHex(srcNodeHex)
			if err != nil {
				return fmt.Errorf("graft2: parse pointer: %v", err)
			}
			srcTree, err := tree.NewTree(ops.treeStore, tree.WithRoot(srcNodeKey))
			if err != nil {
				return fmt.Errorf("graft2: load source tree: %v", err)
			}
			srcRoot := srcTree.Attach()
			var srcLeafNode *tree.Node
			if len(srcPathElems) > 0 {
				wn, err := srcTree.Walk(srcRoot, srcPathElems...)
				if err != nil || len(wn) != len(srcPathElems) {
					return fmt.Errorf("graft2: walk to source: %v", err)
				}
				srcLeafNode = wn[len(wn)-1]
			} else {
				srcLeafNode = srcRoot
			}
			_, dstRoot := ops.tree.Root()
			var dstReceiver *tree.Node
			if len(dstReceiverPathElems) > 0 {
				wn, err := ops.tree.Walk(dstRoot, dstReceiverPathElems...)
				if err != nil {
					return fmt.Errorf("graft2: walk to destination: %v", err)
				}
				dstReceiver = wn[len(wn)-1]
			} else {
				dstReceiver = dstRoot
			}
			fmt.Printf("Grafting %s into %s\n", srcLeafNode, dstReceiver)
			err = ops.tree.Graft(dstReceiver, srcLeafNode, dstLeafNodeName)
			if err != nil {
				log.Printf("graft2: %v", err)
				return srv.Eperm
			}
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
		historicalTree, err := tree.NewTree(ops.treeStore, tree.WithRevision(key))
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
		err = ops.tree.Graft(localParent, historicalChild, localBaseName)
		if err != nil {
			log.WithFields(log.Fields{
				"receiver": localParent,
				"donor":    historicalChild,
				"cause":    err,
			}).Error("Graft failed")
			return srv.Eperm
		}
	case "trim":
		// This, I think, is the only protection against loading large
		// files temporarily. The problem with large files is that they
		// take up a lot of memory and changes the GC target too much. This
		// is the only way to free up that memory.
		_, root := ops.tree.Root()
		root.Trim()
		debug.FreeOSMemory()
	case "flush":
		if err := ops.tree.Flush(); err != nil {
			return fmt.Errorf("could not flush: %v", err)
		}
		_, _ = fmt.Fprintln(outputBuffer, "flushed")
	case "pull":
		localbase, err := ops.treeStore.LocalBasePointer()
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
		localbasetree, err := tree.NewTree(ops.treeStore, tree.WithRevision(localbase))
		if err != nil {
			return output(err)
		}
		remotebasetree, err := tree.NewTree(ops.treeStore, tree.WithRevision(remotebase))
		if err != nil {
			return output(err)
		}
		commands, err := ops.tree.PullWorklog(ops.cfg, localbasetree, remotebasetree)
		if err != nil {
			return output(err)
		}
		if len(commands) == 0 {
			_, _ = fmt.Fprintln(outputBuffer, "no commands to run, pull is a no-op")
			if err := ops.treeStore.SetLocalBasePointer(remotebase); err != nil {
				return output(err)
			}
			return nil
		}
		outputBuffer.WriteString(commands)
		return nil
	case "push":
		localbase, err := ops.treeStore.LocalBasePointer()
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
		revision := tree.NewRevision(localroot, remotebase)
		if err := ops.treeStore.StoreRevision(revision); err != nil {
			return output(err)
		}
		ops.tree.SetRevision(revision)
		_, _ = fmt.Fprintf(outputBuffer, "push: revision created: %s\n", revision.ShortString())

		if err := ops.treeStore.SetRemoteBasePointer(revision.Key()); err != nil {
			return output(err)
		}
		_, _ = fmt.Fprintf(outputBuffer, "push: updated remote base pointer: %v\n", revision.Key())
		if err := ops.treeStore.SetLocalBasePointer(revision.Key()); err != nil {
			return output(err)
		}
		_, _ = fmt.Fprintf(outputBuffer, "push: updated local base pointer: %v\n", revision.Key())
		ops.pairedStore.Notify()
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
			logRespondError(r, err.Error())
			return
		}
		r.RespondRwrite(uint32(len(r.Tc.Data)))
	default:
		node := r.Fid.Aux.(*fsNode)
		if err := node.WriteAt(r.Tc.Data, int64(r.Tc.Offset)); err != nil {
			logRespondError(r, err.Error())
			return
		}
		r.RespondRwrite(uint32(len(r.Tc.Data)))
	}
}

func (ops *ops) Clunk(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if r.Fid.Aux != ops.c {
		node := r.Fid.Aux.(*fsNode)
		if node.lock != nil {
			unlockNode(node.lock)
			node.lock = nil
		}
		if time.Since(ops.trimmed) > time.Minute {
			_, root := ops.tree.Root()
			root.Trim()
			debug.FreeOSMemory()
			ops.trimmed = time.Now()
		}
	}
	r.RespondRclunk()
}

func (ops *ops) Remove(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		logRespondError(r, Eperm)
	default:
		node := r.Fid.Aux.(*fsNode)
		if node.Unlinked() {
			logRespondError(r, Eunlinked)
			return
		}
		err := ops.tree.Unlink(node.Node)
		if err != nil {
			if errors.Is(err, tree.ErrNotEmpty) {
				logRespondError(r, linuxerr.ENOTEMPTY.Error())
			} else {
				logRespondError(r, err.Error())
			}
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
		node := r.Fid.Aux.(*fsNode)
		if node.Unlinked() {
			logRespondError(r, Eunlinked)
			return
		}
		dir := p9util.NodeDir(node.Node)
		if m := moreMode(dir.Qid.Path); m&p.DMEXCL != 0 {
			dir.Mode |= p.DMEXCL
			dir.Qid.Type |= p.QTEXCL
		} else {
			dir.Mode &^= p.DMEXCL
			dir.Qid.Type &^= p.QTEXCL
		}
		r.RespondRstat(&dir)
	}
}

func (ops *ops) Wstat(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	switch {
	case r.Fid.Aux == ops.c:
		logRespondError(r, Eperm)
	default:
		node := r.Fid.Aux.(*fsNode)
		if node.Unlinked() {
			logRespondError(r, Eunlinked)
			return
		}
		dir := r.Tc.Dir
		if dir.ChangeLength() {
			if node.IsDir() {
				logRespondError(r, Eperm)
				return
			}
			if err := node.Truncate(dir.Length); err != nil {
				logRespondError(r, err.Error())
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
			logRespondError(r, Eperm)
			return
		}

		if dir.ChangeName() {
			node.Rename(dir.Name)
		}
		if dir.ChangeMtime() {
			node.Touch(dir.Mtime)
		}

		if dir.ChangeMode() {
			if err := checkMode(node.Node, dir.Mode); err != nil {
				logRespondError(r, err.Error())
				return
			}
			qid := p9util.NodeQID(node.Node)
			if dir.Mode&p.DMEXCL != 0 {
				setMoreMode(qid.Path, p.DMEXCL)
			} else {
				setMoreMode(qid.Path, 0)
			}
			node.SetPerm(dir.Mode & 0777)
		}

		// TODO: Not sure it's best to 'pretend' it works, or fail.
		if dir.ChangeGID() {
			logRespondError(r, Eperm)
			return
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
	// Do NOT turn on agent.ShutdownCleanup.
	// The installed signal handler will call os.Exit, preventing
	// musclefs from doing a clean shutdown, possibly leading
	// to data loss.
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Printf("Could not start gops agent: %v", err)
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	base := flag.String("base", config.DefaultBaseDirectoryPath, "Base directory for configuration, logs and cache files")
	blockSize := flag.Int("fsdiff.blocksize", -1, "Do NOT use this for production file systems.")
	debug := flag.Bool("D", false, "Print 9P dialogs.")
	flag.Parse()
	if *blockSize != -1 {
		log.Printf("Overriding block size to %d bytes.", *blockSize)
		config.BlockSize = uint32(*blockSize)
	}
	cfg, err := config.Load(*base)
	if err != nil {
		log.Fatalf("Could not load config from %q: %v", *base, err)
	}
	log.SetFormatter(&log.JSONFormatter{})

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
	treeStore, err := tree.NewStore(blockFactory, remoteBasicStore, *base)
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	rootKey, err := treeStore.LocalRootKey()
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	tt, err := tree.NewTree(treeStore, tree.WithRoot(rootKey), tree.WithMutable())
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}

	ops := &ops{
		pairedStore: pairedStore,
		treeStore:   treeStore,
		tree:        tt,
		c:           new(ctl),
		cfg:         cfg,
	}

	now := time.Now()
	ops.c.D.Qid.Path = uint64(now.UnixNano())
	ops.c.D.Mode = 0644
	ops.c.D.Mtime = uint32(now.Unix())
	ops.c.D.Atime = ops.c.D.Mtime
	ops.c.D.Name = "ctl"
	ops.c.D.Uid = p9util.NodeUID
	ops.c.D.Gid = p9util.NodeGID

	fs := &srv.Srv{}
	fs.Dotu = false
	fs.Id = "muscle"
	if *debug {
		fs.Debuglevel = srv.DbgPrintFcalls
	}
	if !fs.Start(ops) {
		log.Fatal("go9p/p/srv.Srv.Start returned false")
	}

	go func() {
		if listener, err := netutil.Listen(cfg.ListenNet, cfg.ListenAddr); err != nil {
			log.Fatalf("Could not start net listener: %v", err)
		} else if err := fs.StartListener(listener); err != nil {
			log.Fatalf("Could not start 9P listener: %v", err)
		}
	}()

	// need to be flushed to the disk cache.
	go func() {
		for {
			// XXX
			// This may interfere with fsdiff's crash inducing code!!!
			// Adds non-determinism to the process.
			time.Sleep(tree.SnapshotFrequency)
			ops.mu.Lock()
			// TODO handle all errors - add errcheck to precommit?
			if err := ops.tree.FlushIfNotDoneRecently(); err != nil {
				log.Printf("Could not flush: %v", err)
			}
			ops.mu.Unlock()
		}
	}()

	log.Print("Awaiting a signal to flush and exit.")
	for sig := range sigc {
		log.Printf("Got signal %q, flushing before exiting.", sig)
		ops.mu.Lock()
		if err := tt.Flush(); err != nil {
			log.Printf("Flushing failed, won't quit: %+v", err)
			ops.mu.Unlock()
			continue
		}
		log.Print("Flushed, quitting.")
		ops.mu.Unlock()
		break
	}
	agent.Close()
}
