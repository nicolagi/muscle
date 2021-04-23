package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
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
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/config"
	"github.com/nicolagi/muscle/internal/linuxerr"
	"github.com/nicolagi/muscle/internal/netutil"
	"github.com/nicolagi/muscle/internal/p9util"
	"github.com/nicolagi/muscle/internal/storage"
	"github.com/nicolagi/muscle/internal/tree"
)

var (
	unsupportedModes = map[uint32]error{
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
	knownModes = 0777 | p.DMDIR | p.DMAPPEND | p.DMEXCL
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

type nodeKind int

const (
	controlFile nodeKind = iota
	muscleNode
	syntheticDir
)

type fsNode struct {
	kind       nodeKind
	*tree.Node                  // For muscle nodes.
	dir        p.Dir            // For the control file and synthetic dirs.
	data       []byte           // For the control file.
	children   []*fsNode        // For the synthetic dirs.
	dirb       p9util.DirBuffer // For muscle nodes and synthetic dirs.
	lock       *nodeLock        // Only meaningful for DMEXCL muscle file nodes.
}

func (node *fsNode) prepareForReads() {
	node.dirb.Reset()
	switch node.kind {
	case muscleNode:
		var dir p.Dir
		for _, child := range node.Children() {
			p9util.NodeDirVar(child, &dir)
			node.dirb.Write(&dir)
		}
	case syntheticDir:
		for _, child := range node.children {
			switch child.kind {
			case controlFile:
				node.dirb.Write(&child.dir)
			case muscleNode:
				var dir p.Dir
				p9util.NodeDirVar(child.Node, &dir)
				node.dirb.Write(&dir)
			case syntheticDir:
				node.dirb.Write(&child.dir)
			}
		}
	}
}

type ops struct {
	pairedStore *storage.Paired
	treeStore   *tree.Store

	// Serializes access to the tree.
	mu      sync.Mutex
	tree    *tree.Tree
	trimmed time.Time

	root *fsNode

	cfg *config.C
}

var (
	_ srv.ReqOps = (*ops)(nil)
	_ srv.FidOps = (*ops)(nil)
)

func logRespondError(r *srv.Req, err error) {
	log.Printf("Rerror: %s", err)
	var e linuxerr.E
	if errors.As(err, &e) {
		r.RespondError(e)
	} else {
		r.RespondError(err)
	}
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
	if fid.Aux == nil {
		return
	}
	node := fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile:
	case syntheticDir:
	default:
		refs := node.Unref()
		if node.lock != nil {
			unlockNode(node.lock)
			node.lock = nil
		}
		if refs == 0 && node.Unlinked() {
			ops.tree.Discard(node.Node)
		}
	}
}

func (ops *ops) Attach(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	r.Fid.Aux = ops.root
	r.RespondRattach(&ops.root.dir.Qid)
}

func (ops *ops) clone(r *srv.Req) {
	node := r.Fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile:
		r.Newfid.Aux = node
		r.RespondRwalk(nil)
	case syntheticDir:
		r.Newfid.Aux = node
		r.RespondRwalk(nil)
	default:
		if node.Unlinked() {
			logRespondError(r, linuxerr.ENOENT)
		} else {
			node.Ref()
			r.Newfid.Aux = node
			r.RespondRwalk(nil)
		}
	}
}

func (ops *ops) walk1(node *fsNode, name string) (*fsNode, error) {
	switch node.kind {
	case controlFile:
		return nil, linuxerr.EACCES
	case syntheticDir:
		if name == ".." && node == ops.root {
			return node, nil
		}
		for _, child := range node.children {
			switch child.kind {
			case controlFile, syntheticDir:
				if child.dir.Name == name {
					return child, nil
				}
			default:
				var dir p.Dir
				p9util.NodeDirVar(child.Node, &dir)
				if dir.Name == name {
					return child, nil
				}
			}
		}
		return nil, linuxerr.ENOENT
	default:
		if node.Unlinked() {
			return nil, linuxerr.ENOENT
		}
		walked, err := ops.tree.Walk(node.Node, name)
		if err != nil {
			return nil, err
		}
		if len(walked) != 1 {
			return nil, linuxerr.EPERM // Incorrect, but will do for now.
		}
		return &fsNode{kind: muscleNode, Node: walked[0]}, nil
	}
}

func (ops *ops) walk(r *srv.Req) {
	node := r.Fid.Aux.(*fsNode)
	var child *fsNode
	var err error
	var qids []p.Qid
	for _, name := range r.Tc.Wname {
		child, err = ops.walk1(node, name)
		if err != nil {
			break
		}
		node = child
		switch node.kind {
		case controlFile:
			qids = append(qids, node.dir.Qid)
		case syntheticDir:
			qids = append(qids, node.dir.Qid)
		default:
			qids = append(qids, p9util.NodeQID(node.Node))
		}
	}
	if errors.Is(err, tree.ErrNotExist) || errors.Is(err, linuxerr.ENOENT) {
		if len(qids) == 0 {
			logRespondError(r, linuxerr.ENOENT)
			return
		}
		// Clear the error if it was of type "not found" and we could walk at least a node.
		err = nil
	}
	if err != nil {
		logRespondError(r, err)
		return
	}
	if len(qids) == len(r.Tc.Wname) {
		r.Newfid.Aux = node
		if node.kind == muscleNode {
			node.Ref()
		}
	}
	r.RespondRwalk(qids)
}

func (ops *ops) Walk(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if len(r.Tc.Wname) == 0 {
		ops.clone(r)
	} else {
		ops.walk(r)
	}
}

func (ops *ops) Open(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if r.Tc.Mode&p.ORCLOSE != 0 {
		logRespondError(r, linuxerr.EACCES)
	}
	node := r.Fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile:
		r.RespondRopen(&node.dir.Qid, 0)
	case syntheticDir:
		node.prepareForReads()
		r.RespondRopen(&node.dir.Qid, 0)
	default:
		if node.Unlinked() {
			logRespondError(r, linuxerr.ENOENT)
			return
		}
		qid := p9util.NodeQID(node.Node)
		if qid.Type&p.QTEXCL != 0 {
			node.lock = lockNode(r.Fid, node.Node)
			if node.lock == nil {
				logRespondError(r, fmt.Errorf("file already locked"))
				return
			}
			qid.Type |= p.QTEXCL
		}
		switch {
		case node.IsDir():
			if err := ops.tree.Grow(node.Node); err != nil {
				logRespondError(r, err)
				return
			}
			node.prepareForReads()
		default:
			if r.Tc.Mode&p.OTRUNC != 0 && qid.Type&p.QTAPPEND == 0 {
				if err := node.Truncate(0); err != nil {
					logRespondError(r, err)
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
	parent := r.Fid.Aux.(*fsNode)
	switch parent.kind {
	case controlFile, syntheticDir:
		logRespondError(r, linuxerr.EACCES)
	default:
		if parent.Unlinked() {
			logRespondError(r, linuxerr.ENOENT)
			return
		}
		if err := checkMode(nil, r.Tc.Perm); err != nil {
			logRespondError(r, err)
			return
		}
		node, err := ops.tree.Add(parent.Node, r.Tc.Name, r.Tc.Perm)
		if err != nil {
			logRespondError(r, err)
			return
		}
		node.Ref()
		parent.Unref()
		child := &fsNode{kind: muscleNode, Node: node}
		r.Fid.Aux = child
		qid := p9util.NodeQID(node)
		if r.Tc.Perm&p.DMEXCL != 0 {
			child.lock = lockNode(r.Fid, child.Node)
			if child.lock == nil {
				logRespondError(r, fmt.Errorf("out of locks"))
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
		logRespondError(r, err)
		return
	}
	node := r.Fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile:
		node.dir.Atime = uint32(time.Now().Unix())
		if o := int(r.Tc.Offset); o > len(node.data) {
			p.SetRreadCount(r.Rc, 0)
		} else {
			count := copy(r.Rc.Data[:r.Tc.Count], node.data[o:])
			p.SetRreadCount(r.Rc, uint32(count))
		}
	case syntheticDir:
		node.dir.Atime = uint32(time.Now().Unix())
		count, err := node.dirb.Read(r.Rc.Data[:r.Tc.Count], int(r.Tc.Offset))
		if err != nil {
			logRespondError(r, err)
			return
		}
		p.SetRreadCount(r.Rc, uint32(count))
	default:
		var count int
		var err error
		if node.IsDir() {
			count, err = node.dirb.Read(r.Rc.Data[:r.Tc.Count], int(r.Tc.Offset))
		} else {
			count, err = node.ReadAt(r.Rc.Data[:r.Tc.Count], int64(r.Tc.Offset))
		}
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				err = fmt.Errorf("%v: %w", err, linuxerr.ENODATA)
			}
			logRespondError(r, err)
			return
		}
		p.SetRreadCount(r.Rc, uint32(count))
	}
	r.Respond()
}

func runCommand(ops *ops, controlNode *fsNode, cmd string) error {
	const method = "runCommand"
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
		controlNode.data = outputBuffer.Bytes()
		controlNode.dir.Length = uint64(len(controlNode.data))
	}()

	switch cmd {
	case "diff":
		if err := ops.tree.Flush(); err != nil {
			return fmt.Errorf("could not flush: %v", err)
		}
		return doDiff(outputBuffer, ops.tree, ops.treeStore, ops.cfg.MuscleFSMount, ops.cfg.SnapshotsFSMount, args)
	case "lsof":
		paths := ops.tree.ListNodesInUse()
		sort.Strings(paths)
		for _, path := range paths {
			outputBuffer.WriteString(path)
			outputBuffer.WriteByte(10)
		}
	case "dump":
		ops.tree.DumpNodes(outputBuffer)
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
			return err
		}
	case "unlink":
		usage := func() {
			_, _ = fmt.Fprint(outputBuffer, "Usage: unlink NAME\nNAME is a non-empty path relative to the musclefs root.\n")
		}
		if len(args) != 1 {
			usage()
			return linuxerr.EINVAL
		}
		name := args[0]
		if len(name) == 0 || name[0] == '/' {
			usage()
			return linuxerr.EINVAL
		}
		elems := strings.Split(name, "/")
		_, r := ops.tree.Root()
		nn, err := ops.tree.Walk(r, elems...)
		if err != nil {
			if errors.Is(err, tree.ErrNotExist) {
				return linuxerr.ENOENT
			}
			_, _ = fmt.Fprintln(outputBuffer, err)
			return err
		}
		if len(nn) != len(elems) {
			return linuxerr.ENOENT
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
				return linuxerr.EACCES
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
			return errorf(method, "%v: %w", err, linuxerr.EACCES)
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
		if err := ops.tree.Flush(); err != nil {
			return fmt.Errorf("could not flush: %v", err)
		}
		localbase, err := ops.treeStore.LocalBasePointer()
		if err != nil {
			return output(err)
		}
		tag, err := ops.treeStore.RemoteTag("base")
		if err != nil {
			return output(err)
		}
		if localbase.Equals(tag.Pointer) {
			_, _ = fmt.Fprintln(outputBuffer, "local base matches remote base, pull is a no-op")
			return nil
		}
		var localbasetree *tree.Tree
		if localbase.IsNull() {
			// Assume an empty base tree, e.g., we're sitting on a new “branch.”
			localbasetree, err = tree.NewTree(ops.treeStore)
		} else {
			localbasetree, err = tree.NewTree(ops.treeStore, tree.WithRevision(localbase))
		}
		if err != nil {
			return output(err)
		}
		remotebasetree, err := tree.NewTree(ops.treeStore, tree.WithRevision(tag.Pointer))
		if err != nil {
			return output(err)
		}
		commands, err := ops.tree.PullWorklog(ops.cfg, localbasetree, remotebasetree)
		if err != nil {
			return output(err)
		}

		// XXX It's a bit silly to return a string of concatenated commands
		// and then split it! But let's first see if automatically running
		// commands works out well and simplifies usage, and we'll refactor
		// if so.
		var cs []string
		successful := 0
		for _, c := range strings.Split(commands, "\n") {
			args := strings.Fields(c)
			if len(args) > 0 && (args[0] == "flush" || args[0] == "graft2" || args[0] == "unlink") {
				log.Printf("DEBUG auto-running: %q", c)
				if err := runCommand(ops, controlNode, c); err != nil {
					cs = append(cs, c)
				} else {
					successful++
				}
			} else {
				cs = append(cs, c)
			}
		}
		commands = strings.Join(cs, "\n")

		if len(commands) == 0 || commands == "pull\n" {
			_, _ = fmt.Fprintf(outputBuffer, "# pull successful (%d commands run)\n", successful)
			if err := ops.treeStore.SetLocalBasePointer(tag.Pointer); err != nil {
				return output(err)
			}
			return nil
		} else {
			_, _ = fmt.Fprintf(outputBuffer, "# %d commands were run automatically\n", successful)
		}
		outputBuffer.WriteString(commands)
		return nil
	case "push":
		tagNames := append([]string{"base"}, args...)
		localbase, err := ops.treeStore.LocalBasePointer()
		if err != nil {
			return output(err)
		}
		tags, err := ops.treeStore.RemoteTags(tagNames)
		if err != nil {
			return output(err)
		}
		remotebase := tags[0].Pointer
		if !localbase.Equals(remotebase) {
			return output(fmt.Errorf("local base %v does not match remote base %v, pull first", localbase, remotebase))
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
		revision := tree.NewRevision(localroot, tags)
		if err := ops.treeStore.StoreRevision(revision); err != nil {
			return output(err)
		}
		ops.tree.SetRevision(revision)
		_, _ = fmt.Fprintf(outputBuffer, "push: revision created: %s\n", revision.ShortString())

		if err := ops.treeStore.SetRemoteTags(tagNames, revision.Key()); err != nil {
			return output(err)
		}
		_, _ = fmt.Fprintf(outputBuffer, "push: updated remote tags %v to %v\n", tagNames, revision.Key())

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
	node := r.Fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile:
		node.dir.Mtime = uint32(time.Now().Unix())
		// Assumption: One Twrite per command.
		if err := runCommand(ops, node, string(r.Tc.Data)); err != nil {
			logRespondError(r, err)
			return
		}
		r.RespondRwrite(uint32(len(r.Tc.Data)))
	case syntheticDir:
		logRespondError(r, linuxerr.EACCES)
	default:
		if err := node.WriteAt(r.Tc.Data, int64(r.Tc.Offset)); err != nil {
			logRespondError(r, err)
			return
		}
		r.RespondRwrite(uint32(len(r.Tc.Data)))
	}
}

func (ops *ops) Clunk(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile, syntheticDir:
	default:
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
	node := r.Fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile, syntheticDir:
		logRespondError(r, linuxerr.EACCES)
	default:
		if node.Unlinked() {
			logRespondError(r, linuxerr.ENOENT)
			return
		}
		err := ops.tree.Unlink(node.Node)
		if err != nil {
			if errors.Is(err, tree.ErrNotEmpty) {
				logRespondError(r, linuxerr.ENOTEMPTY)
			} else {
				logRespondError(r, err)
			}
		} else {
			r.RespondRremove()
		}
	}
}

func (ops *ops) Stat(r *srv.Req) {
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile, syntheticDir:
		r.RespondRstat(&node.dir)
	default:
		if node.Unlinked() {
			logRespondError(r, linuxerr.ENOENT)
			return
		}
		dir := p9util.NodeDir(node.Node)
		r.RespondRstat(&dir)
	}
}

func (ops *ops) Wstat(r *srv.Req) {
	const method = "ops.Wstat"
	ops.mu.Lock()
	defer ops.mu.Unlock()
	node := r.Fid.Aux.(*fsNode)
	switch node.kind {
	case controlFile, syntheticDir:
		logRespondError(r, linuxerr.EPERM)
	default:
		dir := r.Tc.Dir
		if dir.ChangeLength() {
			if node.IsDir() {
				logRespondError(r, linuxerr.EACCES)
				return
			}
			eqid := p9util.NodeQID(node.Node)
			if eqid.Type&p.QTAPPEND != 0 {
				logRespondError(r, linuxerr.EPERM)
				return
			}
			if err := node.Truncate(dir.Length); err != nil {
				logRespondError(r, err)
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
			logRespondError(r, errorf(method, "illegal fields for %q: %w", node.Path(), linuxerr.EPERM))
			return
		}

		if dir.ChangeName() {
			if err := node.Rename(dir.Name); err != nil {
				logRespondError(r, err)
				return
			}
		}
		if dir.ChangeMtime() {
			node.Touch(dir.Mtime)
		}

		if dir.ChangeMode() {
			if err := checkMode(node.Node, dir.Mode); err != nil {
				logRespondError(r, err)
				return
			}
			node.SetMode(dir.Mode)
		}

		// TODO: Not sure it's best to 'pretend' it works, or fail.
		if dir.ChangeGID() {
			logRespondError(r, linuxerr.EACCES)
			return
		}

		r.RespondRwstat()
	}
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
	tt, err := tree.NewTree(treeStore, tree.WithRoot(rootKey), tree.WithRootName("live"), tree.WithMutable())
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}

	ops := &ops{
		pairedStore: pairedStore,
		treeStore:   treeStore,
		tree:        tt,
		cfg:         cfg,
	}

	now := time.Now()
	controlNode := &fsNode{
		kind: controlFile,
		dir: p.Dir{
			Name:  "ctl",
			Mode:  0644,
			Uid:   p9util.NodeUID,
			Gid:   p9util.NodeGID,
			Atime: uint32(now.Unix()),
			Mtime: uint32(now.Unix()),
			Qid: p.Qid{
				Path: uint64(now.UnixNano()),
			},
		},
	}

	now = time.Now()
	ops.root = &fsNode{
		kind: syntheticDir,
		dir: p.Dir{
			Name:  "muscle",
			Mode:  p.DMDIR | 0555,
			Uid:   p9util.NodeUID,
			Gid:   p9util.NodeGID,
			Atime: uint32(now.Unix()),
			Mtime: uint32(now.Unix()),
			Qid: p.Qid{
				Type: p.QTDIR,
				Path: uint64(now.UnixNano()),
			},
		},
	}
	ops.root.children = append(ops.root.children, controlNode)

	main := ops.tree.Attach()
	main.Ref()
	ops.root.children = append(ops.root.children, &fsNode{kind: muscleNode, Node: main})

	ops.root.prepareForReads()

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
			// This may interfere with fsdiff's crash inducing code!!!
			// Adds non-determinism to the process.
			time.Sleep(tree.SnapshotFrequency)
			ops.mu.Lock()
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
