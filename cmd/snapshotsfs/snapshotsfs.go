package main

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/srv"
	"github.com/nicolagi/muscle/internal/block"
	"github.com/nicolagi/muscle/internal/config"
	"github.com/nicolagi/muscle/internal/netutil"
	"github.com/nicolagi/muscle/internal/p9util"
	"github.com/nicolagi/muscle/internal/storage"
	"github.com/nicolagi/muscle/internal/tree"
	log "github.com/sirupsen/logrus"
)

type node interface {
	qid() p.Qid
	stat() p.Dir
	walk(name string) (child node, err error)
	open(r *srv.Req) (qid p.Qid, err error)
	read(r *srv.Req) (n int, err error)
}

type treenode struct {
	tree *tree.Tree
	node *tree.Node
	dirb p9util.DirBuffer
	// If non-empty, overrides the node's nameOverride.
	nameOverride string
}

func (tn *treenode) prepareForReads() {
	tn.dirb.Reset()
	var dir p.Dir
	for _, child := range tn.node.Children() {
		p9util.NodeDirVar(child, &dir)
		tn.dirb.Write(&dir)
	}
}

func (tn *treenode) qid() p.Qid {
	return p9util.NodeQID(tn.node)
}

func (tn *treenode) stat() p.Dir {
	dir := p9util.NodeDir(tn.node)
	if tn.nameOverride != "" {
		dir.Name = tn.nameOverride
	}
	return dir
}

func (tn *treenode) walk(name string) (child node, err error) {
	nodes, err := tn.tree.Walk(tn.node, name)
	if err != nil {
		if errors.Is(err, tree.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return &treenode{
		tree: tn.tree,
		node: nodes[0],
	}, nil
}

func (tn *treenode) open(r *srv.Req) (qid p.Qid, err error) {
	if r.Tc.Mode&(p.OWRITE|p.ORDWR|p.OTRUNC|p.ORCLOSE) != 0 {
		err = srv.Eperm
		return
	}
	switch {
	case tn.node.IsDir():
		if err = tn.tree.Grow(tn.node); err != nil {
			return
		}
		tn.prepareForReads()
	default:
	}
	return tn.qid(), nil
}

func (tn *treenode) read(r *srv.Req) (n int, err error) {
	if tn.node.IsDir() {
		n, err = tn.dirb.Read(r.Rc.Data[:r.Tc.Count], int(r.Tc.Offset))
	} else {
		n, err = tn.node.ReadAt(r.Rc.Data[:r.Tc.Count], int64(r.Tc.Offset))
	}
	return
}

var _ node = (*treenode)(nil)

type rootdir struct {
	dir       p.Dir
	dirb      p9util.DirBuffer
	treeroots map[string]*treenode
	treestore *tree.Store
	loaded    time.Time
}

var _ node = (*rootdir)(nil)

func (root *rootdir) qid() p.Qid { return root.dir.Qid }

func (root *rootdir) stat() p.Dir { return root.dir }

func (root *rootdir) walk(name string) (child node, err error) {
	if n, ok := root.treeroots[name]; ok {
		return n, nil
	}
	// The name can't be looked up.
	// Try to interpret it as a hash pointer pointing to a revision.
	revpointer, err := storage.NewPointerFromHex(name)
	if err != nil {
		return nil, nil
	}
	revtree, err := tree.NewTree(root.treestore, tree.WithRevision(revpointer))
	if err != nil {
		return nil, err
	}
	_, revroot := revtree.Root()
	revnode := &treenode{
		tree:         revtree,
		node:         revroot,
		nameOverride: name,
	}
	root.treeroots[name] = revnode
	root.preparedirentries()
	return revnode, nil
}

func (root *rootdir) open(r *srv.Req) (qid p.Qid, err error) {
	if r.Tc.Mode&(p.OWRITE|p.ORDWR|p.OTRUNC|p.ORCLOSE) != 0 {
		err = srv.Eperm
		return
	}
	if time.Since(root.loaded) > 5*time.Minute {
		if err = root.reload(); err != nil {
			return
		}
		root.loaded = time.Now()
	}
	return root.dir.Qid, err
}

func (root *rootdir) reload() error {
	remotebase, err := root.treestore.RemoteBasePointer()
	if err != nil {
		return err
	}
	remote, err := root.treestore.LoadRevisionByKey(remotebase)
	if err != nil {
		return err
	}
	revisions, err := root.treestore.History(10, remote)
	if err != nil {
		return err
	}
	added := 0
	for _, revision := range revisions {
		revname := revision.Time().Format("2006-01-02T15-04")
		if _, ok := root.treeroots[revname]; ok {
			continue
		}
		revtree, err := tree.NewTree(root.treestore, tree.WithRevision(revision.Key()))
		if err != nil {
			log.Println(err)
			continue
		}
		_, revroot := revtree.Root()
		root.treeroots[revname] = &treenode{
			tree:         revtree,
			node:         revroot,
			nameOverride: revname,
		}
		added++
	}
	if added > 0 {
		root.preparedirentries()
	}
	return nil
}

func (root *rootdir) preparedirentries() {
	root.dirb.Reset()
	var dir p.Dir
	for _, tn := range root.treeroots {
		dir = tn.stat()
		root.dirb.Write(&dir)
	}
}

func (root *rootdir) read(r *srv.Req) (n int, err error) {
	offset := int(r.Tc.Offset)
	count := int(r.Tc.Count)
	return root.dirb.Read(r.Rc.Data[:count], offset)
}

type fs struct {
	root *rootdir
}

var _ srv.ReqOps = (*fs)(nil)

func (fs *fs) Attach(r *srv.Req) {
	if r.Afid != nil {
		r.RespondError(srv.Enoauth)
		return
	}
	r.Fid.Aux = fs.root
	qid := fs.root.qid()
	r.RespondRattach(&qid)
}

func (fs *fs) Stat(r *srv.Req) {
	dir := r.Fid.Aux.(node).stat()
	r.RespondRstat(&dir)
}

func (fs *fs) Wstat(r *srv.Req) {
	r.RespondError(srv.Eperm)
}

func (fs *fs) Create(r *srv.Req) {
	r.RespondError(srv.Eperm)
}

func (fs *fs) Open(r *srv.Req) {
	qid, err := r.Fid.Aux.(node).open(r)
	if err != nil {
		r.RespondError(err)
		return
	}
	r.RespondRopen(&qid, 0)
}

func (fs *fs) Read(r *srv.Req) {
	if err := p.InitRread(r.Rc, r.Tc.Count); err != nil {
		r.RespondError(err)
		return
	}
	n, err := r.Fid.Aux.(node).read(r)
	if err != nil {
		r.RespondError(err)
		return
	}
	p.SetRreadCount(r.Rc, uint32(n))
	r.Respond()
}

func (fs *fs) Write(r *srv.Req) {
	r.RespondError(srv.Eperm)
}

func (fs *fs) Clunk(r *srv.Req) {
	n := r.Fid.Aux.(node)
	if tn, ok := n.(*treenode); ok {
		tn.node.Unref()
	}
	r.RespondRclunk()
}

func (fs *fs) Remove(r *srv.Req) {
	r.RespondError(srv.Eperm)
}

func (fs *fs) Walk(r *srv.Req) {
	if len(r.Tc.Wname) == 0 {
		fs.clone(r)
	} else {
		fs.walk(r)
	}
}

func (fs *fs) clone(r *srv.Req) {
	n := r.Fid.Aux.(node)
	if tn, ok := n.(*treenode); ok {
		tn.node.Ref()
	}
	r.Newfid.Aux = n
	r.RespondRwalk(nil)
}

func (fs *fs) walk(r *srv.Req) {
	n := r.Fid.Aux.(node)

	var qids []p.Qid
	var err error
	for _, name := range r.Tc.Wname {
		if n, err = n.walk(name); n == nil || err != nil {
			break
		} else {
			qids = append(qids, n.qid())
		}
	}
	if err != nil {
		r.RespondError(err)
		return
	}
	if len(qids) == 0 {
		r.RespondError(srv.Enoent)
		return
	}
	if len(qids) == len(r.Tc.Wname) {
		r.Newfid.Aux = n
		if tn, ok := n.(*treenode); ok {
			tn.node.Ref()
		}
	}
	r.RespondRwalk(qids)
}

func main() {
	base := flag.String("base", config.DefaultBaseDirectoryPath, "directory for caches, configuration, logs, etc.")
	var logLevel string
	var levels []string
	for _, l := range log.AllLevels {
		levels = append(levels, l.String())
	}
	flag.StringVar(&logLevel, "verbosity", "info", "sets the log `level`, among "+strings.Join(levels, ", "))
	flag.Parse()

	cfg, err := config.Load(*base)
	if err != nil {
		log.Fatalf("Could not load config from %q: %v", *base, err)
	}
	ll, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Fatalf("Could not parse log level %q: %v", logLevel, err)
	}
	log.SetLevel(ll)

	remoteStore, err := storage.NewStore(cfg)
	if err != nil {
		log.Fatalf("Could not create remote store: %v", err)
	}

	stagingStore := storage.NullStore{}
	cacheStore := storage.NewDiskStore(cfg.CacheDirectoryPath())
	pairedStore, err := storage.NewPaired(cacheStore, remoteStore, "")
	if err != nil {
		log.Fatalf("Could not start new paired store: %v", err)
	}
	blockFactory, err := block.NewFactory(stagingStore, pairedStore, cfg.EncryptionKeyBytes())
	if err != nil {
		log.Fatalf("Could not build block factory: %v", err)
	}
	treestore, err := tree.NewStore(blockFactory, remoteStore, *base)
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}

	root := &rootdir{
		treestore: treestore,
		treeroots: make(map[string]*treenode),
	}
	root.dir.Name = "snapshots"
	root.dir.Mode = 0700 | p.DMDIR
	root.dir.Uid = p9util.NodeUID
	root.dir.Gid = p9util.NodeGID
	root.dir.Mtime = uint32(time.Now().Unix())
	root.dir.Atime = root.dir.Mtime
	root.dir.Qid.Type = p.QTDIR

	fs := fs{
		root: root,
	}

	s := &srv.Srv{}
	s.Dotu = false
	s.Id = "snapshots"
	if !s.Start(&fs) {
		log.Fatal("go9p/p/srv.Srv.Start returned false")
	}
	if listener, err := netutil.Listen(cfg.SnapshotsListenNet, cfg.SnapshotsListenAddr); err != nil {
		log.Fatalf("Could not start net listener: %v", err)
	} else if err := s.StartListener(listener); err != nil {
		log.Fatalf("Could not start 9P listener: %v", err)
	}
}
