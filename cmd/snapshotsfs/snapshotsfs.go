package main

import (
	"flag"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/nicolagi/go9p/p"
	"github.com/nicolagi/go9p/p/srv"
	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
)

var (
	treeFactory *tree.Factory
	treeStore   *tree.Store
	qidPath     uint64
	owner       p.User
	group       p.Group
)

type muscleFile struct {
	srv.File
	node *tree.Node
	tree *tree.Tree
}

func newMuscleFile(node *tree.Node, tree *tree.Tree) (*muscleFile, error) {
	mf := new(muscleFile)
	mf.node = node
	mf.tree = tree
	mf.File.Dir = node.D
	mf.Uid = owner.Name()
	mf.Gid = group.Name()
	return mf, nil
}

// Read implements srv.FReadOp.
func (mf *muscleFile) Read(fid *srv.FFid, b []byte, off uint64) (int, error) {
	n, err := mf.tree.ReadAt(mf.node, b, int64(off))
	log.WithFields(log.Fields{
		"requested": len(b),
		"returned":  n,
		"offset":    off,
		"basename":  fid.F.Name,
		"err":       err,
	}).Debug("Read request")
	return n, err
}

type muscleDir struct {
	srv.File
	node *tree.Node
	tree *tree.Tree
}

// Find implements srv.FFindOp.
func (md *muscleDir) Find(child string) (*srv.File, error) {
	if err := md.ensureLoaded(); err != nil {
		return nil, err
	}
	return md.File.Find(child), nil
}

// Open implements srv.FOpenOp.
func (md *muscleDir) Open(*srv.FFid, uint8) error {
	return md.ensureLoaded()
}

func (md *muscleDir) ensureLoaded() error {
	// Whether the children were loaded or not, we need to call grow to ensure child nodes that were trimmed
	// (by code in the tree package) are reloaded.
	if err := md.tree.Grow(md.node); err != nil {
		return err
	}
	// TODO: This is kinda expensive and ugly. Need to keep in sync go9p's FSrv child nodes with those in muscle.
	// Should probably use Srv directly like musclefs does.
	for _, child := range md.node.Children() {
		if known := md.File.Find(child.D.Name); known != nil {
			switch ops := known.Ops.(type) {
			case *muscleDir:
				if ops.node != child {
					ops.node = child
				}
			case *muscleFile:
				if ops.node != child {
					ops.node = child
				}
			default:
				panic("what kinda node is that!")
			}
			continue
		}
		if child.D.Mode&p.DMDIR != 0 {
			d, err := newDirectory(child.D.Name, child, md.tree)
			if err != nil {
				return err
			}
			d.Add(&md.File, child.D.Name, owner, group, 0700|p.DMDIR, d)
		} else {
			d, err := newMuscleFile(child, md.tree)
			if err != nil {
				return err
			}
			d.Add(&md.File, child.D.Name, owner, group, 0700, d)
		}
	}
	return nil
}

func newDirectory(nameOverride string, node *tree.Node, tree *tree.Tree) (*muscleDir, error) {
	md := new(muscleDir)
	md.node = node
	md.tree = tree
	if nameOverride != "" {
		md.Name = nameOverride
	} else {
		md.Name = node.D.Name
	}
	md.Uid = owner.Name()
	md.Gid = group.Name()
	md.Mode = 0700 | p.DMDIR
	md.Qid.Type = p.QTDIR
	md.Qid.Path = qidPath
	qidPath++
	md.Length = node.D.Length
	md.Atime = node.D.Atime
	md.Mtime = node.D.Mtime
	return md, nil
}

type instanceSnapshotsDirectory struct {
	srv.File
	lastRefreshed time.Time
}

func newInstanceSnapshotsDirectory(instanceID string) *instanceSnapshotsDirectory {
	return &instanceSnapshotsDirectory{}
}

func (isd *instanceSnapshotsDirectory) Open(fid *srv.FFid, mode uint8) error {
	if time.Since(isd.lastRefreshed) > 5*time.Minute {
		if err := isd.reload(); err != nil {
			log.Printf("Could not reload instance root: %v", err)
		}
		isd.lastRefreshed = time.Now()
	}
	return nil
}

func (isd *instanceSnapshotsDirectory) reload() error {
	remote, err := treeStore.RemoteRevisionKey(isd.Name)
	if err != nil {
		return err
	}
	remoteRevisions, err := treeStore.History(10, remote)
	if err != nil {
		return err
	}
	for _, revision := range remoteRevisions {
		rootName := revision.Time().Format("2006-01-02T15-04")
		child := isd.File.Find(rootName)
		if child != nil {
			// We know about it already.
			continue
		}

		revisionTree, err := treeFactory.NewTree(revision.Key(), true)
		if err != nil {
			log.Println(err)
			continue
		}
		_, revisionRootNode := revisionTree.Root()
		revisionOysterRootNode, err := newDirectory(rootName, revisionRootNode, revisionTree)
		if err != nil {
			log.Println(err)
			continue
		}
		_ = revisionOysterRootNode.Add(&isd.File, rootName, owner, group, 0700|p.DMDIR, revisionOysterRootNode)
	}
	return nil
}

type rootDir struct {
	srv.File
}

// Find implements srv.FFindOp. It attemps to load a revision tree.
func (root *rootDir) Find(hex string) (*srv.File, error) {
	child := root.File.Find(hex)
	if child != nil {
		return child, nil
	}
	revp, err := storage.NewPointerFromHex(hex)
	if err != nil {
		log.WithField("key", hex).Info("Not a hash pointer")
		return nil, nil
	}
	revisionTree, err := treeFactory.NewTree(revp, true)
	if err != nil {
		log.WithFields(log.Fields{
			"key": hex,
			"err": err,
		}).Info("Could not create tree")
		return nil, nil
	}
	_, revRootKey := revisionTree.Root()
	revRoot, err := newDirectory(hex, revRootKey, revisionTree)
	if err != nil {
		log.WithFields(log.Fields{
			"key": hex,
			"err": err,
		}).Info("Could not create revision root node")
	}
	_ = revRoot.Add(&root.File, hex, owner, group, 0700|p.DMDIR, revRoot)

	return &revRoot.File, nil
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

	owner = p.OsUsers.Uid2User(os.Getuid())
	group = p.OsUsers.Gid2Group(os.Getgid())

	cfg, err := config.Load(*base)
	if err != nil {
		log.Fatalf("Could not load config from %q: %v", *base, err)
	}
	f, err := os.OpenFile(cfg.SnapshotsFSLogFilePath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Fatalf("Could not open log file %q: %v", cfg.SnapshotsFSLogFilePath(), err)
	}
	log.SetOutput(f)
	ll, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Fatalf("Could not parse log level %q: %v", logLevel, err)
	}
	log.SetLevel(ll)
	defer f.Close()

	remoteStore, err := storage.NewStore(cfg)
	if err != nil {
		log.Fatalf("Could not create remote store: %v", err)
	}

	stagingStore := storage.NullStore{}
	cacheStore := storage.NewDiskStore(cfg.CacheDirectoryPath())
	pairedStore, err := storage.NewPaired(cacheStore, remoteStore, os.DevNull)
	if err != nil {
		log.Fatalf("Could not start new paired store with log %q: %v", cfg.PropagationLogFilePath(), err)
	}
	martino := storage.NewMartino(stagingStore, pairedStore)
	treeStore, err = tree.NewStore(martino, remoteStore, cfg.RootKeyFilePath(), tree.RemoteRootKeyPrefix+cfg.Instance, cfg.EncryptionKeyBytes())
	if err != nil {
		log.Fatalf("Could not load tree: %v", err)
	}
	treeFactory = tree.NewFactory(treeStore)

	var root rootDir
	root.Add(nil, "/", owner, group, p.DMDIR|0700, &root)

	for _, instance := range cfg.ReadOnlyInstances {
		d := newInstanceSnapshotsDirectory(instance)
		_ = d.Add(&root.File, instance, owner, group, p.DMDIR|0700, d)
	}

	s := srv.NewFileSrv(&root.File)
	s.Dotu = false
	s.Id = "snapshotsfs"
	s.Start(s)
	if err := s.StartNetListener("tcp", cfg.SnapshotsFSListenAddr()); err != nil {
		log.Fatalf("Could not start net listener: %v", err)
	}
}
