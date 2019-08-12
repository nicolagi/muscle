// +build ignore

package main

import (
	"flag"

	log "github.com/sirupsen/logrus"

	"github.com/nicolagi/muscle/config"
	"github.com/nicolagi/muscle/storage"
	"github.com/nicolagi/muscle/tree"
)

func main() {
	flag.Parse()
	args := flag.Args()
	instance := args[0] // e.g. plank, accapi
	revision := args[1] // hex of revision
	cfg, err := config.Load(config.DefaultBaseDirectoryPath)
	if err != nil {
		log.Fatalf("Could not load config from %q: %v", config.DefaultBaseDirectoryPath, err)
	}
	remoteBasicStore, err := storage.NewStore(cfg)
	if err != nil {
		log.Fatalf("Could not create remote store: %v", err)
	}
	remoteRootKey := tree.RemoteRootKeyPrefix + instance
	if err := remoteBasicStore.Put(remoteRootKey, []byte(revision)); err != nil {
		log.Fatal(err)
	}
}
