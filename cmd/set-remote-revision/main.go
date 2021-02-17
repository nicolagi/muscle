// +build ignore

package main

import (
	"flag"

	log "github.com/sirupsen/logrus"

	"github.com/nicolagi/muscle/internal/config"
	"github.com/nicolagi/muscle/internal/storage"
	"github.com/nicolagi/muscle/internal/tree"
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
	if err := remoteBasicStore.Put(storage.Key(remoteRootKey), []byte(revision)); err != nil {
		log.Fatal(err)
	}
}
