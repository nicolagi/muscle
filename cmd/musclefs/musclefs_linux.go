// +build linux

package main

import (
	"github.com/google/gops/agent"
	"github.com/nicolagi/go9p/p"
	log "github.com/sirupsen/logrus"
)

func usersPool() p.Users {
	return p.OsUsers
}

func gopsListen() {
	if err := agent.Listen(agent.Options{
		ShutdownCleanup: true,
	}); err != nil {
		log.Warningf("Could not start gops agent: %v", err)
	}
}
