// +build plan9

package main

import (
	"os"

	"github.com/nicolagi/go9p/p"
)

type user string

func (user) Id() int { return -1 }

func (username user) Name() string { return string(username) }

func (user) IsMember(p.Group) bool { return false }

func (user) Groups() []p.Group { return nil }

type group string

func (group) Id() int { return -1 }

func (username group) Name() string { return string(username) }

func (group) Members() []p.User { return nil }

type pool string

func (username pool) Gid2Group(int) p.Group {
	return nil
}

func (username pool) Gname2Group(name string) p.Group {
	if string(username) == name {
		return group(username)
	}
	return nil
}

func (username pool) Uid2User(int) p.User {
	return nil
}

func (username pool) Uname2User(name string) p.User {
	if string(username) == name {
		return user(username)
	}
	return nil
}

func usersPool() p.Users {
	return pool(os.Getenv("user"))
}

func gopsListen() {
}
