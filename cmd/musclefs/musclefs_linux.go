// +build linux

package main

import "github.com/nicolagi/go9p/p"

func usersPool() p.Users {
	return p.OsUsers
}
