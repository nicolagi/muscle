package netutil

import (
	"net"
	"os"
	"strings"
)

func Listen(network string, address string) (net.Listener, error) {
	if network != "unix" {
		return net.Listen(network, address)
	}
	listener, err := net.Listen(network, address)
	if err != nil && strings.HasSuffix(err.Error(), "bind: address already in use") && !reachable(address) {
		_ = os.Remove(address)
		listener, err = net.Listen(network, address)
	}
	return listener, err
}

func reachable(pathname string) bool {
	conn, err := net.Dial("unix", pathname)
	if conn != nil {
		defer func() { _ = conn.Close() }()
	}
	if err == nil {
		return true
	}
	return !strings.HasSuffix(err.Error(), "connect: connection refused")
}
