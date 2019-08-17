package netutil

import (
	"net"
	"time"
)

// WaitForListener tries to connect to the given TCP addr and returns
// nil or the last error occurred when trying to dial that addr, in case
// of timeout.
func WaitForListener(addr string, timeout time.Duration) error {
	start := time.Now()
	var lastErr error
	for time.Since(start) < timeout {
		if lastErr = tryDial(addr); lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

func tryDial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		err = conn.Close()
	}
	return err
}
