//go:build !linux

package sockopt

import (
	"net"
	"syscall"
)

const DefaultTCPMaxSeg = 1360

func SetNoDelay(conn net.Conn, enabled bool) error {
	return nil
}

func SetBuffers(conn net.Conn, sndBuf, rcvBuf int) error {
	return nil
}

func SetMaxSeg(conn net.Conn, mss int) error {
	return nil
}

func ListenConfigControlForMaxSeg(mss int) func(network, address string, c syscall.RawConn) error {
	return nil
}
