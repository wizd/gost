//go:build linux

package sockopt

import (
	"net"
	"syscall"
)

const DefaultTCPMaxSeg = 1360

func SetNoDelay(conn net.Conn, enabled bool) error {
	tc := unwrapTCPConn(conn)
	if tc == nil {
		return nil
	}
	return tc.SetNoDelay(enabled)
}

func SetBuffers(conn net.Conn, sndBuf, rcvBuf int) error {
	tc := unwrapTCPConn(conn)
	if tc == nil {
		return nil
	}
	if sndBuf > 0 {
		if err := tc.SetWriteBuffer(sndBuf); err != nil {
			return err
		}
	}
	if rcvBuf > 0 {
		if err := tc.SetReadBuffer(rcvBuf); err != nil {
			return err
		}
	}
	return nil
}

func SetMaxSeg(conn net.Conn, mss int) error {
	if mss <= 0 {
		return nil
	}
	tc := unwrapTCPConn(conn)
	if tc == nil {
		return nil
	}
	rc, err := tc.SyscallConn()
	if err != nil {
		return err
	}
	var sockErr error
	if err := rc.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, mss)
	}); err != nil {
		return err
	}
	return sockErr
}

func ListenConfigControlForMaxSeg(mss int) func(network, address string, c syscall.RawConn) error {
	if mss <= 0 {
		return nil
	}
	return func(network, address string, c syscall.RawConn) error {
		var sockErr error
		if err := c.Control(func(fd uintptr) {
			sockErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, mss)
		}); err != nil {
			return err
		}
		return sockErr
	}
}

func unwrapTCPConn(conn net.Conn) *net.TCPConn {
	for conn != nil {
		if tc, ok := conn.(*net.TCPConn); ok {
			return tc
		}
		nc, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			return nil
		}
		conn = nc.NetConn()
	}
	return nil
}
