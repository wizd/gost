package bptls

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/go-gost/core/dialer"
	"github.com/go-gost/core/logger"
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	"github.com/go-gost/gost/internal/sockopt"
	"github.com/go-gost/x/registry"
)

func init() {
	registry.DialerRegistry().Register("bptls", NewDialer)
}

type bptlsDialer struct {
	md      metadata
	logger  logger.Logger
	options dialer.Options
}

func NewDialer(opts ...dialer.Option) dialer.Dialer {
	options := dialer.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &bptlsDialer{
		logger:  options.Logger,
		options: options,
	}
}

func (d *bptlsDialer) Init(m md.Metadata) (err error) {
	return d.parseMetadata(m)
}

func (d *bptlsDialer) Dial(ctx context.Context, addr string, opts ...dialer.DialOption) (net.Conn, error) {
	var options dialer.DialOptions
	for _, opt := range opts {
		opt(&options)
	}

	raw, err := d.dialRaw(ctx, addr, options)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(raw, d.options.TLSConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	cfg := d.md.cfg
	cfg.WarmupMS = 0
	conn, err := busypipe.ClientConn(tlsConn, cfg)
	if err != nil {
		tlsConn.Close()
		return nil, err
	}
	return conn, nil
}

// dialRaw 在底层 TCP 之上打开连接。
//
// 当 metadata 中 mptcp=true 时，使用本地 net.Dialer 并显式 SetMultipathTCP(true)，
// 让客户端在内核支持 MPTCP 的环境下与启用了 mptcp 的服务端协商出多子流；
// 内核不支持时会回退到普通 TCP 三次握手。该路径不经过 chain 的 options.Dialer，
// 因此会绕过 interface/netns/mark 等扩展能力。需要那些能力时应让 mptcp=false，
// 并由上层 NetDialer 自行注入 MPTCP（当前 NetDialer 未实现）。
func (d *bptlsDialer) dialRaw(ctx context.Context, addr string, options dialer.DialOptions) (net.Conn, error) {
	applySocketOpts := func(raw net.Conn) {
		if err := sockopt.SetMaxSeg(raw, d.md.tcpMaxSeg); err != nil && d.logger != nil {
			d.logger.Debugf("set TCP_MAXSEG failed: %v", err)
		}
		if err := sockopt.SetNoDelay(raw, d.md.noDelay); err != nil && d.logger != nil {
			d.logger.Debugf("set TCP_NODELAY failed: %v", err)
		}
		if err := sockopt.SetBuffers(raw, d.md.sndBuf, d.md.rcvBuf); err != nil && d.logger != nil {
			d.logger.Debugf("set TCP buffers failed: %v", err)
		}
	}

	if !d.md.mptcp {
		conn, err := options.Dialer.Dial(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		applySocketOpts(conn)
		return conn, nil
	}
	var nd net.Dialer
	nd.SetMultipathTCP(true)
	conn, err := nd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	applySocketOpts(conn)
	if d.logger != nil {
		if tc, ok := conn.(*net.TCPConn); ok {
			if used, errUsed := tc.MultipathTCP(); errUsed == nil {
				d.logger.Debugf("mptcp client dial: enabled=%v", used)
			}
		}
	}
	return conn, nil
}
