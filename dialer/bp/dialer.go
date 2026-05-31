package bp

import (
	"context"
	"net"

	"github.com/go-gost/core/dialer"
	"github.com/go-gost/core/logger"
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	"github.com/go-gost/gost/internal/sockopt"
	"github.com/go-gost/x/registry"
)

func init() {
	registry.DialerRegistry().Register("bp", NewDialer)
}

type bpDialer struct {
	md      metadata
	logger  logger.Logger
	options dialer.Options
}

func NewDialer(opts ...dialer.Option) dialer.Dialer {
	options := dialer.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &bpDialer{
		logger:  options.Logger,
		options: options,
	}
}

func (d *bpDialer) Init(m md.Metadata) (err error) {
	return d.parseMetadata(m)
}

func (d *bpDialer) Dial(ctx context.Context, addr string, opts ...dialer.DialOption) (net.Conn, error) {
	var options dialer.DialOptions
	for _, opt := range opts {
		opt(&options)
	}
	raw, err := d.dialRaw(ctx, addr, options)
	if err != nil {
		return nil, err
	}
	cfg := d.md.cfg
	cfg.WarmupMS = 0
	conn, err := busypipe.ClientConn(raw, cfg)
	if err != nil {
		raw.Close()
		return nil, err
	}
	return conn, nil
}

// dialRaw 在底层 TCP 之上打开连接，mptcp=true 时启用 Go 的 MPTCP 客户端模式。
// 详见 dialer/bptls/dialer.go 的同名说明。
func (d *bpDialer) dialRaw(ctx context.Context, addr string, options dialer.DialOptions) (net.Conn, error) {
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
