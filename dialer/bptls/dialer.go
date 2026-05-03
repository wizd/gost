package bptls

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/go-gost/core/dialer"
	"github.com/go-gost/core/logger"
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
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
	raw, err := options.Dialer.Dial(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(raw, d.options.TLSConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	conn, err := busypipe.ClientConn(tlsConn, d.md.cfg)
	if err != nil {
		tlsConn.Close()
		return nil, err
	}
	return conn, nil
}
