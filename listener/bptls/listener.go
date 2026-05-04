package bptls

import (
	"context"
	"crypto/tls"
	"net"
	"strings"

	"github.com/go-gost/core/listener"
	"github.com/go-gost/core/logger"
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	"github.com/go-gost/x/registry"
)

func init() {
	registry.ListenerRegistry().Register("bptls", NewListener)
}

type bptlsListener struct {
	ln      net.Listener
	logger  logger.Logger
	md      metadata
	options listener.Options
}

func NewListener(opts ...listener.Option) listener.Listener {
	options := listener.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &bptlsListener{
		logger:  options.Logger,
		options: options,
	}
}

func (l *bptlsListener) Init(m md.Metadata) (err error) {
	if err = l.parseMetadata(m); err != nil {
		return err
	}
	network := "tcp"
	if isIPv4(l.options.Addr) {
		network = "tcp4"
	}
	lc := net.ListenConfig{}
	if l.md.mptcp {
		lc.SetMultipathTCP(true)
		l.logger.Debugf("mptcp enabled: %v", lc.MultipathTCP())
	}
	ln, err := lc.Listen(context.Background(), network, l.options.Addr)
	if err != nil {
		return err
	}
	l.ln = tls.NewListener(ln, l.options.TLSConfig)
	return nil
}

func (l *bptlsListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return nil, err
		}
		remoteAddr := conn.RemoteAddr()

		if tc, ok := conn.(*tls.Conn); ok {
			if err := tc.Handshake(); err != nil {
				_ = conn.Close()
				if l.logger != nil {
					l.logger.Warnf("bptls tls handshake failed from %s: %v", remoteAddr, err)
				}
				continue
			}
		}

		wrapped, err := busypipe.ServerConn(conn, l.md.cfg)
		if err != nil {
			if l.logger != nil {
				l.logger.Warnf("bptls busypipe handshake failed from %s: %v", remoteAddr, err)
			}
			continue
		}
		return wrapped, nil
	}
}

func (l *bptlsListener) Addr() net.Addr {
	return l.ln.Addr()
}

func (l *bptlsListener) Close() error {
	return l.ln.Close()
}

func isIPv4(addr string) bool {
	host := addr
	if strings.Contains(addr, ":") {
		if h, _, err := net.SplitHostPort(addr); err == nil {
			host = h
		}
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	return ip != nil && ip.To4() != nil
}
