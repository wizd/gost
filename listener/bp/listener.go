package bp

import (
	"context"
	"net"
	"strings"

	"github.com/go-gost/core/listener"
	"github.com/go-gost/core/logger"
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	"github.com/go-gost/x/registry"
)

func init() {
	registry.ListenerRegistry().Register("bp", NewListener)
}

type bpListener struct {
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
	return &bpListener{
		logger:  options.Logger,
		options: options,
	}
}

func (l *bpListener) Init(m md.Metadata) (err error) {
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
	l.ln, err = lc.Listen(context.Background(), network, l.options.Addr)
	return err
}

func (l *bpListener) Accept() (net.Conn, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	wrapped, err := busypipe.ServerConn(conn, l.md.cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return wrapped, nil
}

func (l *bpListener) Addr() net.Addr {
	return l.ln.Addr()
}

func (l *bpListener) Close() error {
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
