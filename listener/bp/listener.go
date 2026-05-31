package bp

import (
	"context"
	"net"
	"strings"
	"sync"

	"github.com/go-gost/core/listener"
	"github.com/go-gost/core/logger"
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	"github.com/go-gost/gost/internal/sockopt"
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

	startOnce sync.Once
	closeOnce sync.Once
	connCh    chan net.Conn
	errCh     chan error
	closeCh   chan struct{}
}

func NewListener(opts ...listener.Option) listener.Listener {
	options := listener.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &bpListener{
		logger:  options.Logger,
		options: options,
		connCh:  make(chan net.Conn, 1024),
		errCh:   make(chan error, 1),
		closeCh: make(chan struct{}),
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
	lc.Control = sockopt.ListenConfigControlForMaxSeg(l.md.tcpMaxSeg)
	l.ln, err = lc.Listen(context.Background(), network, l.options.Addr)
	return err
}

func (l *bpListener) Accept() (net.Conn, error) {
	l.startOnce.Do(func() {
		go l.acceptLoop()
	})

	select {
	case conn := <-l.connCh:
		return conn, nil
	case err := <-l.errCh:
		return nil, err
	case <-l.closeCh:
		return nil, net.ErrClosed
	}
}

func (l *bpListener) Addr() net.Addr {
	return l.ln.Addr()
}

func (l *bpListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closeCh)
	})
	return l.ln.Close()
}

func (l *bpListener) acceptLoop() {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			select {
			case <-l.closeCh:
				return
			default:
			}
			l.reportAcceptErr(err)
			return
		}
		go l.handleConn(conn)
	}
}

func (l *bpListener) handleConn(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()
	if err := sockopt.SetMaxSeg(conn, l.md.tcpMaxSeg); err != nil && l.logger != nil {
		l.logger.Debugf("set TCP_MAXSEG failed from %s: %v", remoteAddr, err)
	}
	if err := sockopt.SetNoDelay(conn, l.md.noDelay); err != nil && l.logger != nil {
		l.logger.Debugf("set TCP_NODELAY failed from %s: %v", remoteAddr, err)
	}
	if err := sockopt.SetBuffers(conn, l.md.sndBuf, l.md.rcvBuf); err != nil && l.logger != nil {
		l.logger.Debugf("set TCP buffers failed from %s: %v", remoteAddr, err)
	}

	wrapped, err := busypipe.ServerConn(conn, l.md.cfg)
	if err != nil {
		if l.logger != nil {
			l.logger.Warnf("bp busypipe handshake failed from %s: %v", remoteAddr, err)
		}
		return
	}

	select {
	case l.connCh <- wrapped:
	case <-l.closeCh:
		_ = wrapped.Close()
	}
}

func (l *bpListener) reportAcceptErr(err error) {
	select {
	case l.errCh <- err:
	default:
	}
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
