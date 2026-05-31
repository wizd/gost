package bptls

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-gost/core/listener"
	"github.com/go-gost/core/logger"
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	"github.com/go-gost/gost/internal/sockopt"
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
	return &bptlsListener{
		logger:  options.Logger,
		options: options,
		connCh:  make(chan net.Conn, 1024),
		errCh:   make(chan error, 1),
		closeCh: make(chan struct{}),
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
	lc.Control = sockopt.ListenConfigControlForMaxSeg(l.md.tcpMaxSeg)
	ln, err := lc.Listen(context.Background(), network, l.options.Addr)
	if err != nil {
		return err
	}
	l.ln = tls.NewListener(ln, l.options.TLSConfig)
	return nil
}

func (l *bptlsListener) Accept() (net.Conn, error) {
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

func (l *bptlsListener) Addr() net.Addr {
	return l.ln.Addr()
}

func (l *bptlsListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closeCh)
	})
	return l.ln.Close()
}

func (l *bptlsListener) acceptLoop() {
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

func (l *bptlsListener) handleConn(conn net.Conn) {
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

	timeout := time.Duration(l.md.cfg.IdleTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = time.Duration(busypipe.DefaultIdleTimeoutMS) * time.Millisecond
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))

	if tc, ok := conn.(*tls.Conn); ok {
		if err := tc.Handshake(); err != nil {
			_ = conn.Close()
			if l.logger != nil {
				l.logger.Warnf("bptls tls handshake failed from %s: %v", remoteAddr, err)
			}
			return
		}
	}

	_ = conn.SetDeadline(time.Time{})

	wrapped, err := busypipe.ServerConn(conn, l.md.cfg)
	if err != nil {
		if l.logger != nil {
			l.logger.Warnf("bptls busypipe handshake failed from %s: %v", remoteAddr, err)
		}
		return
	}

	select {
	case l.connCh <- wrapped:
	case <-l.closeCh:
		_ = wrapped.Close()
	}
}

func (l *bptlsListener) reportAcceptErr(err error) {
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
