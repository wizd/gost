package busypipe

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestCodecRoundTrip(t *testing.T) {
	c := NewCodec(1400)
	raw, err := c.Encode(FrameDATA, []byte("hello"), 0)
	if err != nil {
		t.Fatal(err)
	}
	f, err := c.ReadFrame(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if f.Type != FrameDATA {
		t.Fatalf("unexpected frame type: %v", f.Type)
	}
	if string(f.Payload) != "hello" {
		t.Fatalf("unexpected payload: %q", string(f.Payload))
	}
}

func TestMixedBuildParse(t *testing.T) {
	m := NewMixedBuilder(8)
	p, err := m.Build([]byte("abc"), 64)
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.Parse(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "abc" {
		t.Fatalf("unexpected mixed payload: %q", string(out))
	}
}

func TestSchedulerTarget(t *testing.T) {
	s := NewMinRateScheduler(8000, 250)
	if got := s.TargetBytesPerTick(); got != 250 {
		t.Fatalf("target bytes mismatch: got=%d want=250", got)
	}
}

func TestSchedulerDeficit(t *testing.T) {
	s := NewMinRateScheduler(8000, 250)
	if got := s.Deficit(); got != 250 {
		t.Fatalf("initial deficit mismatch: got=%d want=250", got)
	}
	s.RecordSent(100)
	if got := s.Deficit(); got != 150 {
		t.Fatalf("deficit after 100 bytes mismatch: got=%d want=150", got)
	}
	s.RecordSent(200)
	if got := s.Deficit(); got != 0 {
		t.Fatalf("deficit should floor at zero: got=%d", got)
	}
}

func TestConnSendReceive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	cfg := DefaultConfig()
	cfg.TickMS = 100
	cfg.IdleTimeoutMS = 5000
	cfg.WarmupMS = 0

	serverErr := make(chan error, 1)
	serverReady := make(chan *Conn, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		c, err := ServerConn(raw, cfg)
		if err != nil {
			serverErr <- err
			return
		}
		serverReady <- c
	}()

	clientRaw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientRaw.Close()

	clientConn, err := ClientConn(clientRaw, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	var serverConn *Conn
	select {
	case err := <-serverErr:
		t.Fatal(err)
	case serverConn = <-serverReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server conn handshake timeout")
	}
	defer serverConn.Close()

	_ = clientConn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := clientConn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	_ = serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(serverConn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("unexpected data: %q", string(buf))
	}
}

func TestConnHighRateDirectData(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	cfg := DefaultConfig()
	cfg.TickMS = 250
	cfg.IdleTimeoutMS = 5000
	cfg.WarmupMS = 0

	serverErr := make(chan error, 1)
	serverReady := make(chan *Conn, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		c, err := ServerConn(raw, cfg)
		if err != nil {
			serverErr <- err
			return
		}
		serverReady <- c
	}()

	clientRaw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientRaw.Close()

	clientConn, err := ClientConn(clientRaw, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	var serverConn *Conn
	select {
	case err := <-serverErr:
		t.Fatal(err)
	case serverConn = <-serverReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server conn handshake timeout")
	}
	defer serverConn.Close()

	payload := bytes.Repeat([]byte("a"), 1500)
	_ = clientConn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := clientConn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if got := clientConn.scheduler.Deficit(); got != 0 {
		t.Fatalf("deficit should be zero after high-rate write, got=%d", got)
	}

	_ = serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(serverConn, buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatalf("unexpected payload length/content: got=%d want=%d", len(buf), len(payload))
	}
}

func TestConnWarmupBlocksRealData(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	cfg := DefaultConfig()
	cfg.TickMS = 50
	cfg.IdleTimeoutMS = 5000
	cfg.WarmupMS = 200

	serverErr := make(chan error, 1)
	serverReady := make(chan *Conn, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		c, err := ServerConn(raw, cfg)
		if err != nil {
			serverErr <- err
			return
		}
		serverReady <- c
	}()

	clientRaw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientRaw.Close()

	clientConn, err := ClientConn(clientRaw, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	var serverConn *Conn
	select {
	case err := <-serverErr:
		t.Fatal(err)
	case serverConn = <-serverReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server conn handshake timeout")
	}
	defer serverConn.Close()

	start := time.Now()
	_ = clientConn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := clientConn.Write([]byte("warm")); err != nil {
		t.Fatal(err)
	}

	_ = serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(serverConn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "warm" {
		t.Fatalf("unexpected data: %q", string(buf))
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("warmup should delay data delivery, elapsed=%s", elapsed)
	}
}

func TestConnWarmupRespectsClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	cfg := DefaultConfig()
	cfg.TickMS = 50
	cfg.IdleTimeoutMS = 5000
	cfg.WarmupMS = 500

	serverErr := make(chan error, 1)
	serverReady := make(chan *Conn, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		c, err := ServerConn(raw, cfg)
		if err != nil {
			serverErr <- err
			return
		}
		serverReady <- c
	}()

	clientRaw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientRaw.Close()

	clientConn, err := ClientConn(clientRaw, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	var serverConn *Conn
	select {
	case err := <-serverErr:
		t.Fatal(err)
	case serverConn = <-serverReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server conn handshake timeout")
	}
	defer serverConn.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte("blocked"))
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	_ = clientConn.Close()

	select {
	case err := <-errCh:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected net.ErrClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write did not return after close")
	}
}

func TestConnWarmupRespectsWriteDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	cfg := DefaultConfig()
	cfg.TickMS = 500
	cfg.IdleTimeoutMS = 5000
	cfg.WarmupMS = 300

	serverErr := make(chan error, 1)
	serverReady := make(chan *Conn, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		c, err := ServerConn(raw, cfg)
		if err != nil {
			serverErr <- err
			return
		}
		serverReady <- c
	}()

	clientRaw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientRaw.Close()

	clientConn, err := ClientConn(clientRaw, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	var serverConn *Conn
	select {
	case err := <-serverErr:
		t.Fatal(err)
	case serverConn = <-serverReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server conn handshake timeout")
	}
	defer serverConn.Close()

	_ = clientConn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	start := time.Now()
	n, err := clientConn.Write([]byte("timeout"))
	if n != 0 {
		t.Fatalf("expected n=0, got=%d", n)
	}
	var te interface{ Timeout() bool }
	if err == nil || !errors.As(err, &te) || !te.Timeout() {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("write should timeout during warmup, elapsed=%s", elapsed)
	}
}

func TestConnWarmupDisabled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	cfg := DefaultConfig()
	cfg.TickMS = 50
	cfg.IdleTimeoutMS = 5000
	cfg.WarmupMS = 0

	serverErr := make(chan error, 1)
	serverReady := make(chan *Conn, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		c, err := ServerConn(raw, cfg)
		if err != nil {
			serverErr <- err
			return
		}
		serverReady <- c
	}()

	clientRaw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientRaw.Close()

	clientConn, err := ClientConn(clientRaw, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	var serverConn *Conn
	select {
	case err := <-serverErr:
		t.Fatal(err)
	case serverConn = <-serverReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server conn handshake timeout")
	}
	defer serverConn.Close()

	start := time.Now()
	_ = clientConn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := clientConn.Write([]byte("fast")); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 120*time.Millisecond {
		t.Fatalf("write should not wait when warmup disabled, elapsed=%s", elapsed)
	}
}

func TestConfigNegotiateWarmupMax(t *testing.T) {
	cfgA := DefaultConfig()
	cfgB := DefaultConfig()
	cfgA.WarmupMS = 1000
	cfgB.WarmupMS = 5000

	out, err := cfgA.Negotiate(cfgB)
	if err != nil {
		t.Fatal(err)
	}
	if out.WarmupMS != 5000 {
		t.Fatalf("warmup negotiate mismatch: got=%d want=5000", out.WarmupMS)
	}
}
