package busypipe

import (
	"bytes"
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
