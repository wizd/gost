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
