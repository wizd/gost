package busypipe

import (
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dialPair 建立一对 TCP 上的 BusyPipe 连接（client + server），方便测试复用。
func dialPair(t *testing.T, cfg Config) (client, server *Conn, cleanup func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

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
		ln.Close()
		t.Fatal(err)
	}
	client, err = ClientConn(clientRaw, cfg)
	if err != nil {
		clientRaw.Close()
		ln.Close()
		t.Fatal(err)
	}

	select {
	case server = <-serverReady:
	case err := <-serverErr:
		client.Close()
		ln.Close()
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		client.Close()
		ln.Close()
		t.Fatal("server handshake timeout")
	}

	cleanup = func() {
		_ = client.Close()
		_ = server.Close()
		_ = ln.Close()
	}
	return client, server, cleanup
}

// TestConnUnidirectionalNoIdleTimeout 验证只有 client→server 单向业务流量时，
// keepalive PAD 能持续刷新双方 lastRecv，从而避免 idle timeout 误断。
//
// 该场景对应「大文件下载」：HTTP 服务端持续发 body，客户端不写业务数据，
// 此前默认 15s idle 容易在 PAD 调度被业务 Write 挤占时误触发。
func TestConnUnidirectionalNoIdleTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickMS = 50
	cfg.IdleTimeoutMS = 1500
	cfg.WarmupMS = 0
	cfg.ReadBufferBytes = 64 * 1024

	client, server, cleanup := dialPair(t, cfg)
	defer cleanup()

	stop := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 4096)
		for {
			select {
			case <-stop:
				writerDone <- nil
				return
			default:
			}
			if _, err := client.Write(buf); err != nil {
				writerDone <- err
				return
			}
		}
	}()

	// 服务端持续消费，避免 readBuf 反压导致 Write 阻塞。
	readerDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 8192)
		for {
			if _, err := server.Read(buf); err != nil {
				readerDone <- err
				return
			}
		}
	}()

	// 运行远超 idleTimeoutMS (1.5s) 的时长；如有误断，下面任一通道会先返回错误。
	select {
	case err := <-writerDone:
		t.Fatalf("writer exited unexpectedly: %v", err)
	case err := <-readerDone:
		t.Fatalf("reader exited unexpectedly: %v", err)
	case <-time.After(4 * time.Second):
	}

	close(stop)
	_ = client.Close()
}

// TestConnReadBackpressureBounded 验证慢消费时 server 的 readBuf
// 不会超过 ReadBufferBytes，TCP 背压把压力传回到 client。
//
// 注意：这里测的是上层缓冲不无限增长；TCP 内核窗口本身的拥塞由系统负责。
func TestConnReadBackpressureBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickMS = 200
	cfg.IdleTimeoutMS = 10000
	cfg.WarmupMS = 0
	cfg.ReadBufferBytes = 16 * 1024 // 显式调小，方便观察

	client, server, cleanup := dialPair(t, cfg)
	defer cleanup()

	// client 后台持续 Write；server 不消费。
	writeErr := make(chan error, 1)
	go func() {
		payload := bytes.Repeat([]byte("x"), 1024)
		for i := 0; i < 4096; i++ { // 最多写 4 MB
			if _, err := client.Write(payload); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	// 给生产/背压稳态时间。
	time.Sleep(400 * time.Millisecond)

	// 断言：server 的 readBuf 大小不超过 ReadBufferBytes 上限。
	server.readMu.Lock()
	bufLen := server.readBuf.Len()
	server.readMu.Unlock()
	if bufLen > cfg.ReadBufferBytes {
		t.Fatalf("server readBuf exceeded limit: got=%d, limit=%d", bufLen, cfg.ReadBufferBytes)
	}
	if bufLen == 0 {
		t.Fatal("server readBuf should accumulate some data when consumer is idle")
	}

	// 关闭后 client.Write 应返回（不死锁）。
	_ = server.Close()
	select {
	case <-writeErr:
	case <-time.After(2 * time.Second):
		t.Fatal("client writer did not return after server close")
	}
}

// TestConnReadBackpressureReleaseAfterConsume 验证消费恢复后背压会自动释放。
func TestConnReadBackpressureReleaseAfterConsume(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickMS = 200
	cfg.IdleTimeoutMS = 10000
	cfg.WarmupMS = 0
	cfg.ReadBufferBytes = 8 * 1024

	client, server, cleanup := dialPair(t, cfg)
	defer cleanup()

	payload := bytes.Repeat([]byte("y"), 64*1024)

	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		writeDone <- err
	}()

	received := make([]byte, 0, len(payload))
	buf := make([]byte, 4096)
	deadline := time.Now().Add(5 * time.Second)
	for len(received) < len(payload) {
		_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := server.Read(buf)
		if n > 0 {
			received = append(received, buf[:n]...)
		}
		if err != nil {
			t.Fatalf("read error after %d bytes: %v", len(received), err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not receive full payload: got %d/%d", len(received), len(payload))
		}
	}

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("writer error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not finish after consumer drained")
	}

	if !bytes.Equal(received, payload) {
		t.Fatalf("payload mismatch: len(received)=%d", len(received))
	}
}

// TestConnLargeBidirectionalTransfer 模拟大流量收发：双向各传 256 KB，
// 在 keepalive 持续 PAD 的情况下不能死锁或丢字节。
func TestConnLargeBidirectionalTransfer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickMS = 100
	cfg.IdleTimeoutMS = 10000
	cfg.WarmupMS = 0
	cfg.ReadBufferBytes = 128 * 1024

	client, server, cleanup := dialPair(t, cfg)
	defer cleanup()

	payload := bytes.Repeat([]byte("Z"), 256*1024)

	var wg sync.WaitGroup
	errCh := make(chan error, 4)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := client.Write(payload); err != nil {
			errCh <- err
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := server.Write(payload); err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(server, buf); err != nil {
			errCh <- err
			return
		}
		if !bytes.Equal(buf, payload) {
			errCh <- io.ErrUnexpectedEOF
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(client, buf); err != nil {
			errCh <- err
			return
		}
		if !bytes.Equal(buf, payload) {
			errCh <- io.ErrUnexpectedEOF
		}
	}()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
		t.Fatal("bidirectional transfer did not complete in 15s")
	}

	select {
	case err := <-errCh:
		t.Fatalf("transfer error: %v", err)
	default:
	}
}

// TestSchedulerConcurrentStress 在多 goroutine 并发下调用 RecordSent/Deficit/ConsumeDeficit，
// 不应 panic 也不应出现负数 deficit（mutex 已保护内部状态）。
func TestSchedulerConcurrentStress(t *testing.T) {
	s := NewMinRateScheduler(8000, 250)

	const goroutines = 16
	const iters = 5000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				s.RecordSent(1)
				_ = s.Deficit()
				if j%200 == 0 {
					_ = s.ConsumeDeficit()
				}
			}
		}()
	}
	wg.Wait()

	if got := s.Deficit(); got < 0 {
		t.Fatalf("deficit went negative under concurrent load: %d", got)
	}
}

// TestKeepalivePADNotStarvedByWrite 间接验证 keepalive 不会因业务 Write
// 长期持锁而无限饿死：业务持续写时，对端 readLoop 仍应陆续收到 PAD/MIXED/DATA，
// 从而刷新 lastRecv，不触发 idle timeout。
func TestKeepalivePADNotStarvedByWrite(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TickMS = 50
	cfg.IdleTimeoutMS = 1500
	cfg.WarmupMS = 0
	cfg.ReadBufferBytes = 64 * 1024

	client, server, cleanup := dialPair(t, cfg)
	defer cleanup()

	// server 持续写业务流量，client 也持续写——确保两端 writeMu 都被频繁占用。
	stop := make(chan struct{})
	defer close(stop)

	var serverWriteErr atomic.Value
	go func() {
		buf := make([]byte, 2048)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := server.Write(buf); err != nil {
				serverWriteErr.Store(err)
				return
			}
		}
	}()
	var clientWriteErr atomic.Value
	go func() {
		buf := make([]byte, 2048)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := client.Write(buf); err != nil {
				clientWriteErr.Store(err)
				return
			}
		}
	}()

	// 两端持续消费，否则 readBuf 满了会触发背压让 Write 阻塞，
	// 也间接验证 keepalive PAD 不会因 writeMu 长期被业务占用而饿死。
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := server.Read(buf); err != nil {
				return
			}
		}
	}()
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := client.Read(buf); err != nil {
				return
			}
		}
	}()

	// 运行远超 idleTimeoutMS (1.5s) 的时长。如果 PAD 被饿死，会触发 idle close，
	// Write/Read 会返回错误。
	time.Sleep(4 * time.Second)
	if err := serverWriteErr.Load(); err != nil {
		t.Fatalf("server write errored: %v", err)
	}
	if err := clientWriteErr.Load(); err != nil {
		t.Fatalf("client write errored: %v", err)
	}
}
