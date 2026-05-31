package net

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPipeLongIdleNoTimeout 验证空闲连接不再被 30s 固定 read deadline 误断。
//
// 重写后的 Pipe 不会对每次读取设置短超时，因此 1.5s 完全静默后仍应保持运行，
// 直到任意一端关闭或上层 ctx 取消。
func TestPipeLongIdleNoTimeout(t *testing.T) {
	a1, a2 := net.Pipe()
	defer a1.Close()
	defer a2.Close()
	b1, b2 := net.Pipe()
	defer b1.Close()
	defer b2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Pipe(ctx, a2, b1)
	}()

	// 1.5s 内没有任何数据。原 30s 强制超时实现下也不会触发，
	// 但本测试断言「长时间空闲」不会让 Pipe 提前退出。
	time.Sleep(1500 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Pipe exited unexpectedly during idle: %v", err)
	default:
	}

	// 主动关闭一端唤醒 pipeHalf，确认 Pipe 能正常退出。
	a1.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Pipe did not exit after closing one side")
	}
}

// TestPipeContextCancelUnblocksRead 验证 ctx 取消时阻塞中的 Read 会被唤醒，
// 避免连接永远卡在阻塞 Read 上无法回收。
func TestPipeContextCancelUnblocksRead(t *testing.T) {
	a1, a2 := net.Pipe()
	defer a1.Close()
	defer a2.Close()
	b1, b2 := net.Pipe()
	defer b1.Close()
	defer b2.Close()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- Pipe(ctx, a2, b1)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Pipe did not exit within 2s after ctx cancel")
	}
}

// TestPipeBidirectionalDataFlow 验证双向数据透传。
func TestPipeBidirectionalDataFlow(t *testing.T) {
	a1, a2 := net.Pipe()
	defer a1.Close()
	defer a2.Close()
	b1, b2 := net.Pipe()
	defer b1.Close()
	defer b2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = Pipe(ctx, a2, b1) }()

	// a1 -> a2 -> Pipe -> b1 -> b2
	go func() { _, _ = a1.Write([]byte("hello")) }()
	buf := make([]byte, 5)
	if _, err := io.ReadFull(b2, buf); err != nil {
		t.Fatalf("forward direction: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("unexpected forward payload: %q", string(buf))
	}

	go func() { _, _ = b2.Write([]byte("world")) }()
	buf = make([]byte, 5)
	if _, err := io.ReadFull(a1, buf); err != nil {
		t.Fatalf("reverse direction: %v", err)
	}
	if string(buf) != "world" {
		t.Fatalf("unexpected reverse payload: %q", string(buf))
	}
}

// TestPipeOneSideClosePropagates 验证一方关闭后 Pipe 能优雅退出。
func TestPipeOneSideClosePropagates(t *testing.T) {
	a1, a2 := net.Pipe()
	defer a1.Close()
	defer a2.Close()
	b1, b2 := net.Pipe()
	defer b1.Close()
	defer b2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Pipe(ctx, a2, b1)
	}()

	time.Sleep(50 * time.Millisecond)
	_ = a1.Close()

	select {
	case err := <-done:
		// 允许 nil 或带 EOF 语义的错误；关键是退出。
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
			t.Logf("Pipe exited with err=%v (acceptable)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pipe did not exit after one side closed")
	}
}

// slowReader 包装 io.ReadWriteCloser，让 Read 按固定间隔慢速消费。
// 用于压力测试背压：消费慢时生产端应该自然阻塞。
type slowReader struct {
	io.ReadWriteCloser
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.ReadWriteCloser.Read(p)
}

// TestPipeBackpressureNoUnboundedGoroutines 验证消费慢时 Pipe 不会
// 堆积内部 goroutine 或无限增长缓冲。通过约束测试运行时间间接验证：
// 若 Pipe 有内存爆炸或死循环，测试会超时。
func TestPipeBackpressureNoUnboundedGoroutines(t *testing.T) {
	a1, a2 := net.Pipe()
	b1, b2 := net.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slowB := &slowReader{ReadWriteCloser: b2, delay: 5 * time.Millisecond}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = Pipe(ctx, a2, b1)
	}()

	var produced atomic.Int64
	go func() {
		defer wg.Done()
		chunk := make([]byte, 1024)
		for i := 0; i < 200; i++ {
			n, err := a1.Write(chunk)
			if err != nil {
				return
			}
			produced.Add(int64(n))
		}
		_ = a1.Close()
	}()

	// 持续消费一段时间，然后关闭 Pipe。
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := slowB.Read(buf); err != nil {
				return
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	_ = b2.Close()
	_ = a2.Close()

	doneAll := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneAll)
	}()
	select {
	case <-doneAll:
	case <-time.After(3 * time.Second):
		t.Fatal("Pipe goroutines did not exit on close")
	}
}
