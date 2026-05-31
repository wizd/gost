package net

import (
	"context"
	"io"
	"time"

	"github.com/go-gost/core/common/bufpool"
	xio "github.com/go-gost/x/internal/io"
)

const (
	// tcpWaitTimeout implements a TCP half-close timeout.
	tcpWaitTimeout = 10 * time.Second
)

// Pipe 在两个连接之间建立双向数据通道。
//
// 该函数面向长生命周期的代理/隧道场景（如 relay+bptls 承载 WireGuard UDP），
// 因此不再对每次读取强制设置短超时。空闲连接的检测交由上层协议
// （BusyPipe 的 idleTimeoutMs、PING/PONG）或系统 TCP keepalive 负责。
//
// 退出条件：
//   - 任意方向产生非 EOF 错误。
//   - 任意方向自然返回 EOF（半关闭后另一方向继续，直到自身也结束）。
//   - 调用方 ctx 被取消（此时会主动关闭两端读端，唤醒阻塞的 Read）。
func Pipe(ctx context.Context, rw1, rw2 io.ReadWriteCloser) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 监听 ctx 取消事件，强制关闭两端读端以唤醒阻塞的 Read；
	// 退出时通过 stop 通道结束 watcher，避免 goroutine 泄漏。
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			closeRead(rw1)
			closeRead(rw2)
		case <-stop:
		}
	}()

	errCh := make(chan error, 2)
	go func() {
		errCh <- pipeHalf(rw1, rw2)
	}()
	go func() {
		errCh <- pipeHalf(rw2, rw1)
	}()

	var firstErr error
	for i := 0; i < 2; i++ {
		err := <-errCh
		if firstErr == nil && err != nil {
			firstErr = err
		}
		// 任一方向结束后取消 ctx，唤醒 watcher 关闭另一端读端，
		// 避免单向流量正常退出后另一方向永远阻塞。
		cancel()
	}

	return firstErr
}

// pipeHalf 单向管道传输：阻塞读、阻塞写，直到 src 自然结束或被外部关闭。
// 不再周期性设置 read deadline，避免长连接被误断。
func pipeHalf(src, dst io.ReadWriteCloser) error {
	defer halfClose(src, dst)

	buf := bufpool.Get(bufferSize / 2)
	defer bufpool.Put(buf)

	for {
		nr, err := src.Read(buf)
		if nr > 0 {
			if _, werr := dst.Write(buf[:nr]); werr != nil {
				return werr
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// closeRead 半关闭读端；若不支持半关闭，则整体关闭。
// 用于在 ctx 取消时唤醒阻塞在 Read 上的 pipeHalf。
func closeRead(rw io.ReadWriteCloser) {
	if rw == nil {
		return
	}
	if cr, ok := rw.(xio.CloseRead); ok {
		if err := cr.CloseRead(); err != xio.ErrUnsupported {
			return
		}
	}
	_ = rw.Close()
}

// halfClose 执行 TCP 半关闭：关闭 src 的读端，半关闭 dst 的写端；
// 不支持半关闭时整体关闭 dst，并为半关闭后的 dst 设置回收超时。
func halfClose(src, dst io.ReadWriteCloser) {
	if cr, ok := src.(xio.CloseRead); ok {
		_ = cr.CloseRead()
	}

	if cw, ok := dst.(xio.CloseWrite); ok {
		if err := cw.CloseWrite(); err == xio.ErrUnsupported {
			_ = dst.Close()
			return
		}
		// 给对端一个有限时间排空，避免半关闭后永久挂起。
		if rd, ok := dst.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = rd.SetReadDeadline(time.Now().Add(tcpWaitTimeout))
		}
		return
	}
	_ = dst.Close()
}
