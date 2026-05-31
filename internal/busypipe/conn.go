package busypipe

import (
	"bytes"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type helloPayload struct {
	Version       uint8 `json:"version"`
	MinBPS        int   `json:"min_bps"`
	TickMS        int   `json:"tick_ms"`
	MaxFrameSize  int   `json:"max_frame_size"`
	IdleTimeoutMS int   `json:"idle_timeout_ms"`
	MinJitter     int   `json:"min_jitter_bytes"`
	WarmupMS      int   `json:"warmup_ms"`
}

type Conn struct {
	raw      net.Conn
	isClient bool

	cfg       Config
	codec     *Codec
	scheduler *MinRateScheduler
	mixed     *MixedBuilder

	writeMu sync.Mutex

	warmupUntil     time.Time
	writeDeadlineMu sync.Mutex
	writeDeadline   time.Time
	writeDeadlineCh chan struct{}

	readMu       sync.Mutex
	readCond     *sync.Cond
	readNotFull  *sync.Cond
	readBuf      bytes.Buffer
	readErr      error
	readDeadline time.Time
	readBufLimit int

	lastRecvMu sync.Mutex
	lastRecv   time.Time

	closed  chan struct{}
	closeMu sync.Mutex
	once    sync.Once
}

func ClientConn(raw net.Conn, cfg Config) (*Conn, error) {
	return newConn(raw, cfg, true)
}

func ServerConn(raw net.Conn, cfg Config) (*Conn, error) {
	return newConn(raw, cfg, false)
}

func newConn(raw net.Conn, cfg Config, isClient bool) (*Conn, error) {
	enableTCPKeepAlive(raw)

	handshakeTimeout := time.Duration(cfg.IdleTimeoutMS) * time.Millisecond
	if handshakeTimeout <= 0 {
		handshakeTimeout = time.Duration(DefaultIdleTimeoutMS) * time.Millisecond
	}
	_ = raw.SetDeadline(time.Now().Add(handshakeTimeout))

	c := &Conn{
		raw:             raw,
		isClient:        isClient,
		cfg:             cfg,
		codec:           NewCodec(cfg.MaxFrameSize),
		scheduler:       NewMinRateScheduler(cfg.MinBPS, cfg.TickMS),
		mixed:           NewMixedBuilder(cfg.MinJitter),
		closed:          make(chan struct{}),
		lastRecv:        time.Now(),
		writeDeadlineCh: make(chan struct{}),
		readBufLimit:    cfg.ReadBufferBytes,
	}
	c.readCond = sync.NewCond(&c.readMu)
	c.readNotFull = sync.NewCond(&c.readMu)

	if err := c.handshake(); err != nil {
		raw.Close()
		return nil, err
	}
	_ = raw.SetDeadline(time.Time{})

	go c.readLoop()
	go c.keepaliveLoop()
	go c.idleLoop()

	return c, nil
}

func (c *Conn) handshake() error {
	hello := helloPayload{
		Version:       c.cfg.Version,
		MinBPS:        c.cfg.MinBPS,
		TickMS:        c.cfg.TickMS,
		MaxFrameSize:  c.cfg.MaxFrameSize,
		IdleTimeoutMS: c.cfg.IdleTimeoutMS,
		MinJitter:     c.cfg.MinJitter,
		WarmupMS:      c.cfg.WarmupMS,
	}
	payload, err := json.Marshal(hello)
	if err != nil {
		return err
	}
	if err := c.writeFrame(FrameHELLO, payload, false); err != nil {
		return err
	}

	f, err := c.codec.ReadFrame(c.raw)
	if err != nil {
		return err
	}
	if f.Type != FrameHELLO {
		return errors.New("busypipe: expected HELLO frame")
	}
	var peer helloPayload
	if err := json.Unmarshal(f.Payload, &peer); err != nil {
		return err
	}
	negotiated, err := c.cfg.Negotiate(Config{
		Version:       peer.Version,
		MinBPS:        peer.MinBPS,
		TickMS:        peer.TickMS,
		MaxFrameSize:  peer.MaxFrameSize,
		IdleTimeoutMS: peer.IdleTimeoutMS,
		MinJitter:     peer.MinJitter,
		WarmupMS:      peer.WarmupMS,
	})
	if err != nil {
		return err
	}
	c.cfg = negotiated
	c.codec.SetMaxFrameSize(negotiated.MaxFrameSize)
	c.scheduler = NewMinRateScheduler(negotiated.MinBPS, negotiated.TickMS)
	c.mixed = NewMixedBuilder(negotiated.MinJitter)
	if negotiated.WarmupMS > 0 {
		c.warmupUntil = time.Now().Add(time.Duration(negotiated.WarmupMS) * time.Millisecond)
	} else {
		c.warmupUntil = time.Time{}
	}
	c.touchRecv()
	return nil
}

func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for c.readBuf.Len() == 0 && c.readErr == nil {
		if !c.readDeadline.IsZero() {
			d := time.Until(c.readDeadline)
			if d <= 0 {
				return 0, timeoutError{}
			}
			timer := time.AfterFunc(d, func() {
				c.readMu.Lock()
				c.readCond.Broadcast()
				c.readMu.Unlock()
			})
			c.readCond.Wait()
			timer.Stop()
			continue
		}
		c.readCond.Wait()
	}
	if c.readBuf.Len() == 0 && c.readErr != nil {
		return 0, c.readErr
	}
	n, err := c.readBuf.Read(p)
	// 消费后唤醒等待容量的 readLoop（pushReadData）。
	c.readNotFull.Broadcast()
	return n, err
}

func (c *Conn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	if err := c.waitWarmup(); err != nil {
		return 0, err
	}

	written := 0

	for len(p) > 0 {
		if c.scheduler.Deficit() > 0 {
			chunkLen := c.maxMixedData()
			if chunkLen > len(p) {
				chunkLen = len(p)
			}
			chunk := p[:chunkLen]
			p = p[chunkLen:]

			deficit := c.scheduler.Deficit()
			deficitPayload := deficit - HeaderLen
			if deficitPayload < 0 {
				deficitPayload = 0
			}
			minPayload := MixedMetadataLen + len(chunk) + c.cfg.MinJitter
			targetPayloadLen := maxInt(minPayload, deficitPayload)
			if targetPayloadLen > c.codec.MaxPayloadSize() {
				targetPayloadLen = c.codec.MaxPayloadSize()
			}

			payload, err := c.mixed.Build(chunk, targetPayloadLen)
			if err != nil {
				if err := c.writeFrame(FrameDATA, chunk, true); err != nil {
					return written, err
				}
			} else {
				if err := c.writeFrame(FrameMIXED, payload, true); err != nil {
					return written, err
				}
			}
			written += chunkLen
		} else {
			chunkLen := c.codec.MaxPayloadSize()
			if chunkLen > len(p) {
				chunkLen = len(p)
			}
			chunk := p[:chunkLen]
			p = p[chunkLen:]

			if err := c.writeFrame(FrameDATA, chunk, true); err != nil {
				return written, err
			}
			written += chunkLen
		}
	}
	return written, nil
}

func (c *Conn) maxMixedData() int {
	v := c.codec.MaxPayloadSize() - MixedMetadataLen - c.cfg.MinJitter
	if v < 1 {
		return 1
	}
	return v
}

func (c *Conn) Close() error {
	c.closeInternal(true, io.EOF)
	return nil
}

func (c *Conn) LocalAddr() net.Addr {
	return c.raw.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.raw.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.readMu.Lock()
	c.readDeadline = t
	c.readCond.Broadcast()
	c.readMu.Unlock()
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	if err := c.raw.SetWriteDeadline(t); err != nil {
		return err
	}
	c.writeDeadlineMu.Lock()
	c.writeDeadline = t
	close(c.writeDeadlineCh)
	c.writeDeadlineCh = make(chan struct{})
	c.writeDeadlineMu.Unlock()
	return nil
}

type timeoutError struct{}

func (timeoutError) Error() string {
	return "i/o timeout"
}

func (timeoutError) Timeout() bool {
	return true
}

func (timeoutError) Temporary() bool {
	return true
}

func (c *Conn) waitWarmup() error {
	for {
		if c.warmupUntil.IsZero() {
			return nil
		}
		remaining := time.Until(c.warmupUntil)
		if remaining <= 0 {
			return nil
		}

		deadline, deadlineCh := c.currentWriteDeadline()
		if !deadline.IsZero() {
			d := time.Until(deadline)
			if d <= 0 {
				return timeoutError{}
			}
			if d < remaining {
				remaining = d
			}
		}

		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
		case <-c.closed:
			if !timer.Stop() {
				<-timer.C
			}
			return net.ErrClosed
		case <-deadlineCh:
			if !timer.Stop() {
				<-timer.C
			}
		}
	}
}

func (c *Conn) currentWriteDeadline() (time.Time, chan struct{}) {
	c.writeDeadlineMu.Lock()
	defer c.writeDeadlineMu.Unlock()
	return c.writeDeadline, c.writeDeadlineCh
}

func (c *Conn) readLoop() {
	for {
		select {
		case <-c.closed:
			return
		default:
		}

		f, err := c.codec.ReadFrame(c.raw)
		if err != nil {
			c.closeInternal(false, err)
			return
		}
		c.touchRecv()
		switch f.Type {
		case FrameDATA:
			c.pushReadData(f.Payload)
		case FrameMIXED:
			data, err := c.mixed.Parse(f.Payload)
			if err != nil {
				c.closeInternal(false, err)
				return
			}
			c.pushReadData(data)
		case FramePAD, FramePONG:
			// no-op
		case FramePING:
			if err := c.writeFrame(FramePONG, f.Payload, true); err != nil {
				c.closeInternal(false, err)
				return
			}
		case FrameCLOSE:
			c.closeInternal(false, io.EOF)
			return
		case FrameHELLO:
			c.closeInternal(false, errors.New("busypipe: unexpected HELLO after handshake"))
			return
		default:
			c.closeInternal(false, errors.New("busypipe: unknown frame type"))
			return
		}
	}
}

func (c *Conn) keepaliveLoop() {
	ticker := time.NewTicker(time.Duration(c.cfg.TickMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// 先只读 Deficit，不重置 sentTick，避免与业务 Write 竞争丢字节。
			deficit := c.scheduler.Deficit()
			if deficit <= 0 {
				c.scheduler.ConsumeDeficit()
				continue
			}
			payloadLen := deficit - HeaderLen
			if payloadLen < 0 {
				payloadLen = 0
			}
			if payloadLen > c.codec.MaxPayloadSize() {
				payloadLen = c.codec.MaxPayloadSize()
			}
			pad := make([]byte, payloadLen)
			if payloadLen > 0 {
				if _, err := crand.Read(pad); err != nil {
					c.closeInternal(false, err)
					return
				}
			}
			// 业务 Write 持锁时跳过本 tick 的 PAD，下一 tick 再补。
			// 协议允许背压时跳过 PAD（见 protocol.md 中调度规则）。
			ok, err := c.tryWriteFramePAD(pad)
			if err != nil {
				c.closeInternal(false, err)
				return
			}
			if ok {
				// 真正写出了 PAD，本 tick 计数清零，下个 tick 重新核算。
				c.scheduler.ConsumeDeficit()
			}
			// 未写出时保留 sentTick：业务字节继续累计，避免错误地把
			// 业务写计数清零导致后续 tick 估算偏高。
		case <-c.closed:
			return
		}
	}
}

func (c *Conn) idleLoop() {
	timeout := time.Duration(c.cfg.IdleTimeoutMS) * time.Millisecond
	interval := time.Second
	if timeout < interval {
		interval = timeout
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.lastRecvMu.Lock()
			idle := time.Since(c.lastRecv)
			c.lastRecvMu.Unlock()
			if idle > timeout {
				// 当接收缓冲已满时，readLoop 会在 pushReadData 上阻塞，
				// 由应用层消费速度决定恢复时间。此时不应误判为链路 idle。
				if c.isReadBackpressured() {
					continue
				}
				c.closeInternal(false, errors.New("busypipe: idle timeout"))
				return
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Conn) isReadBackpressured() bool {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	return c.readBufLimit > 0 &&
		c.readBuf.Len() >= c.readBufLimit &&
		c.readErr == nil
}

func (c *Conn) touchRecv() {
	c.lastRecvMu.Lock()
	c.lastRecv = time.Now()
	c.lastRecvMu.Unlock()
}

func (c *Conn) pushReadData(data []byte) {
	if len(data) == 0 {
		return
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	// 背压：当 readBuf 达到上限且未关闭/未出错时，readLoop 在此阻塞，
	// 把压力传回 TCP 接收窗口，避免无界内存膨胀。
	for c.readBufLimit > 0 &&
		c.readBuf.Len() >= c.readBufLimit &&
		c.readErr == nil {
		c.readNotFull.Wait()
	}
	if c.readErr != nil {
		// 连接已关闭/出错，丢弃数据并结束等待。
		return
	}
	c.readBuf.Write(data)
	c.readCond.Signal()
}

// writeFrame 串行化向底层 raw 连接写入一个完整帧。
//
// 关键约束：
//   - 帧编码错误或 raw.Write 错误都会立刻关闭连接，避免下一次 Write 把后续帧
//     拼接到一个已经损坏（部分写入）的 stream 上，导致对端 magic/crc 校验失败。
//   - Conn.Write 在多帧循环中不持有 writeMu，因此 keepaliveLoop 可以在帧之间
//     抢锁插入 PAD；这是把背压传回 TCP 缓冲的关键。
func (c *Conn) writeFrame(t FrameType, payload []byte, recordRate bool) error {
	if err := c.writeFrameRaw(t, payload, recordRate); err != nil {
		c.closeInternal(false, err)
		return err
	}
	return nil
}

// writeFrameRaw 编码并发送一个帧，但不在错误路径上触发 closeInternal。
// 该函数允许 closeInternal 在 sendClose 路径下安全地发送 FrameCLOSE
// 而不会因 sync.Once 嵌套调用产生死锁。
func (c *Conn) writeFrameRaw(t FrameType, payload []byte, recordRate bool) error {
	frame, err := c.codec.Encode(t, payload, 0)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	n, err := c.raw.Write(frame)
	c.writeMu.Unlock()
	if err != nil {
		return err
	}
	if n != len(frame) {
		// net.Conn.Write 在没有 deadline 的情况下会写完或返回错误；
		// 出现部分写说明底层有非预期行为，立即关闭以保护 stream 完整性。
		return io.ErrShortWrite
	}
	if recordRate {
		c.scheduler.RecordSent(len(frame))
	}
	return nil
}

// tryWriteFramePAD 仅用于 keepalive 调度器：抢不到 writeMu 时立即返回，
// 让出本 tick 的 PAD，避免与业务 Write 互相饿死或在 TCP 拥塞时无限堆积。
// 返回值表示是否实际写出；非 nil err 表示底层连接已无法继续使用。
func (c *Conn) tryWriteFramePAD(payload []byte) (bool, error) {
	frame, err := c.codec.Encode(FramePAD, payload, 0)
	if err != nil {
		return false, err
	}
	if !c.writeMu.TryLock() {
		return false, nil
	}
	n, werr := c.raw.Write(frame)
	c.writeMu.Unlock()
	if werr != nil {
		return false, werr
	}
	if n != len(frame) {
		return false, io.ErrShortWrite
	}
	c.scheduler.RecordSent(len(frame))
	return true, nil
}

func (c *Conn) closeInternal(sendClose bool, cause error) {
	c.once.Do(func() {
		if cause == nil {
			cause = io.EOF
		}
		if sendClose {
			_ = c.raw.SetWriteDeadline(time.Now().Add(5 * time.Second))
			// 使用 writeFrameRaw 避免重入 closeInternal（sync.Once 同 goroutine 嵌套会死锁）。
			_ = c.writeFrameRaw(FrameCLOSE, nil, false)
		}
		close(c.closed)
		_ = c.raw.Close()

		c.readMu.Lock()
		if c.readErr == nil {
			c.readErr = cause
		}
		c.readCond.Broadcast()
		c.readNotFull.Broadcast()
		c.readMu.Unlock()
	})
}

func enableTCPKeepAlive(conn net.Conn) {
	for conn != nil {
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(60 * time.Second)
			return
		}
		nc, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			return
		}
		conn = nc.NetConn()
	}
}
