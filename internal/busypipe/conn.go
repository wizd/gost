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
}

type Conn struct {
	raw      net.Conn
	isClient bool

	cfg       Config
	codec     *Codec
	scheduler *MinRateScheduler
	mixed     *MixedBuilder

	writeMu sync.Mutex

	readMu       sync.Mutex
	readCond     *sync.Cond
	readBuf      bytes.Buffer
	readErr      error
	readDeadline time.Time

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
	c := &Conn{
		raw:       raw,
		isClient:  isClient,
		cfg:       cfg,
		codec:     NewCodec(cfg.MaxFrameSize),
		scheduler: NewMinRateScheduler(cfg.MinBPS, cfg.TickMS),
		mixed:     NewMixedBuilder(cfg.MinJitter),
		closed:    make(chan struct{}),
		lastRecv:  time.Now(),
	}
	c.readCond = sync.NewCond(&c.readMu)

	if err := c.handshake(); err != nil {
		raw.Close()
		return nil, err
	}

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
	})
	if err != nil {
		return err
	}
	c.cfg = negotiated
	c.codec.SetMaxFrameSize(negotiated.MaxFrameSize)
	c.scheduler = NewMinRateScheduler(negotiated.MinBPS, negotiated.TickMS)
	c.mixed = NewMixedBuilder(negotiated.MinJitter)
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
	return c.readBuf.Read(p)
}

func (c *Conn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}

	written := 0
	maxMixedData := c.codec.MaxPayloadSize() - MixedMetadataLen - c.cfg.MinJitter
	if maxMixedData < 1 {
		maxMixedData = 1
	}

	for len(p) > 0 {
		chunkLen := maxMixedData
		if chunkLen > len(p) {
			chunkLen = len(p)
		}
		chunk := p[:chunkLen]
		p = p[chunkLen:]

		targetPayloadLen := maxInt(
			MixedMetadataLen+len(chunk)+c.cfg.MinJitter,
			64,
		)
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
	}
	return written, nil
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
	return c.raw.SetWriteDeadline(t)
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
			deficit := c.scheduler.ConsumeDeficit()
			if deficit <= 0 {
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
			if err := c.writeFrame(FramePAD, pad, true); err != nil {
				c.closeInternal(false, err)
				return
			}
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
				c.closeInternal(false, errors.New("busypipe: idle timeout"))
				return
			}
		case <-c.closed:
			return
		}
	}
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
	c.readBuf.Write(data)
	c.readCond.Signal()
	c.readMu.Unlock()
}

func (c *Conn) writeFrame(t FrameType, payload []byte, recordRate bool) error {
	frame, err := c.codec.Encode(t, payload, 0)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.raw.Write(frame); err != nil {
		return err
	}
	if recordRate {
		c.scheduler.RecordSent(len(frame))
	}
	return nil
}

func (c *Conn) closeInternal(sendClose bool, cause error) {
	c.once.Do(func() {
		if cause == nil {
			cause = io.EOF
		}
		if sendClose {
			_ = c.writeFrame(FrameCLOSE, nil, false)
		}
		close(c.closed)
		_ = c.raw.Close()

		c.readMu.Lock()
		if c.readErr == nil {
			c.readErr = cause
		}
		c.readCond.Broadcast()
		c.readMu.Unlock()
	})
}
