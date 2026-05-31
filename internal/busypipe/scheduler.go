package busypipe

import "sync"

// MinRateScheduler 跟踪每个 tick 已发送的字节数，并按差额提示需要补齐的 PAD/MIXED 字节。
//
// 调度器在 Conn.Write、Conn.writeFrame 和 keepaliveLoop 等多个 goroutine 间共享，
// 因此所有可变状态都通过内部 mutex 保护，避免并发读写 sentTick 产生 data race。
type MinRateScheduler struct {
	MinBPS int
	TickMS int

	mu       sync.Mutex
	sentTick int
}

func NewMinRateScheduler(minBPS, tickMS int) *MinRateScheduler {
	if minBPS <= 0 {
		minBPS = DefaultMinBPS
	}
	if tickMS <= 0 {
		tickMS = DefaultTickMS
	}
	return &MinRateScheduler{
		MinBPS: minBPS,
		TickMS: tickMS,
	}
}

func (s *MinRateScheduler) TargetBytesPerTick() int {
	target := ((s.MinBPS / 8) * s.TickMS) / 1000
	if target < 1 {
		target = 1
	}
	return target
}

func (s *MinRateScheduler) RecordSent(size int) {
	if size <= 0 {
		return
	}
	s.mu.Lock()
	s.sentTick += size
	s.mu.Unlock()
}

func (s *MinRateScheduler) Deficit() int {
	target := s.TargetBytesPerTick()
	s.mu.Lock()
	sent := s.sentTick
	s.mu.Unlock()
	deficit := target - sent
	if deficit < 0 {
		return 0
	}
	return deficit
}

func (s *MinRateScheduler) ConsumeDeficit() int {
	target := s.TargetBytesPerTick()
	s.mu.Lock()
	sent := s.sentTick
	s.sentTick = 0
	s.mu.Unlock()
	deficit := target - sent
	if deficit < 0 {
		return 0
	}
	return deficit
}
