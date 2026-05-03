package busypipe

type MinRateScheduler struct {
	MinBPS   int
	TickMS   int
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
	if size > 0 {
		s.sentTick += size
	}
}

func (s *MinRateScheduler) ConsumeDeficit() int {
	deficit := s.TargetBytesPerTick() - s.sentTick
	if deficit < 0 {
		deficit = 0
	}
	s.sentTick = 0
	return deficit
}
