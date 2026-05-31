package busypipe

import (
	"errors"

	md "github.com/go-gost/core/metadata"
	mdutil "github.com/go-gost/x/metadata/util"
)

const (
	Magic            uint16 = 0x4250
	Version          uint8  = 2
	HeaderLen               = 16
	MixedMetadataLen        = 8

	DefaultMinBPS        = 8000
	DefaultTickMS        = 250
	DefaultMaxFrameSize  = 1400
	DefaultIdleTimeoutMS = 15000
	DefaultMinJitter     = 8
	DefaultWarmupMS      = 3000
	// DefaultReadBufferBytes 默认 256 KB；超过该上限时 readLoop 阻塞，
	// 把背压从进程内存压回 TCP 接收窗口。
	DefaultReadBufferBytes = 256 * 1024
)

type Config struct {
	Version       uint8
	MinBPS        int
	TickMS        int
	MaxFrameSize  int
	IdleTimeoutMS int
	MinJitter     int
	WarmupMS      int
	// ReadBufferBytes 控制接收侧已解码但未被上层消费的最大字节数。
	// <=0 表示不限制（不推荐，会丢失背压能力）。
	ReadBufferBytes int
}

func DefaultConfig() Config {
	return Config{
		Version:         Version,
		MinBPS:          DefaultMinBPS,
		TickMS:          DefaultTickMS,
		MaxFrameSize:    DefaultMaxFrameSize,
		IdleTimeoutMS:   DefaultIdleTimeoutMS,
		MinJitter:       DefaultMinJitter,
		WarmupMS:        DefaultWarmupMS,
		ReadBufferBytes: DefaultReadBufferBytes,
	}
}

func ConfigFromMetadata(m md.Metadata) Config {
	cfg := DefaultConfig()
	cfg.MinBPS = firstPositiveInt(
		mdutil.GetInt(m, "bp.minBps"),
		mdutil.GetInt(m, "bp.min_bps"),
		mdutil.GetInt(m, "minBps"),
		mdutil.GetInt(m, "min_bps"),
		cfg.MinBPS,
	)
	cfg.TickMS = firstPositiveInt(
		mdutil.GetInt(m, "bp.tickMs"),
		mdutil.GetInt(m, "bp.tick_ms"),
		mdutil.GetInt(m, "tickMs"),
		mdutil.GetInt(m, "tick_ms"),
		cfg.TickMS,
	)
	cfg.MaxFrameSize = firstPositiveInt(
		mdutil.GetInt(m, "bp.maxFrameSize"),
		mdutil.GetInt(m, "bp.max_frame_size"),
		mdutil.GetInt(m, "maxFrameSize"),
		mdutil.GetInt(m, "max_frame_size"),
		cfg.MaxFrameSize,
	)
	cfg.IdleTimeoutMS = firstPositiveInt(
		mdutil.GetInt(m, "bp.idleTimeoutMs"),
		mdutil.GetInt(m, "bp.idle_timeout_ms"),
		mdutil.GetInt(m, "idleTimeoutMs"),
		mdutil.GetInt(m, "idle_timeout_ms"),
		cfg.IdleTimeoutMS,
	)
	cfg.MinJitter = firstPositiveInt(
		mdutil.GetInt(m, "bp.minJitterBytes"),
		mdutil.GetInt(m, "bp.min_jitter_bytes"),
		mdutil.GetInt(m, "minJitterBytes"),
		mdutil.GetInt(m, "min_jitter_bytes"),
		cfg.MinJitter,
	)
	if mdutil.IsExists(m, "bp.warmupMs", "bp.warmup_ms", "warmupMs", "warmup_ms") {
		cfg.WarmupMS = mdutil.GetInt(m, "bp.warmupMs", "bp.warmup_ms", "warmupMs", "warmup_ms")
		if cfg.WarmupMS < 0 {
			cfg.WarmupMS = 0
		}
	}
	if mdutil.IsExists(m,
		"bp.readBufferBytes", "bp.read_buffer_bytes",
		"readBufferBytes", "read_buffer_bytes",
	) {
		cfg.ReadBufferBytes = mdutil.GetInt(m,
			"bp.readBufferBytes", "bp.read_buffer_bytes",
			"readBufferBytes", "read_buffer_bytes",
		)
		if cfg.ReadBufferBytes < 0 {
			cfg.ReadBufferBytes = 0
		}
	}
	if cfg.MaxFrameSize < HeaderLen+1 {
		cfg.MaxFrameSize = HeaderLen + 1
	}
	if cfg.MinJitter < 0 {
		cfg.MinJitter = 0
	}
	return cfg
}

func (c Config) Negotiate(peer Config) (Config, error) {
	if c.Version != peer.Version {
		return Config{}, errors.New("busypipe: incompatible protocol version")
	}
	out := Config{
		Version:       c.Version,
		MinBPS:        maxInt(c.MinBPS, peer.MinBPS),
		TickMS:        maxInt(c.TickMS, peer.TickMS),
		MaxFrameSize:  minInt(c.MaxFrameSize, peer.MaxFrameSize),
		IdleTimeoutMS: minInt(c.IdleTimeoutMS, peer.IdleTimeoutMS),
		MinJitter:     maxInt(c.MinJitter, peer.MinJitter),
		WarmupMS:      maxInt(c.WarmupMS, peer.WarmupMS),
	}
	if out.MaxFrameSize < HeaderLen+1 {
		out.MaxFrameSize = HeaderLen + 1
	}
	return out, nil
}

func firstPositiveInt(values ...int) int {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
