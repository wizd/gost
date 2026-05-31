package bptls

import (
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	"github.com/go-gost/gost/internal/sockopt"
	mdutil "github.com/go-gost/x/metadata/util"
)

type metadata struct {
	mptcp     bool
	cfg       busypipe.Config
	tcpMaxSeg int
	noDelay   bool
	sndBuf    int
	rcvBuf    int
}

func (l *bptlsListener) parseMetadata(m md.Metadata) error {
	l.md.mptcp = mdutil.GetBool(m, "mptcp")
	l.md.cfg = busypipe.ConfigFromMetadata(m)
	if !mdutil.IsExists(m, "bp.tcpMaxSeg", "bp.tcp_max_seg", "tcpMaxSeg", "tcp_max_seg") {
		l.md.tcpMaxSeg = sockopt.DefaultTCPMaxSeg
	} else {
		l.md.tcpMaxSeg = mdutil.GetInt(m, "bp.tcpMaxSeg", "bp.tcp_max_seg", "tcpMaxSeg", "tcp_max_seg")
	}
	if !mdutil.IsExists(m, "bp.noDelay", "bp.nodelay", "bp.no_delay", "noDelay", "nodelay", "no_delay") {
		l.md.noDelay = true
	} else {
		l.md.noDelay = mdutil.GetBool(m, "bp.noDelay", "bp.nodelay", "bp.no_delay", "noDelay", "nodelay", "no_delay")
	}
	l.md.sndBuf = mdutil.GetInt(m, "bp.sndBuf", "bp.sndbuf", "bp.snd_buf", "sndBuf", "sndbuf", "snd_buf")
	l.md.rcvBuf = mdutil.GetInt(m, "bp.rcvBuf", "bp.rcvbuf", "bp.rcv_buf", "rcvBuf", "rcvbuf", "rcv_buf")
	return nil
}
