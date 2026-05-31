package bptls

import (
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	"github.com/go-gost/gost/internal/sockopt"
	mdutil "github.com/go-gost/x/metadata/util"
)

type metadata struct {
	cfg       busypipe.Config
	mptcp     bool
	tcpMaxSeg int
	noDelay   bool
	sndBuf    int
	rcvBuf    int
}

func (d *bptlsDialer) parseMetadata(m md.Metadata) error {
	d.md.cfg = busypipe.ConfigFromMetadata(m)
	d.md.mptcp = mdutil.GetBool(m, "mptcp")
	if !mdutil.IsExists(m, "bp.tcpMaxSeg", "bp.tcp_max_seg", "tcpMaxSeg", "tcp_max_seg") {
		d.md.tcpMaxSeg = sockopt.DefaultTCPMaxSeg
	} else {
		d.md.tcpMaxSeg = mdutil.GetInt(m, "bp.tcpMaxSeg", "bp.tcp_max_seg", "tcpMaxSeg", "tcp_max_seg")
	}
	if !mdutil.IsExists(m, "bp.noDelay", "bp.nodelay", "bp.no_delay", "noDelay", "nodelay", "no_delay") {
		d.md.noDelay = true
	} else {
		d.md.noDelay = mdutil.GetBool(m, "bp.noDelay", "bp.nodelay", "bp.no_delay", "noDelay", "nodelay", "no_delay")
	}
	d.md.sndBuf = mdutil.GetInt(m, "bp.sndBuf", "bp.sndbuf", "bp.snd_buf", "sndBuf", "sndbuf", "snd_buf")
	d.md.rcvBuf = mdutil.GetInt(m, "bp.rcvBuf", "bp.rcvbuf", "bp.rcv_buf", "rcvBuf", "rcvbuf", "rcv_buf")
	return nil
}
