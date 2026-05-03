package bp

import (
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
	mdutil "github.com/go-gost/x/metadata/util"
)

type metadata struct {
	mptcp bool
	cfg   busypipe.Config
}

func (l *bpListener) parseMetadata(m md.Metadata) error {
	l.md.mptcp = mdutil.GetBool(m, "mptcp")
	l.md.cfg = busypipe.ConfigFromMetadata(m)
	return nil
}
