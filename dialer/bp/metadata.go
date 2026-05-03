package bp

import (
	md "github.com/go-gost/core/metadata"
	"github.com/go-gost/gost/internal/busypipe"
)

type metadata struct {
	cfg busypipe.Config
}

func (d *bpDialer) parseMetadata(m md.Metadata) error {
	d.md.cfg = busypipe.ConfigFromMetadata(m)
	return nil
}
