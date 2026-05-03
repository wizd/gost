package busypipe

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync/atomic"
)

type FrameType uint8

const (
	FrameHELLO FrameType = 0x01
	FrameDATA  FrameType = 0x02
	FramePAD   FrameType = 0x03
	FramePING  FrameType = 0x04
	FramePONG  FrameType = 0x05
	FrameCLOSE FrameType = 0x06
	FrameMIXED FrameType = 0x07
)

type Frame struct {
	Type    FrameType
	Flags   uint8
	Seq     uint32
	Payload []byte
}

type Codec struct {
	maxFrameSize int
	seq          uint32
}

func NewCodec(maxFrameSize int) *Codec {
	if maxFrameSize < HeaderLen+1 {
		maxFrameSize = HeaderLen + 1
	}
	return &Codec{
		maxFrameSize: maxFrameSize,
	}
}

func (c *Codec) SetMaxFrameSize(maxFrameSize int) {
	if maxFrameSize < HeaderLen+1 {
		maxFrameSize = HeaderLen + 1
	}
	c.maxFrameSize = maxFrameSize
}

func (c *Codec) MaxPayloadSize() int {
	return c.maxFrameSize - HeaderLen
}

func (c *Codec) Encode(t FrameType, payload []byte, flags uint8) ([]byte, error) {
	if len(payload) > c.MaxPayloadSize() {
		return nil, errors.New("busypipe: payload exceeds max frame size")
	}
	seq := atomic.AddUint32(&c.seq, 1) - 1
	buf := make([]byte, HeaderLen+len(payload))
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	buf[2] = Version
	buf[3] = byte(t)
	buf[4] = flags
	buf[5] = HeaderLen
	binary.BigEndian.PutUint16(buf[6:8], uint16(len(payload)))
	binary.BigEndian.PutUint32(buf[8:12], seq)
	// crc placeholder at [12:16] is zero for checksum
	crc := crc32.ChecksumIEEE(buf[:HeaderLen])
	binary.BigEndian.PutUint32(buf[12:16], crc)
	copy(buf[HeaderLen:], payload)
	return buf, nil
}

func (c *Codec) ReadFrame(r io.Reader) (Frame, error) {
	var header [HeaderLen]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}
	if binary.BigEndian.Uint16(header[0:2]) != Magic {
		return Frame{}, errors.New("busypipe: invalid frame magic")
	}
	if header[2] != Version {
		return Frame{}, errors.New("busypipe: unsupported frame version")
	}
	if header[5] != HeaderLen {
		return Frame{}, errors.New("busypipe: invalid frame header length")
	}
	payloadLen := int(binary.BigEndian.Uint16(header[6:8]))
	if payloadLen+HeaderLen > c.maxFrameSize {
		return Frame{}, errors.New("busypipe: frame exceeds max frame size")
	}

	var hNoCRC [HeaderLen]byte
	copy(hNoCRC[:], header[:])
	hNoCRC[12], hNoCRC[13], hNoCRC[14], hNoCRC[15] = 0, 0, 0, 0
	expected := crc32.ChecksumIEEE(hNoCRC[:])
	got := binary.BigEndian.Uint32(header[12:16])
	if expected != got {
		return Frame{}, errors.New("busypipe: invalid frame crc")
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, err
		}
	}
	return Frame{
		Type:    FrameType(header[3]),
		Flags:   header[4],
		Seq:     binary.BigEndian.Uint32(header[8:12]),
		Payload: payload,
	}, nil
}
