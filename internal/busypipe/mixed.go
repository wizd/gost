package busypipe

import (
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"math/rand"
	"time"
)

type MixedBuilder struct {
	MinJitter  int
	lastOffset int
	hasLast    bool
	rng        *rand.Rand
}

func NewMixedBuilder(minJitter int) *MixedBuilder {
	if minJitter < 0 {
		minJitter = 0
	}
	return &MixedBuilder{
		MinJitter: minJitter,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (m *MixedBuilder) Build(data []byte, targetLen int) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("busypipe: mixed requires non-empty data")
	}
	if targetLen < MixedMetadataLen+len(data) {
		return nil, errors.New("busypipe: target mixed payload too small")
	}
	minOffset := MixedMetadataLen
	maxOffset := targetLen - len(data)
	offset, ok := m.chooseOffset(minOffset, maxOffset)
	if !ok {
		return nil, errors.New("busypipe: unable to satisfy mixed jitter")
	}

	prefixLen := offset - MixedMetadataLen
	suffixLen := targetLen - offset - len(data)

	out := make([]byte, targetLen)
	binary.BigEndian.PutUint16(out[0:2], uint16(offset))
	binary.BigEndian.PutUint16(out[2:4], uint16(len(data)))
	if _, err := crand.Read(out[4:8]); err != nil {
		return nil, err
	}
	if prefixLen > 0 {
		if _, err := crand.Read(out[MixedMetadataLen : MixedMetadataLen+prefixLen]); err != nil {
			return nil, err
		}
	}
	copy(out[offset:offset+len(data)], data)
	if suffixLen > 0 {
		if _, err := crand.Read(out[offset+len(data):]); err != nil {
			return nil, err
		}
	}
	m.lastOffset = offset
	m.hasLast = true
	return out, nil
}

func (m *MixedBuilder) Parse(payload []byte) ([]byte, error) {
	if len(payload) < MixedMetadataLen {
		return nil, errors.New("busypipe: mixed payload too short")
	}
	offset := int(binary.BigEndian.Uint16(payload[0:2]))
	length := int(binary.BigEndian.Uint16(payload[2:4]))
	if offset < MixedMetadataLen {
		return nil, errors.New("busypipe: mixed offset points into metadata")
	}
	if length <= 0 || offset+length > len(payload) {
		return nil, errors.New("busypipe: mixed data out of range")
	}
	data := make([]byte, length)
	copy(data, payload[offset:offset+length])
	return data, nil
}

func (m *MixedBuilder) chooseOffset(minOffset, maxOffset int) (int, bool) {
	if maxOffset < minOffset {
		return 0, false
	}
	if !m.hasLast {
		return minOffset + m.rng.Intn(maxOffset-minOffset+1), true
	}
	candidates := make([]int, 0, maxOffset-minOffset+1)
	for i := minOffset; i <= maxOffset; i++ {
		if absInt(i-m.lastOffset) >= m.MinJitter {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return 0, false
	}
	return candidates[m.rng.Intn(len(candidates))], true
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
