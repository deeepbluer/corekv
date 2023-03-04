package cache

import (
	"math/rand"
	"time"
)

const (
	cmDepth = 4
)

type cmSketch struct {
	rows [cmDepth]cmRow
	seed [cmDepth]uint64
	mask uint64
}

type cmRow []byte

func newCmSketch(size int64) *cmSketch {
	n := next2Power(size)
	cm := &cmSketch{
		mask: uint64(n - 1),
	}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < cmDepth; i++ {
		cm.rows[i] = make([]byte, size)
		cm.seed[i] = source.Uint64()
	}
	return cm
}

func (c *cmSketch) Increment(key uint64) {
	for i := 0; i < cmDepth; i++ {
		c.rows[i].increment((key ^ c.seed[i]) & c.mask)
	}
}

func (c *cmSketch) Reset() {
	for i := 0; i < cmDepth; i++ {
		c.rows[i].reset()
	}
}

func (c *cmSketch) Estimate(key uint64) uint8 {
	res := uint8(255)
	for i := 0; i < cmDepth; i++ {
		r := c.rows[i].get((key ^ c.seed[i]) & c.mask)
		if r < res {
			res = r
		}
	}
	return res
}

func (c cmRow) increment(hashed uint64) {
	pos := hashed / 2
	shift := (hashed & 1) * 4

	v := (c[pos] >> shift) & 0x0f
	if v < 15 {
		c[pos] += 1 << shift
	}
}

func (c cmRow) get(hashed uint64) uint8 {
	pos := hashed / 2
	shift := (hashed & 1) * 4

	v := (c[pos] >> shift) & 0x0f
	return v
}

func (c cmRow) reset() {
	for i := 0; i < len(c); i++ {
		c[i] = (c[i] >> 1) & 0x77
	}
}

func (c cmRow) clear() {
	for i := 0; i < len(c); i++ {
		c[i] = 0
	}
}

// 快速计算大于 X，且最接近 X 的二次幂
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}
