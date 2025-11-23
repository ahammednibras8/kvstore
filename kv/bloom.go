package kv

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bitset []byte
	m      int
	k      int
}

func NewBloomFilter(n int, p float64) *BloomFilter {
	if n <= 0 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}

	// m = - (n * ln(p)) / (ln(2)^2)
	mFloat := -float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	m := int(math.Ceil(mFloat))

	// k = (m/n) * ln(2)
	kFloat := (float64(m) / float64(n)) * math.Ln2
	k := int(math.Ceil(kFloat))
	if k < 1 {
		k = 1
	}

	// allocate bitset (m bits → ceil(m/8) bytes)
	byteSize := (m + 7) / 8
	bitset := make([]byte, byteSize)

	return &BloomFilter{
		bitset: bitset,
		m:      m,
		k:      k,
	}
}

func (bf *BloomFilter) hashPair(key string) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write([]byte(key))

	h2 := fnv.New64a()
	h2.Write([]byte(key))
	h2.Write([]byte("SALT"))

	return h1.Sum64(), h2.Sum64()
}

func (bf *BloomFilter) Add(key string) {
	h1, h2 := bf.hashPair(key)

	for i := 0; i < bf.k; i++ {
		// index_i = (h1 + i*h2) % m
		idx := (h1 + uint64(i)*h2) % uint64(bf.m)

		byteIndex := idx / 8
		bitPos := idx % 8

		bitMask := byte(1 << bitPos)

		bf.bitset[byteIndex] |= bitMask
	}
}

func (bf *BloomFilter) Contains(key string) bool {
	h1, h2 := bf.hashPair(key)

	for i := 0; i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.m)

		byteIndex := idx / 8
		bitPos := idx % 8

		bitMask := byte(1 << bitPos)

		if bf.bitset[byteIndex]&bitMask == 0 {
			return false
		}
	}

	return true
}
