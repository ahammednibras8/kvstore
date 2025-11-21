package main

import (
	"fmt"
	"math/rand"
	"time"
)

type ZipfKeyGen struct {
	rng  *rand.Rand
	zipf *rand.Zipf
	max  uint64
}

func NewZipfKeyGen(max uint64) *ZipfKeyGen {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))

	zipf := rand.NewZipf(rng, 1.5, 1.0, max)

	return &ZipfKeyGen{
		rng:  rng,
		zipf: zipf,
		max:  max,
	}
}

func (z *ZipfKeyGen) NextKey() string {
	rank := z.zipf.Uint64()
	return fmt.Sprintf("key_%d", rank)
}

func main() {
	gen := NewZipfKeyGen(1_000_000)

	for i := 0; i < 10; i++ {
		fmt.Println(gen.NextKey())
	}
}