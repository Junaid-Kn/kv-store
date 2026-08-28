package storage_engine

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits          []byte
	itemCount     uint64
	fpProbability float64
	bitCount      uint64
	hashCount     uint32
}

// NewBloomFilter creates a Bloom filter.
//
// fpProbability should be something like:
// 0.01 = 1% false-positive probability
// 0.001 = 0.1% false-positive probability
func NewBloomFilter(itemCount uint64, fpProbability float64) *BloomFilter {
	if itemCount == 0 {
		itemCount = 1
	}

	if fpProbability <= 0 || fpProbability >= 1 {
		panic("false-positive probability must be between 0 and 1")
	}

	bitCount := GetSize(itemCount, fpProbability)
	hashCount := GetHashCount(bitCount, itemCount)

	// Round up to a whole number of bytes.
	byteCount := (bitCount + 7) / 8

	// Since we allocate whole bytes, the actual number of bits
	// available is byteCount * 8.
	bitCount = byteCount * 8

	return &BloomFilter{
		bits:          make([]byte, byteCount),
		itemCount:     itemCount,
		fpProbability: fpProbability,
		bitCount:      bitCount,
		hashCount:     hashCount,
	}
}

// GetSize returns the number of bits required for the desired
// false-positive probability.
//
// m = -(n * ln(p)) / (ln(2)^2)
func GetSize(itemCount uint64, fpProbability float64) uint64 {
	if itemCount == 0 {
		return 0
	}

	return uint64(math.Ceil(
		-(float64(itemCount) * math.Log(fpProbability)) /
			math.Pow(math.Log(2), 2),
	))
}

// GetHashCount returns the optimal number of hash functions.
//
// k = (m / n) * ln(2)
func GetHashCount(bitCount, itemCount uint64) uint32 {
	if itemCount == 0 {
		return 1
	}

	k := float64(bitCount) / float64(itemCount) * math.Ln2

	hashCount := uint32(math.Round(k))

	if hashCount < 1 {
		hashCount = 1
	}

	return hashCount
}

// Add inserts a key into the Bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hashKey(key)

	for i := uint32(0); i < bf.hashCount; i++ {
		// Double hashing:
		//
		// hash_i = h1 + i*h2
		hash := h1 + uint64(i)*h2
		index := hash % bf.bitCount

		byteIndex := index / 8
		bitIndex := index % 8

		bf.bits[byteIndex] |= 1 << bitIndex
	}
}

// MayContain returns:
//
// true  -> key MAY exist
// false -> key DEFINITELY does not exist
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := hashKey(key)

	for i := uint32(0); i < bf.hashCount; i++ {
		hash := h1 + uint64(i)*h2
		index := hash % bf.bitCount

		byteIndex := index / 8
		bitIndex := index % 8

		if bf.bits[byteIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}

	return true
}

// hashKey generates two independent-ish 64-bit hashes.
// We use them to generate k hash functions through double hashing.
func hashKey(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(key)

	h2 := fnv.New64()
	h2.Write(key)

	return h1.Sum64(), h2.Sum64()
}

// Serialize converts the Bloom filter into bytes.
//
// Format:
//
// [8 bytes bitCount]
// [4 bytes hashCount]
// [8 bytes itemCount]
// [8 bytes fpProbability]
// [4 bytes bit-array length]
// [bit array]
func (bf *BloomFilter) Serialize() []byte {
	const headerSize = 8 + 4 + 8 + 8 + 4

	result := make([]byte, headerSize+len(bf.bits))

	offset := 0

	binary.LittleEndian.PutUint64(
		result[offset:],
		bf.bitCount,
	)
	offset += 8

	binary.LittleEndian.PutUint32(
		result[offset:],
		bf.hashCount,
	)
	offset += 4

	binary.LittleEndian.PutUint64(
		result[offset:],
		bf.itemCount,
	)
	offset += 8

	binary.LittleEndian.PutUint64(
		result[offset:],
		math.Float64bits(bf.fpProbability),
	)
	offset += 8

	binary.LittleEndian.PutUint32(
		result[offset:],
		uint32(len(bf.bits)),
	)
	offset += 4

	copy(result[offset:], bf.bits)

	return result
}

// Deserialize reconstructs a Bloom filter from serialized bytes.
func DeserializeBloomFilter(data []byte) *BloomFilter {
	const headerSize = 8 + 4 + 8 + 8 + 4

	if len(data) < headerSize {
		return nil
	}

	offset := 0

	bitCount := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	hashCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	itemCount := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	fpProbability := math.Float64frombits(
		binary.LittleEndian.Uint64(data[offset:]),
	)
	offset += 8

	byteCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if len(data) < offset+int(byteCount) {
		return nil
	}

	bits := make([]byte, byteCount)
	copy(bits, data[offset:offset+int(byteCount)])

	return &BloomFilter{
		bits:          bits,
		itemCount:     itemCount,
		fpProbability: fpProbability,
		bitCount:      bitCount,
		hashCount:     hashCount,
	}
}
