// Package layout implements the deterministic object layout validation and
// root digest computation declared by the batch and preservation policy
// catalogue component. It enforces the chunk interval, object length and
// digest algorithm invariants and computes object root digests using a fixed
// binary encoding so that identical inputs always produce identical roots.
package layout

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"archival-replica-integrity-recovery/internal/domain"
)

// Size bounds for a frozen chunk specification.
const (
	MinChunkSize int64 = 1
	MaxChunkSize int64 = 64 * 1024 * 1024 // 64 MiB
)

// DomainSeparator is the single byte that opens every root digest encoding.
// It prevents ambiguity between the object length field and chunk records.
const DomainSeparator byte = 0x1F

// Sentinel errors returned by layout validation. Callers use errors.Is to
// classify a rejected layout without coupling to message text.
var (
	ErrInvalidChunkSize  = errors.New("layout: chunk size out of range [1, 64MiB]")
	ErrNegativeLength    = errors.New("layout: object length must be non-negative")
	ErrChunkNumberGap    = errors.New("layout: chunk numbers must be zero-based and consecutive")
	ErrChunkHole         = errors.New("layout: chunk interval hole")
	ErrChunkOverlap      = errors.New("layout: chunk interval overlap")
	ErrChunkOutOfBounds  = errors.New("layout: chunk interval out of bounds")
	ErrChunkTooLarge     = errors.New("layout: chunk exceeds frozen chunk size")
	ErrChunkEmpty        = errors.New("layout: chunk interval must be non-empty")
	ErrUnsupportedDigest = errors.New("layout: unsupported digest algorithm")
	ErrBadDigestLength   = errors.New("layout: digest length does not match algorithm")
)

// Chunk is a single validated chunk interval. No is the zero-based sequence
// number, Offset is the byte offset from the start of the object and Length is
// the number of bytes covered by the chunk.
type Chunk struct {
	No     int64
	Offset int64
	Length int64
}

// ValidateChunkSize reports whether chunkSize lies within the documented
// 1..64MiB bounds.
func ValidateChunkSize(chunkSize int64) error {
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return ErrInvalidChunkSize
	}
	return nil
}

// ValidateLayout verifies that the object length, frozen chunk size and chunk
// intervals satisfy the layout invariants: non-negative length, valid chunk
// size, zero-based consecutive chunk numbers, non-overlapping contiguous
// intervals that exactly cover the object length, and chunk lengths within the
// frozen size. An empty object must have no chunks.
func ValidateLayout(objectLength, chunkSize int64, chunks []Chunk) error {
	if objectLength < 0 {
		return ErrNegativeLength
	}
	if err := ValidateChunkSize(chunkSize); err != nil {
		return err
	}

	if objectLength == 0 {
		if len(chunks) != 0 {
			return ErrChunkOutOfBounds
		}
		return nil
	}

	var offset int64
	for i, c := range chunks {
		if c.No != int64(i) {
			return ErrChunkNumberGap
		}
		if c.Length <= 0 {
			return ErrChunkEmpty
		}
		if c.Length > chunkSize {
			return ErrChunkTooLarge
		}
		if c.Offset < offset {
			return ErrChunkOverlap
		}
		if c.Offset > offset {
			return ErrChunkHole
		}
		offset = c.Offset + c.Length
	}
	if offset != objectLength {
		if offset < objectLength {
			return ErrChunkHole
		}
		return ErrChunkOutOfBounds
	}
	return nil
}

// ChunkDigest pairs a zero-based chunk number with its normalized digest.
type ChunkDigest struct {
	No     int64
	Digest []byte
}

// RootDigest computes the deterministic object root digest from the declared
// hash algorithm, object length and the chunk digests. Chunks are sorted by
// sequence number and encoded as domain separator, big-endian object length,
// then for each chunk the big-endian sequence number followed by its digest.
func RootDigest(algorithm domain.HashAlgorithm, objectLength int64, chunks []ChunkDigest) ([]byte, error) {
	size := algorithm.DigestSize()
	if size == 0 {
		return nil, ErrUnsupportedDigest
	}

	// A stable copy preserves the caller's slice.
	ordered := make([]ChunkDigest, len(chunks))
	copy(ordered, chunks)
	for i := 1; i < len(ordered); i++ {
		for j := i; j > 0 && ordered[j].No < ordered[j-1].No; j-- {
			ordered[j], ordered[j-1] = ordered[j-1], ordered[j]
		}
	}

	var buf []byte
	buf = append(buf, DomainSeparator)
	var lenBytes [8]byte
	binary.BigEndian.PutUint64(lenBytes[:], uint64(objectLength))
	buf = append(buf, lenBytes[:]...)
	for _, c := range ordered {
		if len(c.Digest) != size {
			return nil, fmt.Errorf("%w: chunk %d", ErrBadDigestLength, c.No)
		}
		var noBytes [8]byte
		binary.BigEndian.PutUint64(noBytes[:], uint64(c.No))
		buf = append(buf, noBytes[:]...)
		buf = append(buf, c.Digest...)
	}

	switch algorithm {
	case domain.HashSHA256:
		sum := sha256.Sum256(buf)
		return sum[:], nil
	default:
		return nil, ErrUnsupportedDigest
	}
}
