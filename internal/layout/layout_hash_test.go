package layout

import (
	"bytes"
	"errors"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
)

func digest32(seed byte) []byte {
	d := make([]byte, 32)
	for i := range d {
		d[i] = seed
	}
	return d
}

func TestRootDigestEmptyObject(t *testing.T) {
	got, err := RootDigest(domain.HashSHA256, 0, nil)
	if err != nil {
		t.Fatalf("RootDigest empty: %v", err)
	}
	if len(got) != 32 {
		t.Fatalf("digest length = %d, want 32", len(got))
	}
	// Determinism: identical inputs must produce identical outputs.
	again, err := RootDigest(domain.HashSHA256, 0, nil)
	if err != nil {
		t.Fatalf("RootDigest empty (2): %v", err)
	}
	if !bytes.Equal(got, again) {
		t.Fatalf("empty-object root not deterministic")
	}
}

func TestRootDigestSingleChunk(t *testing.T) {
	d := digest32(0xAB)
	got, err := RootDigest(domain.HashSHA256, 16, []ChunkDigest{{No: 0, Digest: d}})
	if err != nil {
		t.Fatalf("RootDigest single: %v", err)
	}
	if len(got) != 32 {
		t.Fatalf("digest length = %d, want 32", len(got))
	}
}

func TestRootDigestSortsChunksDeterministically(t *testing.T) {
	a := ChunkDigest{No: 0, Digest: digest32(0x01)}
	b := ChunkDigest{No: 1, Digest: digest32(0x02)}
	c := ChunkDigest{No: 2, Digest: digest32(0x03)}

	ordered, err := RootDigest(domain.HashSHA256, 48, []ChunkDigest{a, b, c})
	if err != nil {
		t.Fatalf("RootDigest ordered: %v", err)
	}
	shuffled, err := RootDigest(domain.HashSHA256, 48, []ChunkDigest{c, a, b})
	if err != nil {
		t.Fatalf("RootDigest shuffled: %v", err)
	}
	if !bytes.Equal(ordered, shuffled) {
		t.Fatalf("root digest differs after chunk reordering: %x != %x", ordered, shuffled)
	}
}

func TestRootDigestRejectsUnsupportedAlgorithm(t *testing.T) {
	if _, err := RootDigest(domain.HashAlgorithm("md5"), 0, nil); !errors.Is(err, ErrUnsupportedDigest) {
		t.Fatalf("err = %v, want ErrUnsupportedDigest", err)
	}
}

func TestRootDigestRejectsBadDigestLength(t *testing.T) {
	if _, err := RootDigest(domain.HashSHA256, 4, []ChunkDigest{{No: 0, Digest: []byte{1, 2, 3}}}); !errors.Is(err, ErrBadDigestLength) {
		t.Fatalf("err = %v, want ErrBadDigestLength", err)
	}
}

func TestValidateLayoutRejectsChunkHole(t *testing.T) {
	chunks := []Chunk{{No: 0, Offset: 0, Length: 4}, {No: 1, Offset: 8, Length: 4}}
	if err := ValidateLayout(8, 8, chunks); !errors.Is(err, ErrChunkHole) {
		t.Fatalf("err = %v, want ErrChunkHole", err)
	}
}

func TestValidateLayoutRejectsChunkOverlap(t *testing.T) {
	chunks := []Chunk{{No: 0, Offset: 0, Length: 5}, {No: 1, Offset: 3, Length: 5}}
	if err := ValidateLayout(8, 8, chunks); !errors.Is(err, ErrChunkOverlap) {
		t.Fatalf("err = %v, want ErrChunkOverlap", err)
	}
}

func TestValidateLayoutRejectsOutOfBounds(t *testing.T) {
	chunks := []Chunk{{No: 0, Offset: 0, Length: 10}}
	if err := ValidateLayout(8, 16, chunks); !errors.Is(err, ErrChunkOutOfBounds) {
		t.Fatalf("err = %v, want ErrChunkOutOfBounds", err)
	}
}

func TestValidateLayoutRejectsChunkNumberGap(t *testing.T) {
	chunks := []Chunk{{No: 0, Offset: 0, Length: 4}, {No: 2, Offset: 4, Length: 4}}
	if err := ValidateLayout(8, 8, chunks); !errors.Is(err, ErrChunkNumberGap) {
		t.Fatalf("err = %v, want ErrChunkNumberGap", err)
	}
}

func TestValidateLayoutAcceptsCoveringLayout(t *testing.T) {
	chunks := []Chunk{{No: 0, Offset: 0, Length: 8}, {No: 1, Offset: 8, Length: 8}}
	if err := ValidateLayout(16, 8, chunks); err != nil {
		t.Fatalf("ValidateLayout covering: %v", err)
	}
}

func TestValidateLayoutRejectsNegativeLength(t *testing.T) {
	if err := ValidateLayout(-1, 8, nil); !errors.Is(err, ErrNegativeLength) {
		t.Fatalf("err = %v, want ErrNegativeLength", err)
	}
}

func TestValidateLayoutRejectsBadChunkSize(t *testing.T) {
	if err := ValidateLayout(0, 0, nil); !errors.Is(err, ErrInvalidChunkSize) {
		t.Fatalf("err = %v, want ErrInvalidChunkSize", err)
	}
	if err := ValidateLayout(0, MaxChunkSize+1, nil); !errors.Is(err, ErrInvalidChunkSize) {
		t.Fatalf("err = %v, want ErrInvalidChunkSize", err)
	}
}
