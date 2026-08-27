// Package domain also owns the canonicalization and validation rules for the
// identifiers used across the service. Object and node identifiers are
// normalized to a canonical Unicode form so that two byte representations of
// the same logical identifier always collide to the same storage key.
package domain

import (
	"errors"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Identifier validation sentinels.
var (
	ErrEmptyIdentifier   = errors.New("domain: identifier must not be empty")
	ErrInvalidUTF8       = errors.New("domain: identifier must be valid UTF-8")
	ErrIdentifierTooLong = errors.New("domain: identifier exceeds maximum length")
)

// MaxIdentifierLength bounds the byte length of any identifier accepted by the
// service. It is enforced before persistence so the database and HTTP layer
// share a single limit.
const MaxIdentifierLength = 512

// CanonicalizeID returns the NFC-normalized, whitespace-trimmed form of an
// identifier. NFC normalization makes canonically equivalent byte sequences
// compare equal, which is the documented "UTF-8 canonical form" invariant.
func CanonicalizeID(id string) (string, error) {
	if id == "" {
		return "", ErrEmptyIdentifier
	}
	if !utf8.ValidString(id) {
		return "", ErrInvalidUTF8
	}
	canonical := norm.NFC.String(strings.TrimSpace(id))
	if canonical == "" {
		return "", ErrEmptyIdentifier
	}
	if len(canonical) > MaxIdentifierLength {
		return "", ErrIdentifierTooLong
	}
	return canonical, nil
}

// CanonicalKey derives the stable storage key for an object identifier. It is
// a thin alias kept for call sites that need to make the canonicalization step
// explicit in their naming.
func CanonicalKey(objectID string) (string, error) {
	return CanonicalizeID(objectID)
}

// ValidateNodeID validates a storage node identifier. Node identifiers share
// the same canonicalization rules as object identifiers but are kept as a
// separate entry point so callers can attach node-specific validation later.
func ValidateNodeID(nodeID string) (string, error) {
	return CanonicalizeID(nodeID)
}

// ValidateFailureDomain validates a storage failure-domain label. A failure
// domain groups nodes that share a common fault (rack, region, power feed) and
// must be a non-empty canonical identifier.
func ValidateFailureDomain(domain string) (string, error) {
	return CanonicalizeID(domain)
}

// WholeObjectChunk is the sentinel chunk number used for the evidence of an
// empty object, which carries only an overall digest and has no chunks.
const WholeObjectChunk int64 = -1
