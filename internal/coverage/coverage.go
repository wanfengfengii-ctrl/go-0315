// Package coverage implements the coverage basis-point computation used by
// the stability verification and recovery arbiter. It enforces the documented
// integer and overflow boundaries: a zero denominator and multiplication
// overflow are both rejected.
package coverage

import (
	"errors"
	"math"
)

// BPSScale is the fixed multiplier that turns a ratio into basis points.
const BPSScale = 10000

// Sentinel errors returned by Compute.
var (
	ErrZeroDenominator = errors.New("coverage: denominator must be positive")
	ErrNegativeNodes   = errors.New("coverage: effective nodes must be non-negative")
	ErrOverflow        = errors.New("coverage: multiplication overflow")
)

// Compute returns floor(effective*BPSScale/frozen). It rejects a non-positive
// denominator, a negative effective count and any multiplication that would
// overflow int64 before the division.
func Compute(effective, frozen int) (int, error) {
	if frozen <= 0 {
		return 0, ErrZeroDenominator
	}
	if effective < 0 {
		return 0, ErrNegativeNodes
	}
	if int64(effective) > math.MaxInt64/BPSScale {
		return 0, ErrOverflow
	}
	num := int64(effective) * BPSScale
	return int(num / int64(frozen)), nil
}
