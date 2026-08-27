package coverage

import (
	"errors"
	"math"
	"testing"
)

func TestComputeExactDivision(t *testing.T) {
	got, err := Compute(3, 4)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if got != 7500 {
		t.Fatalf("Compute(3,4) = %d, want 7500", got)
	}
}

func TestComputeFloors(t *testing.T) {
	got, err := Compute(1, 3)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if got != 3333 {
		t.Fatalf("Compute(1,3) = %d, want 3333 (floor)", got)
	}
}

func TestComputeFullCoverage(t *testing.T) {
	got, err := Compute(5, 5)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if got != BPSScale {
		t.Fatalf("Compute(5,5) = %d, want %d", got, BPSScale)
	}
}

func TestComputeRejectsZeroDenominator(t *testing.T) {
	if _, err := Compute(1, 0); !errors.Is(err, ErrZeroDenominator) {
		t.Fatalf("err = %v, want ErrZeroDenominator", err)
	}
}

func TestComputeRejectsNegativeEffective(t *testing.T) {
	if _, err := Compute(-1, 5); !errors.Is(err, ErrNegativeNodes) {
		t.Fatalf("err = %v, want ErrNegativeNodes", err)
	}
}

func TestComputeRejectsOverflow(t *testing.T) {
	huge := int(math.MaxInt64 / BPSScale)
	if _, err := Compute(huge+1, 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("err = %v, want ErrOverflow", err)
	}
}
