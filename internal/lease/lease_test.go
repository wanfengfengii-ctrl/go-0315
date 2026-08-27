package lease

import (
	"errors"
	"sync"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
)

func TestExpiredAtBoundary(t *testing.T) {
	// Half-open: a tick exactly equal to expires is already outside.
	if Active(10, 0, 10) {
		t.Fatalf("Active(10, 0, 10) = true, want false at boundary")
	}
	if !Expired(10, 10) {
		t.Fatalf("Expired(10, 10) = false, want true at boundary")
	}
	if !Active(9, 0, 10) {
		t.Fatalf("Active(9, 0, 10) = false, want true inside interval")
	}
}

func TestOverlapTouchingBoundariesDoNotOverlap(t *testing.T) {
	if Overlap(0, 10, 10, 20) {
		t.Fatalf("touching intervals reported as overlapping")
	}
	if !Overlap(0, 10, 5, 15) {
		t.Fatalf("overlapping intervals not detected")
	}
}

func TestAcquireGrantsSingleHolder(t *testing.T) {
	m := NewManager()
	if _, err := m.Acquire(domain.ResourceScan, "batch-1", "worker-a", 0, 100); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := m.Acquire(domain.ResourceScan, "batch-1", "worker-b", 0, 100); !errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("second acquire err = %v, want ErrAlreadyHeld", err)
	}
}

func TestConcurrentAcquireOnlyOneWins(t *testing.T) {
	m := NewManager()
	var wg sync.WaitGroup
	wins := make(chan string, 2)

	for _, holder := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(h string) {
			defer wg.Done()
			if _, err := m.Acquire(domain.ResourceWrite, "obj-1", h, 0, 100); err == nil {
				wins <- h
			}
		}(holder)
	}
	wg.Wait()
	close(wins)

	var winners []string
	for h := range wins {
		winners = append(winners, h)
	}
	if len(winners) != 1 {
		t.Fatalf("winners = %v, want exactly one holder", winners)
	}
}

func TestRenewRequiresActiveLeaseAndMatchingHolder(t *testing.T) {
	m := NewManager()
	l, err := m.Acquire(domain.ResourceRead, "obj-2", "h", 0, 100)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := m.Renew(l.LeaseID, "other", 50, 200); !errors.Is(err, ErrHolderMismatch) {
		t.Fatalf("renew wrong holder err = %v, want ErrHolderMismatch", err)
	}
	if _, err := m.Renew(l.LeaseID, "h", 100, 200); !errors.Is(err, ErrExpired) {
		t.Fatalf("renew at boundary err = %v, want ErrExpired", err)
	}
	renewed, err := m.Renew(l.LeaseID, "h", 50, 200)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if renewed.Version != 2 {
		t.Fatalf("renewed version = %d, want 2", renewed.Version)
	}
}

func TestReleaseRequiresMatchingHolder(t *testing.T) {
	m := NewManager()
	l, err := m.Acquire(domain.ResourceTerminal, "batch-2", "h", 0, 100)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := m.Release(l.LeaseID, "intruder"); !errors.Is(err, ErrHolderMismatch) {
		t.Fatalf("release wrong holder err = %v, want ErrHolderMismatch", err)
	}
	if err := m.Release(l.LeaseID, "h"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := m.Release(l.LeaseID, "h"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("double release err = %v, want ErrNotFound", err)
	}
}
