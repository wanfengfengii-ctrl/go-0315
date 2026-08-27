package service_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/service"
	"archival-replica-integrity-recovery/internal/store"
)

func TestModel_LeaseReuseAtHalfOpenExpiry(t *testing.T) {
	tests := []struct {
		name         string
		startTick    int64
		expiresTick  int64
		wantConflict bool
	}{
		{name: "active interval remains exclusive", startTick: 9, expiresTick: 19, wantConflict: true},
		{name: "expiry boundary permits takeover", startTick: 10, expiresTick: 20},
		{name: "tick after expiry permits takeover", startTick: 11, expiresTick: 21},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(filepath.Join(t.TempDir(), "leases.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			svc := service.NewService(st, nil)

			if _, err := svc.AcquireLease(ctx, domain.ResourceWrite, "obj-1", "worker-a", 0, 10); err != nil {
				t.Fatalf("worker A acquire [0,10): %v", err)
			}
			got, err := svc.AcquireLease(ctx, domain.ResourceWrite, "obj-1", "worker-b", tt.startTick, tt.expiresTick)
			if tt.wantConflict {
				if !errors.Is(err, service.ErrLeaseConflict) {
					t.Fatalf("worker B acquire [%d,%d) error = %v, want ErrLeaseConflict", tt.startTick, tt.expiresTick, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("worker B acquire [%d,%d): %v", tt.startTick, tt.expiresTick, err)
			}
			if got.Holder != "worker-b" || got.StartTick != tt.startTick || got.ExpiresTick != tt.expiresTick {
				t.Fatalf("takeover lease = %+v, want worker-b holding [%d,%d)", got, tt.startTick, tt.expiresTick)
			}
		})
	}
}
