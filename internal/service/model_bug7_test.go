package service

import (
	"bytes"
	"errors"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
)

func TestModel_ExpiredLeaseEvidenceHasNoSideEffects(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "expired rejection permits valid replacement and cannot taint verdict",
			run: func(t *testing.T) {
				svc, st := newTestService(t, SuccessAdapter{})
				setupFrozenBatch(t, svc, "b1", "obj-1", 8, 0xAA, threeNodes(), defaultPolicy())
				epoch, err := svc.OpenEpoch(contextB(), "b1")
				if err != nil {
					t.Fatalf("open epoch: %v", err)
				}

				lease, err := st.GetLease(contextB(), "scan-b1-1")
				if err != nil {
					t.Fatalf("get scan lease: %v", err)
				}
				err = svc.SubmitEvidence(contextB(), "b1", epoch, EvidenceInput{
					ObjectID: "obj-1", NodeID: "n1", ChunkNo: 0, Length: 8,
					Digest: digest32(0xBB), OperationID: "expired-op", ObservedTick: lease.ExpiresTick,
				})
				if !errors.Is(err, ErrLeaseConflict) {
					t.Fatalf("expired submission error = %v, want ErrLeaseConflict", err)
				}

				evidence, err := st.ListEvidence(contextB(), "b1", epoch)
				if err != nil {
					t.Fatalf("list evidence after expired submission: %v", err)
				}
				if len(evidence) != 0 {
					t.Fatalf("evidence after expired submission = %+v, want no rows", evidence)
				}

				goodDigest := digest32(0xAA)
				validTick := lease.ExpiresTick - 1
				if err := svc.SubmitEvidence(contextB(), "b1", epoch, EvidenceInput{
					ObjectID: "obj-1", NodeID: "n1", ChunkNo: 0, Length: 8,
					Digest: goodDigest, OperationID: "valid-op", ObservedTick: validTick,
				}); err != nil {
					t.Fatalf("valid replacement after expired rejection: %v", err)
				}
				if err := svc.SubmitEvidence(contextB(), "b1", epoch, EvidenceInput{
					ObjectID: "obj-1", NodeID: "n2", ChunkNo: 0, Length: 8,
					Digest: goodDigest, OperationID: "quorum-op", ObservedTick: validTick,
				}); err != nil {
					t.Fatalf("submit quorum evidence: %v", err)
				}

				result, err := svc.CloseEpoch(contextB(), "b1", epoch)
				if err != nil {
					t.Fatalf("close epoch: %v", err)
				}
				if len(result.Verdicts) != 1 {
					t.Fatalf("verdict count = %d, want 1", len(result.Verdicts))
				}
				verdict := result.Verdicts[0]
				if verdict.VerdictKind != domain.VerdictIntact || !bytes.Equal(verdict.WinningRoot, rootOf(8, goodDigest)) {
					t.Fatalf("verdict = {kind:%q root:%x}, want intact using accepted evidence", verdict.VerdictKind, verdict.WinningRoot)
				}
				if len(result.Anomalies) != 0 {
					t.Fatalf("anomalies = %v, want none", result.Anomalies)
				}
			},
		},
		{
			name: "active lease preserves replay and conflict semantics",
			run: func(t *testing.T) {
				svc, st := newTestService(t, SuccessAdapter{})
				setupFrozenBatch(t, svc, "b1", "obj-1", 8, 0xAA, threeNodes(), defaultPolicy())
				epoch, err := svc.OpenEpoch(contextB(), "b1")
				if err != nil {
					t.Fatalf("open epoch: %v", err)
				}
				lease, err := st.GetLease(contextB(), "scan-b1-1")
				if err != nil {
					t.Fatalf("get scan lease: %v", err)
				}
				in := EvidenceInput{
					ObjectID: "obj-1", NodeID: "n1", ChunkNo: 0, Length: 8,
					Digest: digest32(0xAA), OperationID: "op-1", ObservedTick: lease.StartTick,
				}
				if err := svc.SubmitEvidence(contextB(), "b1", epoch, in); err != nil {
					t.Fatalf("normal submission: %v", err)
				}
				if err := svc.SubmitEvidence(contextB(), "b1", epoch, in); err != nil {
					t.Fatalf("identical replay: %v", err)
				}
				changed := in
				changed.Digest = digest32(0xBB)
				if err := svc.SubmitEvidence(contextB(), "b1", epoch, changed); !errors.Is(err, ErrIdempotencyConflict) {
					t.Fatalf("changed replay error = %v, want ErrIdempotencyConflict", err)
				}

				evidence, err := st.ListEvidence(contextB(), "b1", epoch)
				if err != nil {
					t.Fatalf("list evidence: %v", err)
				}
				if len(evidence) != 1 || !bytes.Equal(evidence[0].Digest, in.Digest) {
					t.Fatalf("stored evidence = %+v, want one unchanged row", evidence)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
