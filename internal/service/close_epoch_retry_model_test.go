package service

import (
	"errors"
	"reflect"
	"testing"

	"archival-replica-integrity-recovery/internal/store"
)

func TestModel_RetriedClosePreservesIsolationGeneration(t *testing.T) {
	svc, st := newTestService(t, SuccessAdapter{})
	batchID, generation := setupAnomalousGeneration(t, svc, SuccessAdapter{})
	if generation != 1 {
		t.Fatalf("first anomalous close generation = %d, want 1", generation)
	}

	batchBefore, err := st.GetBatch(contextB(), batchID)
	if err != nil {
		t.Fatalf("get batch before retry: %v", err)
	}
	verdictsBefore, err := st.ListVerdicts(contextB(), batchID, batchBefore.CurrentEpoch)
	if err != nil {
		t.Fatalf("list verdicts before retry: %v", err)
	}
	membersBefore, err := st.ListIsolationMembers(contextB(), batchID, generation)
	if err != nil {
		t.Fatalf("list isolation members before retry: %v", err)
	}

	cases := []struct {
		name string
	}{
		{name: "first client retry"},
		{name: "repeated client retry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.CloseEpoch(contextB(), batchID, batchBefore.CurrentEpoch); !errors.Is(err, ErrQuorumConflict) {
				t.Fatalf("retried close error = %v, want ErrQuorumConflict", err)
			}

			batchAfter, err := st.GetBatch(contextB(), batchID)
			if err != nil {
				t.Fatalf("get batch after retry: %v", err)
			}
			if batchAfter.Generation != generation {
				t.Fatalf("batch generation after retry = %d, want %d", batchAfter.Generation, generation)
			}
			if _, err := st.GetGeneration(contextB(), batchID, generation+1); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("generation %d lookup error = %v, want store.ErrNotFound", generation+1, err)
			}

			verdictsAfter, err := st.ListVerdicts(contextB(), batchID, batchBefore.CurrentEpoch)
			if err != nil {
				t.Fatalf("list verdicts after retry: %v", err)
			}
			if !reflect.DeepEqual(verdictsAfter, verdictsBefore) {
				t.Fatalf("verdicts changed on retry: before=%+v after=%+v", verdictsBefore, verdictsAfter)
			}
			membersAfter, err := st.ListIsolationMembers(contextB(), batchID, generation)
			if err != nil {
				t.Fatalf("list isolation members after retry: %v", err)
			}
			if !reflect.DeepEqual(membersAfter, membersBefore) {
				t.Fatalf("generation %d isolation changed on retry: before=%+v after=%+v", generation, membersBefore, membersAfter)
			}

			if _, err := svc.PlanRepairs(contextB(), batchID, generation); errors.Is(err, ErrStaleGeneration) {
				t.Fatalf("original generation %d became stale after retried close", generation)
			} else if err != nil {
				t.Fatalf("plan repairs for original generation: %v", err)
			}
		})
	}
}
