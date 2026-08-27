package service

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
)

func submitEvidence(t *testing.T, svc *Service, batchID string, epoch int64, objectID, nodeID string, chunkNo, length int64, digest []byte, op string, tick int64) {
	t.Helper()
	err := svc.SubmitEvidence(contextB(), batchID, epoch, EvidenceInput{
		ObjectID: objectID, NodeID: nodeID, ChunkNo: chunkNo, Length: length,
		Digest: digest, OperationID: op, ObservedTick: tick,
	})
	if err != nil {
		t.Fatalf("submit evidence %s/%s: %v", nodeID, op, err)
	}
}

func TestQuorumProducesSingleWinner(t *testing.T) {
	svc, _ := newTestService(t, SuccessAdapter{})
	setupFrozenBatch(t, svc, "b1", "obj-1", 8, 0xAA, threeNodes(), defaultPolicy())

	epoch, err := svc.OpenEpoch(contextB(), "b1")
	if err != nil {
		t.Fatalf("open epoch: %v", err)
	}

	// n1 and n2 support root A; n3 supports root B. Quorum is 2, so A wins.
	digestA := digest32(0xAA)
	digestB := digest32(0xBB)
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n1", 0, 8, digestA, "op-n1", 10)
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n2", 0, 8, digestA, "op-n2", 11)
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n3", 0, 8, digestB, "op-n3", 12)

	result, err := svc.CloseEpoch(contextB(), "b1", epoch)
	if err != nil {
		t.Fatalf("close epoch: %v", err)
	}

	var objVerdict *domain.IntegrityVerdict
	for i := range result.Verdicts {
		if result.Verdicts[i].ObjectID == "obj-1" {
			objVerdict = &result.Verdicts[i]
		}
	}
	if objVerdict == nil {
		t.Fatalf("no verdict for obj-1")
	}
	if objVerdict.VerdictKind != domain.VerdictIntact {
		t.Fatalf("kind = %q, want intact", objVerdict.VerdictKind)
	}
	if !bytes.Equal(objVerdict.WinningRoot, rootOf(8, digestA)) {
		t.Fatalf("winning root = %x, want root of A", objVerdict.WinningRoot)
	}
	if len(result.Anomalies) != 0 {
		t.Fatalf("anomalies = %v, want none", result.Anomalies)
	}
}

func TestQuorumTieBreakIsDeterministic(t *testing.T) {
	// Four nodes, quorum 2: two roots each reach quorum. The earlier threshold
	// tick must win deterministically.
	svc, _ := newTestService(t, SuccessAdapter{})
	nodes := []domain.StorageNode{
		{NodeID: "n1", FailureDomain: "rack-a", Enabled: true},
		{NodeID: "n2", FailureDomain: "rack-b", Enabled: true},
		{NodeID: "n3", FailureDomain: "rack-c", Enabled: true},
		{NodeID: "n4", FailureDomain: "rack-d", Enabled: true},
	}
	policy := defaultPolicy()
	setupFrozenBatch(t, svc, "b1", "obj-1", 8, 0xAA, nodes, policy)

	epoch, _ := svc.OpenEpoch(contextB(), "b1")
	digestA := digest32(0xAA)
	digestB := digest32(0xBB)
	// Root B reaches quorum first (ticks 10,11) vs root A (ticks 20,21).
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n1", 0, 8, digestB, "op-n1", 10)
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n2", 0, 8, digestB, "op-n2", 11)
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n3", 0, 8, digestA, "op-n3", 20)
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n4", 0, 8, digestA, "op-n4", 21)

	result, err := svc.CloseEpoch(contextB(), "b1", epoch)
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(result.Verdicts) != 1 {
		t.Fatalf("verdict count = %d, want 1", len(result.Verdicts))
	}
	if !bytes.Equal(result.Verdicts[0].WinningRoot, rootOf(8, digestB)) {
		t.Fatalf("winner = %x, want root B (earlier threshold)", result.Verdicts[0].WinningRoot)
	}
}

func TestEvidenceIdempotentReplay(t *testing.T) {
	svc, st := newTestService(t, SuccessAdapter{})
	setupFrozenBatch(t, svc, "b1", "obj-1", 8, 0xAA, threeNodes(), defaultPolicy())
	epoch, _ := svc.OpenEpoch(contextB(), "b1")

	digestA := digest32(0xAA)
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n1", 0, 8, digestA, "op-n1", 10)
	// Replay the same operation with identical content.
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n1", 0, 8, digestA, "op-n1", 10)

	evidence, err := st.ListEvidence(contextB(), "b1", epoch)
	if err != nil {
		t.Fatalf("list evidence: %v", err)
	}
	count := 0
	for _, e := range evidence {
		if e.NodeID == "n1" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("n1 evidence rows = %d, want 1 after idempotent replay", count)
	}
}

func TestEvidenceIdempotencyConflictRollsBack(t *testing.T) {
	svc, st := newTestService(t, SuccessAdapter{})
	setupFrozenBatch(t, svc, "b1", "obj-1", 8, 0xAA, threeNodes(), defaultPolicy())
	epoch, _ := svc.OpenEpoch(contextB(), "b1")

	digestA := digest32(0xAA)
	digestB := digest32(0xBB)
	submitEvidence(t, svc, "b1", epoch, "obj-1", "n1", 0, 8, digestA, "op-n1", 10)

	err := svc.SubmitEvidence(contextB(), "b1", epoch, EvidenceInput{
		ObjectID: "obj-1", NodeID: "n1", ChunkNo: 0, Length: 8,
		Digest: digestB, OperationID: "op-n1", ObservedTick: 10,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}

	evidence, _ := st.ListEvidence(contextB(), "b1", epoch)
	count := 0
	for _, e := range evidence {
		if e.NodeID == "n1" {
			count++
			if !bytes.Equal(e.Digest, digestA) {
				t.Fatalf("stored digest changed to %x after conflict", e.Digest)
			}
		}
	}
	if count != 1 {
		t.Fatalf("n1 evidence rows = %d, want 1 (no new vote)", count)
	}
}

func TestLeaseBoundaryAndConcurrentCompetition(t *testing.T) {
	svc, _ := newTestService(t, SuccessAdapter{})

	// A lease active at [0,10) is not active at tick 10 (half-open boundary).
	l, err := svc.AcquireLease(contextB(), domain.ResourceWrite, "obj-x", "a", 0, 10)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := svc.RenewLease(contextB(), l.LeaseID, "a", 10, 20); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("renew at boundary err = %v, want ErrLeaseConflict", err)
	}

	// Concurrent holders compete for the same resource; only one wins.
	var wg sync.WaitGroup
	wins := make(chan string, 2)
	for _, holder := range []string{"h1", "h2", "h3"} {
		wg.Add(1)
		go func(h string) {
			defer wg.Done()
			if _, err := svc.AcquireLease(contextB(), domain.ResourceTerminal, "batch-z", h, 100, 200); err == nil {
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
		t.Fatalf("winners = %v, want exactly one", winners)
	}
}
