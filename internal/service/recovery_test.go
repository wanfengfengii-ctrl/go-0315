package service

import (
	"errors"
	"sync"
	"testing"
)

// setupMissingGeneration creates a batch whose single object is present only
// on n1 (correct content) and missing on n2/n3, yielding a "missing" verdict
// and a new isolation generation.
func setupMissingGeneration(t *testing.T, svc *Service) (string, int64) {
	t.Helper()
	batchID := "batch-m"
	objID := "obj-1"
	length := int64(8)
	correct := digest32(0xAA)

	if err := svc.CreateBatch(contextB(), batchID); err != nil {
		t.Fatalf("create: %v", err)
	}
	objects := []CatalogObject{{ObjectID: objID, ExpectedLength: length, ExpectedRoot: rootOf(length, correct)}}
	nodes := []CatalogNode{
		{NodeID: "n1", FailureDomain: "rack-a", Enabled: true},
		{NodeID: "n2", FailureDomain: "rack-b", Enabled: true},
		{NodeID: "n3", FailureDomain: "rack-c", Enabled: true},
	}
	if err := svc.CatalogBatch(contextB(), batchID, objects, nil, nodes); err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if _, err := svc.FreezeBatch(contextB(), batchID, defaultPolicy(), []string{"alice", "bob"}); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	epoch, err := svc.OpenEpoch(contextB(), batchID)
	if err != nil {
		t.Fatalf("open epoch: %v", err)
	}
	submitEvidence(t, svc, batchID, epoch, objID, "n1", 0, length, correct, "op-n1", 10)

	result, err := svc.CloseEpoch(contextB(), batchID, epoch)
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if result.Generation == 0 {
		t.Fatalf("expected a generation")
	}
	return batchID, result.Generation
}

// completeRecovery confirms all repairs, submits matching samples on every
// node and records two distinct qualified approvals, leaving the generation
// ready for a release terminal decision.
func completeRecovery(t *testing.T, svc *Service, batchID string, generation int64) {
	t.Helper()
	views, err := svc.PlanRepairs(contextB(), batchID, generation)
	if err != nil {
		t.Fatalf("plan repairs: %v", err)
	}
	for _, v := range views {
		if _, err := svc.DispatchRepair(contextB(), v.ID); err != nil {
			t.Fatalf("dispatch %s: %v", v.ID, err)
		}
		if _, err := svc.ReceiptRepair(contextB(), v.ID, v.ExpectedDigest); err != nil {
			t.Fatalf("receipt %s: %v", v.ID, err)
		}
	}
	root := rootOf(8, digest32(0xAA))
	for _, nid := range []string{"n1", "n2", "n3"} {
		if err := svc.SubmitSample(contextB(), batchID, generation, "obj-1", nid, root); err != nil {
			t.Fatalf("sample %s: %v", nid, err)
		}
	}
	if err := svc.SubmitReview(contextB(), batchID, generation, "alice", true); err != nil {
		t.Fatalf("review alice: %v", err)
	}
	if err := svc.SubmitReview(contextB(), batchID, generation, "bob", true); err != nil {
		t.Fatalf("review bob: %v", err)
	}
}

func TestReviewRequiresTwoDistinctQualifiedReviewers(t *testing.T) {
	svc, _ := newTestService(t, SuccessAdapter{})
	batchID, generation := setupMissingGeneration(t, svc)

	// Unqualified reviewer is rejected.
	if err := svc.SubmitReview(contextB(), batchID, generation, "eve", true); !errors.Is(err, ErrNotQualified) {
		t.Fatalf("unqualified review err = %v, want ErrNotQualified", err)
	}

	// A single qualified reviewer is insufficient.
	if err := svc.SubmitReview(contextB(), batchID, generation, "alice", true); err != nil {
		t.Fatalf("review alice: %v", err)
	}
	// Repeating the same reviewer must not add a second approver.
	if err := svc.SubmitReview(contextB(), batchID, generation, "alice", true); err != nil {
		t.Fatalf("repeat alice: %v", err)
	}
	if _, err := svc.Terminal(contextB(), batchID, generation, TerminalRelease); !errors.Is(err, ErrNotReady) {
		t.Fatalf("terminal with one reviewer err = %v, want ErrNotReady", err)
	}

	// A second distinct qualified reviewer satisfies the gate.
	if err := svc.SubmitReview(contextB(), batchID, generation, "bob", true); err != nil {
		t.Fatalf("review bob: %v", err)
	}
}

func TestReleaseIssuesUniqueCredential(t *testing.T) {
	svc, st := newTestService(t, SuccessAdapter{})
	batchID, generation := setupMissingGeneration(t, svc)
	completeRecovery(t, svc, batchID, generation)

	outcome, err := svc.Terminal(contextB(), batchID, generation, TerminalRelease)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if outcome.Credential == "" {
		t.Fatalf("release produced no credential")
	}
	if outcome.Kind != TerminalRelease {
		t.Fatalf("kind = %q, want release", outcome.Kind)
	}

	cred, err := st.GetCredential(contextB(), batchID, generation)
	if err != nil {
		t.Fatalf("get credential: %v", err)
	}
	if len(cred.Credential) != 32 {
		t.Fatalf("credential length = %d, want 32", len(cred.Credential))
	}

	// A second terminal decision must conflict (single-terminal invariant).
	if _, err := svc.Terminal(contextB(), batchID, generation, TerminalRetire); !errors.Is(err, ErrTerminalConflict) {
		t.Fatalf("second terminal err = %v, want ErrTerminalConflict", err)
	}
}

func TestConcurrentTerminalOnlyOneWins(t *testing.T) {
	svc, st := newTestService(t, SuccessAdapter{})
	batchID, generation := setupMissingGeneration(t, svc)
	completeRecovery(t, svc, batchID, generation)

	kinds := []TerminalDecisionKind{TerminalRelease, TerminalQuarantine, TerminalRetire}
	var wg sync.WaitGroup
	results := make(chan error, len(kinds))
	for _, k := range kinds {
		wg.Add(1)
		go func(k TerminalDecisionKind) {
			defer wg.Done()
			_, err := svc.Terminal(contextB(), batchID, generation, k)
			results <- err
		}(k)
	}
	wg.Wait()
	close(results)

	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrTerminalConflict) {
			t.Fatalf("unexpected terminal error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("terminal successes = %d, want exactly 1", successes)
	}

	// At most one recovery credential is ever issued.
	cred, err := st.GetCredential(contextB(), batchID, generation)
	if err == nil && len(cred.Credential) != 32 {
		t.Fatalf("credential length = %d, want 32", len(cred.Credential))
	}
}

func TestStableWindowResetsOnFork(t *testing.T) {
	svc, _ := newTestService(t, SuccessAdapter{})
	batchID := "batch-s"
	length := int64(8)
	correct := digest32(0xAA)
	divergent := digest32(0xEE)

	if err := svc.CreateBatch(contextB(), batchID); err != nil {
		t.Fatalf("create: %v", err)
	}
	objects := []CatalogObject{{ObjectID: "obj-1", ExpectedLength: length, ExpectedRoot: rootOf(length, correct)}}
	nodes := []CatalogNode{
		{NodeID: "n1", FailureDomain: "rack-a", Enabled: true},
		{NodeID: "n2", FailureDomain: "rack-b", Enabled: true},
	}
	policy := defaultPolicy()
	policy.ReplicaQuorum = 1
	policy.StableTicks = 1000
	if err := svc.CatalogBatch(contextB(), batchID, objects, nil, nodes); err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if _, err := svc.FreezeBatch(contextB(), batchID, policy, []string{"alice", "bob"}); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	epoch, _ := svc.OpenEpoch(contextB(), batchID)
	// n1 corrupt, n2 correct, quorum 1 => winner corrupt (forked).
	submitEvidence(t, svc, batchID, epoch, "obj-1", "n1", 0, length, divergent, "s-n1", 10)
	submitEvidence(t, svc, batchID, epoch, "obj-1", "n2", 0, length, correct, "s-n2", 11)
	result, err := svc.CloseEpoch(contextB(), batchID, epoch)
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	generation := result.Generation

	// A divergent sample resets the stable window.
	if err := svc.SubmitSample(contextB(), batchID, generation, "obj-1", "n1", rootOf(length, divergent)); err != nil {
		t.Fatalf("divergent sample: %v", err)
	}
	gen, err := svc.Store().GetGeneration(contextB(), batchID, generation)
	if err != nil {
		t.Fatalf("get generation: %v", err)
	}
	current, _ := svc.Store().CurrentTick(contextB())
	if current-gen.StableSinceTick >= policy.StableTicks {
		t.Fatalf("stable window not reset after divergent sample")
	}
}
