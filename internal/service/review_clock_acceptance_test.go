package service

import (
	"errors"
	"fmt"
	"testing"

	"archival-replica-integrity-recovery/internal/store"
)

func TestModel_UnqualifiedReviewsDoNotAdvanceStableWindow(t *testing.T) {
	cases := []struct {
		name               string
		unqualifiedReviews []string
	}{
		{name: "single rejected review", unqualifiedReviews: []string{"eve"}},
		{name: "repeated rejected reviews", unqualifiedReviews: []string{"eve", "mallory", "eve"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, st := newTestService(t, SuccessAdapter{})
			batchID := fmt.Sprintf("review-clock-%d", len(tc.unqualifiedReviews))
			const objectID = "obj-1"
			const length int64 = 8
			correct := digest32(0xAA)
			missing := digest32(0xEE)
			policy := defaultPolicy()
			policy.StableTicks = int64(5 + len(tc.unqualifiedReviews))

			if err := svc.CreateBatch(contextB(), batchID); err != nil {
				t.Fatalf("create batch: %v", err)
			}
			objects := []CatalogObject{{ObjectID: objectID, ExpectedLength: length, ExpectedRoot: rootOf(length, correct)}}
			nodes := []CatalogNode{
				{NodeID: "n1", FailureDomain: "rack-a", Enabled: true},
				{NodeID: "n2", FailureDomain: "rack-b", Enabled: true},
				{NodeID: "n3", FailureDomain: "rack-c", Enabled: true},
			}
			if err := svc.CatalogBatch(contextB(), batchID, objects, nil, nodes); err != nil {
				t.Fatalf("catalog batch: %v", err)
			}
			if _, err := svc.FreezeBatch(contextB(), batchID, policy, []string{"alice", "bob"}); err != nil {
				t.Fatalf("freeze batch: %v", err)
			}
			epoch, err := svc.OpenEpoch(contextB(), batchID)
			if err != nil {
				t.Fatalf("open epoch: %v", err)
			}
			submitEvidence(t, svc, batchID, epoch, objectID, "n1", 0, length, correct, "op-n1", 10)
			closed, err := svc.CloseEpoch(contextB(), batchID, epoch)
			if err != nil {
				t.Fatalf("close epoch: %v", err)
			}
			generation := closed.Generation

			repairs, err := svc.PlanRepairs(contextB(), batchID, generation)
			if err != nil {
				t.Fatalf("plan repairs: %v", err)
			}
			for _, repair := range repairs {
				if _, err := svc.DispatchRepair(contextB(), repair.ID); err != nil {
					t.Fatalf("dispatch repair %s: %v", repair.ID, err)
				}
				if _, err := svc.ReceiptRepair(contextB(), repair.ID, repair.ExpectedDigest); err != nil {
					t.Fatalf("confirm repair %s: %v", repair.ID, err)
				}
			}

			root := rootOf(length, correct)
			for _, nodeID := range []string{"n1", "n2", "n3"} {
				if err := svc.SubmitSample(contextB(), batchID, generation, objectID, nodeID, root); err != nil {
					t.Fatalf("matching sample %s: %v", nodeID, err)
				}
			}
			if err := svc.SubmitSample(contextB(), batchID, generation, objectID, "n1", rootOf(length, missing)); err != nil {
				t.Fatalf("divergent sample: %v", err)
			}
			if err := svc.SubmitSample(contextB(), batchID, generation, objectID, "n1", root); err != nil {
				t.Fatalf("corrected sample: %v", err)
			}

			var previousAliceTick int64
			for _, approved := range []bool{true, false, true} {
				if err := svc.SubmitReview(contextB(), batchID, generation, "alice", approved); err != nil {
					t.Fatalf("update alice to approved=%v: %v", approved, err)
				}
				reviews, err := st.ListReviews(contextB(), batchID, generation)
				if err != nil {
					t.Fatalf("list reviews: %v", err)
				}
				if len(reviews) != 1 || reviews[0].Reviewer != "alice" || reviews[0].Approved != approved {
					t.Fatalf("alice update = %+v, want one decision with approved=%v", reviews, approved)
				}
				if reviews[0].Tick <= previousAliceTick {
					t.Fatalf("alice tick = %d, want greater than %d", reviews[0].Tick, previousAliceTick)
				}
				previousAliceTick = reviews[0].Tick
			}
			if err := svc.SubmitReview(contextB(), batchID, generation, "bob", true); err != nil {
				t.Fatalf("approve bob: %v", err)
			}

			beforeRejected, err := st.CurrentTick(contextB())
			if err != nil {
				t.Fatalf("current tick before rejected reviews: %v", err)
			}
			for _, reviewer := range tc.unqualifiedReviews {
				if err := svc.SubmitReview(contextB(), batchID, generation, reviewer, true); !errors.Is(err, ErrNotQualified) {
					t.Fatalf("review by %q error = %v, want ErrNotQualified", reviewer, err)
				}
			}
			afterRejected, err := st.CurrentTick(contextB())
			if err != nil {
				t.Fatalf("current tick after rejected reviews: %v", err)
			}
			if afterRejected != beforeRejected {
				t.Fatalf("rejected reviews advanced tick from %d to %d", beforeRejected, afterRejected)
			}
			reviews, err := st.ListReviews(contextB(), batchID, generation)
			if err != nil {
				t.Fatalf("list final reviews: %v", err)
			}
			if len(reviews) != 2 || !reviews[0].Approved || !reviews[1].Approved {
				t.Fatalf("qualified decisions after rejected reviews = %+v, want two approvals", reviews)
			}

			if outcome, err := svc.Terminal(contextB(), batchID, generation, TerminalRelease); !errors.Is(err, ErrNotReady) || outcome != nil {
				t.Fatalf("early release outcome=%+v error=%v, want nil ErrNotReady", outcome, err)
			}
			if _, err := st.GetTerminal(contextB(), batchID, generation); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("terminal persisted after early release attempt: %v", err)
			}
			if _, err := st.GetCredential(contextB(), batchID, generation); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("credential persisted after early release attempt: %v", err)
			}

			lastTick := afterRejected
			for range tc.unqualifiedReviews {
				if err := svc.SubmitReview(contextB(), batchID, generation, "alice", true); err != nil {
					t.Fatalf("advance stable window with qualified review: %v", err)
				}
				current, err := st.CurrentTick(contextB())
				if err != nil {
					t.Fatalf("current tick after qualified update: %v", err)
				}
				if current <= lastTick {
					t.Fatalf("qualified review tick = %d, want greater than %d", current, lastTick)
				}
				lastTick = current
			}
			outcome, err := svc.Terminal(contextB(), batchID, generation, TerminalRelease)
			if err != nil {
				t.Fatalf("release after stable window: %v", err)
			}
			if outcome.Credential == "" || outcome.Kind != TerminalRelease {
				t.Fatalf("release outcome = %+v, want release with credential", outcome)
			}
		})
	}
}
