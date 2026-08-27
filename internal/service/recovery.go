package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"archival-replica-integrity-recovery/internal/coverage"
	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/store"
)

// SubmitSample records a post-repair re-read of a target node's root digest.
// A sample that diverges from the object's declared expected root resets the
// generation's stable-window clock, enforcing the documented stability rule.
func (s *Service) SubmitSample(ctx context.Context, batchID string, generation int64, objectID, nodeID string, rootDigest []byte) error {
	canonical, err := domain.CanonicalizeID(batchID)
	if err != nil {
		return err
	}
	batch, err := s.store.GetBatch(ctx, canonical)
	if err != nil {
		if err == store.ErrNotFound {
			return ErrBatchNotFound
		}
		return err
	}
	if batch.Status != domain.StatusFrozen || batch.FrozenPolicy == nil {
		return ErrNotFrozen
	}
	if generation != batch.Generation {
		return ErrStaleGeneration
	}
	if _, err := s.store.GetGeneration(ctx, canonical, generation); err != nil {
		return ErrStaleGeneration
	}
	if len(rootDigest) != batch.FrozenPolicy.HashAlgorithm.DigestSize() {
		return fmt.Errorf("%w: sample digest length", ErrInvalidPolicy)
	}

	tick, err := s.store.NextTick(ctx)
	if err != nil {
		return err
	}
	sample := domain.VerificationSample{
		Generation: generation,
		ObjectID:   objectID,
		NodeID:     nodeID,
		RootDigest: append([]byte(nil), rootDigest...),
		SampleTick: tick,
	}
	if err := s.store.PutSample(ctx, canonical, sample); err != nil {
		return err
	}

	expected, err := s.expectedRoot(ctx, canonical, objectID)
	if err != nil {
		return err
	}
	if !bytes.Equal(rootDigest, expected) {
		if err := s.store.ResetStableSince(ctx, canonical, generation, tick); err != nil {
			return err
		}
	}
	return nil
}

// SubmitReview records a qualified reviewer's decision for a generation.
func (s *Service) SubmitReview(ctx context.Context, batchID string, generation int64, reviewer string, approved bool) error {
	canonical, err := domain.CanonicalizeID(batchID)
	if err != nil {
		return err
	}
	batch, err := s.store.GetBatch(ctx, canonical)
	if err != nil {
		if err == store.ErrNotFound {
			return ErrBatchNotFound
		}
		return err
	}
	if batch.Status != domain.StatusFrozen {
		return ErrNotFrozen
	}
	if generation != batch.Generation {
		return ErrStaleGeneration
	}
	tick, err := s.store.NextTick(ctx)
	if err != nil {
		return err
	}
	if !containsReviewer(batch.FrozenReviewers, reviewer) {
		return ErrNotQualified
	}
	decision := domain.ReviewDecision{
		Generation: generation,
		Reviewer:   reviewer,
		Approved:   approved,
		Tick:       tick,
	}
	return s.store.PutReview(ctx, canonical, decision)
}

// TerminalDecisionKind is the client-facing terminal outcome.
type TerminalDecisionKind string

const (
	TerminalRelease    TerminalDecisionKind = "release"
	TerminalQuarantine TerminalDecisionKind = "quarantine"
	TerminalRetire     TerminalDecisionKind = "retire"
)

// TerminalOutcome is the result of a terminal arbitration request.
type TerminalOutcome struct {
	Kind       TerminalDecisionKind
	Generation int64
	Tick       int64
	Credential string
}

// Terminal atomically competes a release, quarantine or retire decision for a
// generation. Only one terminal decision ever wins; a release additionally
// requires full readiness and issues the unique recovery credential.
func (s *Service) Terminal(ctx context.Context, batchID string, generation int64, kind TerminalDecisionKind) (*TerminalOutcome, error) {
	canonical, err := domain.CanonicalizeID(batchID)
	if err != nil {
		return nil, err
	}
	batch, err := s.store.GetBatch(ctx, canonical)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, ErrBatchNotFound
		}
		return nil, err
	}
	if generation != batch.Generation {
		return nil, ErrStaleGeneration
	}
	if _, err := s.store.GetGeneration(ctx, canonical, generation); err != nil {
		return nil, ErrStaleGeneration
	}
	// The single-terminal invariant: once a decision exists, any further
	// terminal request conflicts regardless of the batch status.
	if _, err := s.store.GetTerminal(ctx, canonical, generation); err == nil {
		return nil, ErrTerminalConflict
	}
	if batch.Status != domain.StatusFrozen {
		return nil, ErrNotFrozen
	}

	if kind == TerminalRelease {
		if err := s.releaseReady(ctx, canonical, batch, generation); err != nil {
			return nil, err
		}
	}

	tick, err := s.store.NextTick(ctx)
	if err != nil {
		return nil, err
	}
	decision := domain.TerminalDecision{
		Generation: generation,
		Kind:       domain.TerminalKind(kind),
		Tick:       tick,
	}
	if err := s.store.PutTerminal(ctx, canonical, decision); err != nil {
		if err == store.ErrConflict {
			return nil, ErrTerminalConflict
		}
		return nil, err
	}

	outcome := &TerminalOutcome{Kind: kind, Generation: generation, Tick: tick}
	if kind == TerminalRelease {
		credential, err := s.issueCredential(ctx, canonical, generation, tick)
		if err != nil {
			return nil, err
		}
		outcome.Credential = credential
		if err := s.store.MarkTerminal(ctx, canonical); err != nil {
			return nil, err
		}
	}
	return outcome, nil
}

// releaseReady enforces the four release gates: all repairs confirmed, every
// isolated object reaches the coverage threshold, the stable window has
// elapsed, and two distinct qualified reviewers approved.
func (s *Service) releaseReady(ctx context.Context, batchID string, batch *store.BatchRecord, generation int64) error {
	// Gate 1: every repair task must be confirmed.
	repairs, err := s.store.ListGenerationRepairs(ctx, batchID, generation)
	if err != nil {
		return err
	}
	for _, r := range repairs {
		if r.State != domain.RepairConfirmed {
			return fmt.Errorf("%w: repair %s not confirmed", ErrNotReady, r.ID)
		}
	}

	// Gate 2: coverage per isolated object.
	members, err := s.store.ListIsolationMembers(ctx, batchID, generation)
	if err != nil {
		return err
	}
	nodes, err := s.store.ListNodes(ctx, batchID)
	if err != nil {
		return err
	}
	enabledCount := 0
	for _, n := range nodes {
		if n.Enabled {
			enabledCount++
		}
	}
	if enabledCount == 0 {
		return fmt.Errorf("%w: no enabled nodes", ErrNotReady)
	}
	samples, err := s.store.ListSamples(ctx, batchID, generation)
	if err != nil {
		return err
	}
	sampleByObjectNode := make(map[string]map[string][]byte)
	for _, sm := range samples {
		if sampleByObjectNode[sm.ObjectID] == nil {
			sampleByObjectNode[sm.ObjectID] = make(map[string][]byte)
		}
		sampleByObjectNode[sm.ObjectID][sm.NodeID] = sm.RootDigest
	}
	for _, m := range members {
		expected, err := s.expectedRoot(ctx, batchID, m.ObjectID)
		if err != nil {
			return err
		}
		effective := 0
		for nodeID, root := range sampleByObjectNode[m.ObjectID] {
			if bytes.Equal(root, expected) {
				effective++
			} else {
				_ = nodeID
			}
		}
		bps, err := coverage.Compute(effective, enabledCount)
		if err != nil {
			return fmt.Errorf("%w: coverage: %v", ErrNotReady, err)
		}
		if bps < batch.FrozenPolicy.CoverageBPS {
			return fmt.Errorf("%w: object %s coverage %d < %d", ErrNotReady, m.ObjectID, bps, batch.FrozenPolicy.CoverageBPS)
		}
	}

	// Gate 3: stable window.
	gen, err := s.store.GetGeneration(ctx, batchID, generation)
	if err != nil {
		return err
	}
	current, err := s.store.CurrentTick(ctx)
	if err != nil {
		return err
	}
	if current-gen.StableSinceTick < batch.FrozenPolicy.StableTicks {
		return fmt.Errorf("%w: stable window %d < %d", ErrNotReady, current-gen.StableSinceTick, batch.FrozenPolicy.StableTicks)
	}

	// Gate 4: two distinct qualified reviewers approved.
	reviews, err := s.store.ListReviews(ctx, batchID, generation)
	if err != nil {
		return err
	}
	approvers := make(map[string]bool)
	for _, r := range reviews {
		if r.Approved {
			approvers[r.Reviewer] = true
		}
	}
	if len(approvers) < 2 {
		return fmt.Errorf("%w: %d approved reviewers < 2", ErrNotReady, len(approvers))
	}
	return nil
}

func (s *Service) issueCredential(ctx context.Context, batchID string, generation, tick int64) (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	credential := domain.RecoveryCredential{
		Generation: generation,
		Credential: buf,
		IssuedTick: tick,
	}
	if err := s.store.PutCredential(ctx, batchID, credential); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func containsReviewer(reviewers []string, reviewer string) bool {
	for _, r := range reviewers {
		if r == reviewer {
			return true
		}
	}
	return false
}

// expectedRoot returns the authority-declared expected root for an object.
func (s *Service) expectedRoot(ctx context.Context, batchID, objectID string) ([]byte, error) {
	o, err := s.store.GetObject(ctx, batchID, objectID)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, ErrBatchNotFound
		}
		return nil, err
	}
	return o.ExpectedRoot, nil
}
