package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/store"
)

// RepairView is a reconstruction of a repair task for API responses.
type RepairView struct {
	ID              string
	Generation      int64
	ObjectID        string
	ChunkNo         int64
	SourceNode      string
	TargetNode      string
	ExpectedDigest  []byte
	State           domain.RepairState
	AttemptNo       int
	NextTick        int64
	FailureCategory domain.RepairFailureCategory
}

// PlanRepairs creates the deterministic chunk repair chain for an isolation
// generation. For each quarantined object it selects a trusted source (a node
// whose evidence matches the winning root and is outside the suspect domain)
// and creates one repair task per chunk per divergent target node. Existing
// tasks are preserved so restart never duplicates an already-planned repair.
func (s *Service) PlanRepairs(ctx context.Context, batchID string, generation int64) ([]RepairView, error) {
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
	if batch.Status != domain.StatusFrozen || batch.FrozenPolicy == nil {
		return nil, ErrNotFrozen
	}
	if generation != batch.Generation {
		return nil, ErrStaleGeneration
	}

	members, err := s.store.ListIsolationMembers(ctx, canonical, generation)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, ErrStaleGeneration
	}
	nodes, err := s.store.ListNodes(ctx, canonical)
	if err != nil {
		return nil, err
	}
	evidence, err := s.store.ListEvidence(ctx, canonical, batch.CurrentEpoch)
	if err != nil {
		return nil, err
	}
	verdicts, err := s.store.ListVerdicts(ctx, canonical, batch.CurrentEpoch)
	if err != nil {
		return nil, err
	}

	enabled := make(map[string]bool)
	nodeDomain := make(map[string]string)
	for _, n := range nodes {
		enabled[n.NodeID] = n.Enabled
		nodeDomain[n.NodeID] = n.FailureDomain
	}
	byObject := groupEvidence(evidence, enabled)

	winningRoot := make(map[string][]byte)
	for _, v := range verdicts {
		winningRoot[v.ObjectID] = v.WinningRoot
	}

	var tasks []store.RepairTaskRecord
	for _, m := range members {
		obs := byObject[m.ObjectID]
		if len(obs) == 0 {
			continue
		}
		root := winningRoot[m.ObjectID]
		source := chooseTrustedSource(obs, root, nodeDomain, batch.FrozenPolicy.HashAlgorithm)
		if source == "" {
			continue
		}
		chunkDigests := obs[source].chunks
		// Skip the whole-object sentinel; it is not a copyable chunk.
		delete(chunkDigests, domain.WholeObjectChunk)
		if len(chunkDigests) == 0 {
			continue
		}

		// Targets are every enabled node (including those with no evidence,
		// which are "missing") whose observed root differs from the winning
		// root, excluding the trusted source itself.
		for nid := range enabled {
			if !enabled[nid] || nid == source {
				continue
			}
			if o := obs[nid]; o != nil && bytes.Equal(nodeRoot(batch.FrozenPolicy.HashAlgorithm, o), root) {
				continue
			}
			chunkNos := sortedChunkNos(chunkDigests)
			for _, cn := range chunkNos {
				tasks = append(tasks, store.RepairTaskRecord{
					ID:             repairTaskID(canonical, generation, m.ObjectID, cn, nid),
					BatchID:        canonical,
					Generation:     generation,
					ObjectID:       m.ObjectID,
					ChunkNo:        cn,
					SourceNode:     source,
					TargetNode:     nid,
					ExpectedDigest: append([]byte(nil), chunkDigests[cn]...),
					State:          domain.RepairPending,
				})
			}
		}
	}

	if err := s.store.CreateRepairTasks(ctx, tasks); err != nil {
		return nil, err
	}
	views, err := s.store.ListGenerationRepairs(ctx, canonical, generation)
	if err != nil {
		return nil, err
	}
	return repairViews(views), nil
}

// chooseTrustedSource selects an enabled node whose evidence matches the
// winning root and that is not itself a suspect node in the object's failure
// domain closure. It returns "" when no eligible source exists.
func chooseTrustedSource(obs map[string]*nodeObservation, root []byte, nodeDomain map[string]string, alg domain.HashAlgorithm) string {
	// Determine the suspect failure domains for this object by inspecting
	// which nodes hold a divergent root.
	suspectDomains := make(map[string]bool)
	for nid, o := range obs {
		if !bytes.Equal(nodeRoot(alg, o), root) {
			if d, ok := nodeDomain[nid]; ok {
				suspectDomains[d] = true
			}
		}
	}

	var candidates []string
	for nid, o := range obs {
		if !bytes.Equal(nodeRoot(alg, o), root) {
			continue
		}
		if d, ok := nodeDomain[nid]; ok && suspectDomains[d] {
			continue
		}
		// Prefer nodes with complete chunk coverage.
		if len(o.chunks) == 0 {
			continue
		}
		candidates = append(candidates, nid)
	}
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

// DispatchRepair records a single external copy attempt for a pending task. It
// marks the task dispatched, invokes the adapter outside any transaction, then
// persists either the failure retry tick or leaves the task awaiting a receipt.
// A failed task whose backoff has elapsed is reactivated to pending first, so
// the same repair id can be re-dispatched once next_tick is reached without a
// restart.
func (s *Service) DispatchRepair(ctx context.Context, repairID string) (RepairView, error) {
	tick, err := s.store.CurrentTick(ctx)
	if err != nil {
		return RepairView{}, err
	}
	if err := s.store.ReactivateFailedRepairs(ctx, tick); err != nil {
		return RepairView{}, err
	}

	task, err := s.store.GetRepairTask(ctx, repairID)
	if err != nil {
		if err == store.ErrNotFound {
			return RepairView{}, ErrRepairNotFound
		}
		return RepairView{}, err
	}
	if task.State != domain.RepairPending {
		return RepairView{}, ErrQuorumConflict
	}

	if err := s.store.MarkDispatched(ctx, repairID); err != nil {
		return RepairView{}, err
	}

	category := s.adapter.Copy(task.SourceNode, task.TargetNode, task.ObjectID, task.ChunkNo)
	if category == "" {
		// Copy succeeded; the task awaits a verified receipt.
		return repairViewFrom(task.ID, domain.RepairDispatched, task), nil
	}

	failTick, err := s.store.NextTick(ctx)
	if err != nil {
		return RepairView{}, err
	}
	next := backoffTick(failTick, task.AttemptNo+1)
	if err := s.store.RecordRepairFailure(ctx, repairID, next, category); err != nil {
		return RepairView{}, err
	}
	updated, _ := s.store.GetRepairTask(ctx, repairID)
	return repairViewFrom(task.ID, domain.RepairFailed, updated), nil
}

// ReceiptRepair verifies a post-copy receipt digest against the task's
// expected digest. A match confirms the repair; a mismatch records a
// deterministic digest-mismatch retry.
func (s *Service) ReceiptRepair(ctx context.Context, repairID string, observedDigest []byte) (RepairView, error) {
	task, err := s.store.GetRepairTask(ctx, repairID)
	if err != nil {
		if err == store.ErrNotFound {
			return RepairView{}, ErrRepairNotFound
		}
		return RepairView{}, err
	}
	if task.State != domain.RepairDispatched {
		return RepairView{}, ErrQuorumConflict
	}

	if bytes.Equal(observedDigest, task.ExpectedDigest) {
		if err := s.store.ConfirmRepair(ctx, repairID); err != nil {
			return RepairView{}, err
		}
		updated, _ := s.store.GetRepairTask(ctx, repairID)
		return repairViewFrom(task.ID, domain.RepairConfirmed, updated), nil
	}

	tick, err := s.store.NextTick(ctx)
	if err != nil {
		return RepairView{}, err
	}
	next := backoffTick(tick, task.AttemptNo+1)
	if err := s.store.RecordRepairFailure(ctx, repairID, next, domain.FailureDigestMismatch); err != nil {
		return RepairView{}, err
	}
	updated, _ := s.store.GetRepairTask(ctx, repairID)
	return repairViewFrom(task.ID, domain.RepairFailed, updated), nil
}

// ListRepairs returns the repair chain for a generation in deterministic order.
func (s *Service) ListRepairs(ctx context.Context, batchID string, generation int64) ([]RepairView, error) {
	canonical, err := domain.CanonicalizeID(batchID)
	if err != nil {
		return nil, err
	}
	rows, err := s.store.ListGenerationRepairs(ctx, canonical, generation)
	if err != nil {
		return nil, err
	}
	return repairViews(rows), nil
}

// PendingRepairs returns every repair task eligible for dispatch at the
// current logical tick. Before reading, it reactivates failed tasks whose
// deterministic backoff has already elapsed so a running service keeps
// retrying once the next_tick is reached, rather than requiring a restart.
func (s *Service) PendingRepairs(ctx context.Context) ([]RepairView, error) {
	tick, err := s.store.CurrentTick(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.store.ReactivateFailedRepairs(ctx, tick); err != nil {
		return nil, err
	}
	rows, err := s.store.ListPendingRepairs(ctx, tick)
	if err != nil {
		return nil, err
	}
	return repairViews(rows), nil
}

// backoffTick computes the deterministic retry tick for an attempt number. The
// delay doubles each attempt and is capped, so the same (tick, attempt) pair
// always yields the same next tick.
func backoffTick(now int64, attempt int) int64 {
	exp := attempt - 1
	if exp < 0 {
		exp = 0
	}
	if exp > 20 {
		exp = 20
	}
	return now + (1 << exp)
}

func repairTaskID(batchID string, generation int64, objectID string, chunkNo int64, target string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%d\x00%s", batchID, generation, objectID, chunkNo, target)))
	return hex.EncodeToString(sum[:16])
}

func sortedChunkNos(chunks map[int64][]byte) []int64 {
	out := make([]int64, 0, len(chunks))
	for no := range chunks {
		out = append(out, no)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func repairViews(rows []store.RepairTaskRecord) []RepairView {
	out := make([]RepairView, 0, len(rows))
	for _, r := range rows {
		out = append(out, RepairView{
			ID:              r.ID,
			Generation:      r.Generation,
			ObjectID:        r.ObjectID,
			ChunkNo:         r.ChunkNo,
			SourceNode:      r.SourceNode,
			TargetNode:      r.TargetNode,
			ExpectedDigest:  append([]byte(nil), r.ExpectedDigest...),
			State:           r.State,
			AttemptNo:       r.AttemptNo,
			NextTick:        r.NextTick,
			FailureCategory: r.FailureCategory,
		})
	}
	return out
}

func repairViewFrom(id string, state domain.RepairState, r *store.RepairTaskRecord) RepairView {
	if r == nil {
		return RepairView{ID: id, State: state}
	}
	return RepairView{
		ID:              r.ID,
		Generation:      r.Generation,
		ObjectID:        r.ObjectID,
		ChunkNo:         r.ChunkNo,
		SourceNode:      r.SourceNode,
		TargetNode:      r.TargetNode,
		ExpectedDigest:  append([]byte(nil), r.ExpectedDigest...),
		State:           r.State,
		AttemptNo:       r.AttemptNo,
		NextTick:        r.NextTick,
		FailureCategory: r.FailureCategory,
	}
}
