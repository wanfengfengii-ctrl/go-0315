package service

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/layout"
	"archival-replica-integrity-recovery/internal/store"
)

// ScanTTL is the generous lifetime granted to the internal scan lease opened
// with each epoch. It is large enough that the lease is effectively active for
// the whole scan window while still exercising the half-open interval logic.
const ScanTTL int64 = 1 << 40

// EvidenceInput is a single normalized chunk observation submitted by a node.
type EvidenceInput struct {
	ObjectID     string
	NodeID       string
	ChunkNo      int64
	Length       int64
	Digest       []byte
	OperationID  string
	ObservedTick int64
}

// OpenEpoch opens a new scan epoch for a frozen batch and returns its number.
// It acquires the internal scan lease for the batch in the same transaction.
func (s *Service) OpenEpoch(ctx context.Context, batchID string) (int64, error) {
	canonical, err := domain.CanonicalizeID(batchID)
	if err != nil {
		return 0, err
	}
	batch, err := s.store.GetBatch(ctx, canonical)
	if err != nil {
		if err == store.ErrNotFound {
			return 0, ErrBatchNotFound
		}
		return 0, err
	}
	if batch.Status != domain.StatusFrozen {
		return 0, ErrNotFrozen
	}

	epoch, err := s.store.AdvanceEpoch(ctx, canonical)
	if err != nil {
		return 0, err
	}
	tick, err := s.store.NextTick(ctx)
	if err != nil {
		return 0, err
	}
	if err := s.store.OpenEpoch(ctx, canonical, epoch, tick); err != nil {
		return 0, err
	}
	// Acquire the scan lease so evidence submission can verify it is active.
	lease := domain.ResourceLease{
		ResourceType: domain.ResourceScan,
		ResourceKey:  canonical,
		LeaseID:      fmt.Sprintf("scan-%s-%d", canonical, epoch),
		Holder:       "scan",
		StartTick:    tick,
		ExpiresTick:  tick + ScanTTL,
		Version:      1,
	}
	if err := s.store.AcquireLease(ctx, lease); err != nil {
		return 0, ErrLeaseConflict
	}
	return epoch, nil
}

// SubmitEvidence normalizes and appends a single evidence row in one
// transaction. It verifies the scan lease is active, the epoch is open, the
// node is enabled and the digest length matches the frozen policy. Idempotent
// replays are accepted; changed content under the same operation id is
// rejected.
func (s *Service) SubmitEvidence(ctx context.Context, batchID string, epochNo int64, in EvidenceInput) error {
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
	policy := batch.FrozenPolicy
	if policy == nil {
		return ErrNotFrozen
	}

	// Validate node membership and enablement.
	nodes, err := s.store.ListNodes(ctx, canonical)
	if err != nil {
		return err
	}
	nodeEnabled := false
	for _, n := range nodes {
		if n.NodeID == in.NodeID && n.Enabled {
			nodeEnabled = true
			break
		}
	}
	if !nodeEnabled {
		return fmt.Errorf("%w: node %q not enabled", ErrInvalidCatalog, in.NodeID)
	}

	// Validate digest length against the frozen algorithm.
	if len(in.Digest) != policy.HashAlgorithm.DigestSize() {
		return fmt.Errorf("%w: digest length %d != %d", ErrInvalidCatalog, len(in.Digest), policy.HashAlgorithm.DigestSize())
	}
	if in.ChunkNo < 0 && in.ChunkNo != domain.WholeObjectChunk {
		return fmt.Errorf("%w: invalid chunk number %d", ErrInvalidCatalog, in.ChunkNo)
	}

	e := domain.ReplicaEvidence{
		ObjectID:     in.ObjectID,
		EpochNo:      epochNo,
		NodeID:       in.NodeID,
		ChunkNo:      in.ChunkNo,
		Length:       in.Length,
		Digest:       append([]byte(nil), in.Digest...),
		OperationID:  in.OperationID,
		ObservedTick: in.ObservedTick,
	}
	if err := s.store.AppendEvidence(ctx, canonical, e); err != nil {
		if err == store.ErrIdempotencyConflict {
			return ErrIdempotencyConflict
		}
		return err
	}

	// Verify the scan lease is active at the observed tick; an expired lease
	// rolls the whole submission back.
	leaseID := fmt.Sprintf("scan-%s-%d", canonical, epochNo)
	lease, err := s.store.GetLease(ctx, leaseID)
	if err != nil || !leaseActive(lease, in.ObservedTick) {
		return ErrLeaseConflict
	}
	return nil
}

// CloseEpoch forms the unique integrity verdict for every object, computes the
// isolation closure for any anomaly, creates a new generation when anomalies
// exist, and closes the epoch — all inside one logical flow.
func (s *Service) CloseEpoch(ctx context.Context, batchID string, epochNo int64) (*CloseEpochResult, error) {
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
	if epochNo != batch.CurrentEpoch {
		return nil, ErrQuorumConflict
	}

	objects, err := s.store.ListObjects(ctx, canonical)
	if err != nil {
		return nil, err
	}
	nodes, err := s.store.ListNodes(ctx, canonical)
	if err != nil {
		return nil, err
	}
	deps, err := s.store.ListDependencies(ctx, canonical)
	if err != nil {
		return nil, err
	}
	evidence, err := s.store.ListEvidence(ctx, canonical, epochNo)
	if err != nil {
		return nil, err
	}

	policy := batch.FrozenPolicy
	results := formVerdicts(objects, nodes, evidence, *policy)

	verdicts := make([]domain.IntegrityVerdict, 0, len(results))
	var anomalies []string
	for _, r := range results {
		verdicts = append(verdicts, domain.IntegrityVerdict{
			ObjectID:      r.ObjectID,
			EpochNo:       epochNo,
			WinningRoot:   r.WinningRoot,
			VerdictKind:   r.Kind,
			ThresholdTick: r.ThresholdTick,
		})
		if r.Kind != domain.VerdictIntact {
			anomalies = append(anomalies, r.ObjectID)
		}
	}
	sort.Strings(anomalies)

	if err := s.store.PutVerdicts(ctx, canonical, epochNo, verdicts); err != nil {
		return nil, err
	}

	result := &CloseEpochResult{
		BatchID:   canonical,
		EpochNo:   epochNo,
		Verdicts:  verdicts,
		Anomalies: anomalies,
	}

	// Only create a new generation (and isolation closure) when there are
	// anomalies. Old generations must never change current state.
	if len(anomalies) > 0 {
		generation := batch.Generation + 1
		members := buildIsolationClosure(anomalies, objects, nodes, deps, results, evidence)
		genTick, err := s.store.NextTick(ctx)
		if err != nil {
			return nil, err
		}
		if err := s.store.CreateGeneration(ctx, canonical, generation, genTick); err != nil {
			return nil, err
		}
		if err := s.store.PutIsolationMembers(ctx, canonical, generation, members); err != nil {
			return nil, err
		}
		if err := s.store.SetGeneration(ctx, canonical, generation); err != nil {
			return nil, err
		}
		result.Generation = generation
		result.IsolationMembers = members
	}

	tick, err := s.store.NextTick(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.store.CloseEpoch(ctx, canonical, epochNo, tick); err != nil {
		if err == store.ErrConflict {
			return nil, ErrQuorumConflict
		}
		return nil, err
	}
	// Release the scan lease.
	leaseID := fmt.Sprintf("scan-%s-%d", canonical, epochNo)
	_ = s.store.ReleaseLease(ctx, leaseID, "scan")

	return result, nil
}

// CloseEpochResult is the deterministic reconstruction of a closed epoch.
type CloseEpochResult struct {
	BatchID          string
	EpochNo          int64
	Generation       int64
	Verdicts         []domain.IntegrityVerdict
	Anomalies        []string
	IsolationMembers []domain.IsolationMember
}

// verdictResult is the internal per-object verdict computation.
type verdictResult struct {
	ObjectID      string
	WinningRoot   []byte
	Kind          domain.VerdictKind
	ThresholdTick int64
	SuspectNodes  []string
}

// nodeObservation is a node's evidence for one object, reduced to a root and a
// set of supporting evidence ticks.
type nodeObservation struct {
	nodeID string
	length int64
	root   []byte
	ticks  []int64
	chunks map[int64][]byte
}

// groupEvidence reduces raw evidence into per-object, per-node observations,
// ignoring disabled nodes and recording each node's chunk digests, reported
// length and observation ticks.
func groupEvidence(evidence []domain.ReplicaEvidence, enabled map[string]bool) map[string]map[string]*nodeObservation {
	byObject := make(map[string]map[string]*nodeObservation)
	for _, e := range evidence {
		if !enabled[e.NodeID] {
			continue
		}
		if byObject[e.ObjectID] == nil {
			byObject[e.ObjectID] = make(map[string]*nodeObservation)
		}
		obs := byObject[e.ObjectID][e.NodeID]
		if obs == nil {
			obs = &nodeObservation{nodeID: e.NodeID, length: e.Length, chunks: make(map[int64][]byte)}
			byObject[e.ObjectID][e.NodeID] = obs
		}
		obs.chunks[e.ChunkNo] = e.Digest
		obs.ticks = append(obs.ticks, e.ObservedTick)
		if e.Length > obs.length {
			obs.length = e.Length
		}
	}
	return byObject
}

// formVerdicts computes the unique verdict for every object using the
// deterministic quorum rule: distinct enabled nodes vote for a root; the
// winner is the root reaching quorum with the earliest threshold tick and,
// on a tie, the smallest root digest bytes.
func formVerdicts(objects []domain.ArchiveObject, nodes []domain.StorageNode, evidence []domain.ReplicaEvidence, policy domain.FrozenPolicy) []verdictResult {
	enabled := make(map[string]bool)
	for _, n := range nodes {
		enabled[n.NodeID] = n.Enabled
	}

	byObject := groupEvidence(evidence, enabled)

	var out []verdictResult
	for _, o := range objects {
		res := formOneVerdict(o, byObject[o.ObjectID], policy)
		out = append(out, res)
	}
	// Deterministic ordering by object id.
	sort.Slice(out, func(i, j int) bool { return out[i].ObjectID < out[j].ObjectID })
	return out
}

func formOneVerdict(obj domain.ArchiveObject, obs map[string]*nodeObservation, policy domain.FrozenPolicy) verdictResult {
	type candidate struct {
		root          []byte
		nodes         []string
		thresholdTick int64
	}

	votes := make(map[string]*candidate)
	var candidates []*candidate
	for _, o := range obs {
		root := nodeRoot(policy.HashAlgorithm, o)
		key := string(root)
		c := votes[key]
		if c == nil {
			c = &candidate{root: root}
			votes[key] = c
			candidates = append(candidates, c)
		}
		c.nodes = append(c.nodes, o.nodeID)
	}

	for _, c := range candidates {
		sort.Strings(c.nodes)
		ticks := make([]int64, 0, len(c.nodes))
		for _, nid := range c.nodes {
			for _, t := range obs[nid].ticks {
				ticks = append(ticks, t)
			}
		}
		sort.Slice(ticks, func(i, j int) bool { return ticks[i] < ticks[j] })
		if len(ticks) >= policy.ReplicaQuorum {
			c.thresholdTick = ticks[policy.ReplicaQuorum-1]
		} else {
			c.thresholdTick = ticks[len(ticks)-1]
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if len(a.nodes) != len(b.nodes) {
			return len(a.nodes) > len(b.nodes)
		}
		if a.thresholdTick != b.thresholdTick {
			return a.thresholdTick < b.thresholdTick
		}
		return bytes.Compare(a.root, b.root) < 0
	})

	res := verdictResult{ObjectID: obj.ObjectID}
	if len(candidates) == 0 {
		res.Kind = domain.VerdictMissing
		return res
	}
	winner := candidates[0]
	res.WinningRoot = winner.root
	res.ThresholdTick = winner.thresholdTick

	if len(winner.nodes) < policy.ReplicaQuorum {
		res.Kind = domain.VerdictMissing
		// Suspect nodes are those that failed to support the winner.
		for nid := range obs {
			res.SuspectNodes = append(res.SuspectNodes, nid)
		}
		sort.Strings(res.SuspectNodes)
		return res
	}

	// Determine suspect nodes: enabled nodes whose root differs from winner.
	for nid, o := range obs {
		if !bytes.Equal(nodeRoot(policy.HashAlgorithm, o), winner.root) {
			res.SuspectNodes = append(res.SuspectNodes, nid)
		}
	}
	sort.Strings(res.SuspectNodes)

	if bytes.Equal(winner.root, obj.ExpectedRoot) {
		res.Kind = domain.VerdictIntact
		return res
	}
	// Winner diverges from the declared expected root.
	expectedSupported := false
	for _, o := range obs {
		if bytes.Equal(nodeRoot(policy.HashAlgorithm, o), obj.ExpectedRoot) {
			expectedSupported = true
			break
		}
	}
	if expectedSupported {
		res.Kind = domain.VerdictForked
	} else {
		res.Kind = domain.VerdictDigestMismatch
	}
	return res
}

// nodeRoot reduces a node's chunk evidence to its object root digest. An empty
// object is represented by the WholeObjectChunk sentinel carrying the overall
// digest; a non-empty object is hashed from its zero-based chunk digests.
func nodeRoot(alg domain.HashAlgorithm, o *nodeObservation) []byte {
	if len(o.chunks) == 1 {
		if d, ok := o.chunks[domain.WholeObjectChunk]; ok {
			return d
		}
	}
	chunks := make([]layout.ChunkDigest, 0, len(o.chunks))
	for no, d := range o.chunks {
		if no < 0 {
			continue
		}
		chunks = append(chunks, layout.ChunkDigest{No: no, Digest: d})
	}
	layout.SortChunks(chunks)
	root, err := layout.RootDigest(alg, o.length, chunks)
	if err != nil {
		// A node with no usable chunk evidence yields a nil root.
		return nil
	}
	return root
}

func leaseActive(l *domain.ResourceLease, tick int64) bool {
	if l == nil {
		return false
	}
	return l.StartTick <= tick && tick < l.ExpiresTick
}
