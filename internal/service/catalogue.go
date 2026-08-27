package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/layout"
	"archival-replica-integrity-recovery/internal/store"
)

// CatalogObject is a single archived object declaration submitted with the
// catalogue. ExpectedRoot is the authority-declared known-good root digest.
type CatalogObject struct {
	ObjectID       string
	ExpectedLength int64
	ExpectedRoot   []byte
}

// CatalogDependency is a directed dependency edge submitted with the catalogue.
type CatalogDependency struct {
	FromObject string
	ToObject   string
	Reason     string
}

// CatalogNode is a storage node declaration submitted with the catalogue.
type CatalogNode struct {
	NodeID        string
	FailureDomain string
	Enabled       bool
}

// CreateBatch creates a new draft preservation batch.
func (s *Service) CreateBatch(ctx context.Context, batchID string) error {
	canonical, err := domain.CanonicalizeID(batchID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCatalog, err)
	}
	tick, err := s.store.NextTick(ctx)
	if err != nil {
		return err
	}
	if err := s.store.CreateBatch(ctx, canonical, tick); err != nil {
		if err == store.ErrAlreadyExists {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

// CatalogBatch validates and stores the object catalogue, dependency edges and
// node roster. It is only valid while the batch is a draft. All three tables are
// replaced inside a single database transaction: the draft status is checked
// inside that transaction, so a frozen batch is rejected with ErrAlreadyFrozen
// and the existing catalogue is left completely untouched. This guarantees a
// failed update never leaves a partially replaced catalogue whose objects,
// dependencies and node roster disagree with each other or with the frozen
// policy digest.
func (s *Service) CatalogBatch(ctx context.Context, batchID string, objects []CatalogObject, deps []CatalogDependency, nodes []CatalogNode) error {
	canonical, err := domain.CanonicalizeID(batchID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCatalog, err)
	}
	batch, err := s.store.GetBatch(ctx, canonical)
	if err != nil {
		if err == store.ErrNotFound {
			return ErrBatchNotFound
		}
		return err
	}
	if batch.Status != domain.StatusDraft {
		return ErrAlreadyFrozen
	}

	domainObjects, err := s.validateCatalog(objects, deps, nodes)
	if err != nil {
		return err
	}
	domainDeps := make([]domain.ObjectDependency, len(deps))
	for i, d := range deps {
		domainDeps[i] = domain.ObjectDependency{FromObject: d.FromObject, ToObject: d.ToObject, Reason: d.Reason}
	}
	domainNodes := make([]domain.StorageNode, len(nodes))
	for i, n := range nodes {
		domainNodes[i] = domain.StorageNode{NodeID: n.NodeID, FailureDomain: n.FailureDomain, Enabled: n.Enabled}
	}
	if err := s.store.PutCatalog(ctx, canonical, domainObjects, domainDeps, domainNodes); err != nil {
		if err == store.ErrAlreadyFrozen {
			return ErrAlreadyFrozen
		}
		if err == store.ErrNotFound {
			return ErrBatchNotFound
		}
		return err
	}
	return nil
}

// validateCatalog canonicalizes and validates the whole catalogue, returning
// normalized domain objects. It enforces object-id uniqueness within a batch,
// non-negative lengths, canonical identifiers and distinct node identifiers.
func (s *Service) validateCatalog(objects []CatalogObject, deps []CatalogDependency, nodes []CatalogNode) ([]domain.ArchiveObject, error) {
	if len(objects) == 0 {
		return nil, fmt.Errorf("%w: catalogue must declare at least one object", ErrInvalidCatalog)
	}
	seenObjects := make(map[string]bool)
	out := make([]domain.ArchiveObject, 0, len(objects))
	for _, o := range objects {
		canonical, err := domain.CanonicalizeID(o.ObjectID)
		if err != nil {
			return nil, fmt.Errorf("%w: object %q: %v", ErrInvalidCatalog, o.ObjectID, err)
		}
		if seenObjects[canonical] {
			return nil, fmt.Errorf("%w: duplicate object %q", ErrInvalidCatalog, canonical)
		}
		seenObjects[canonical] = true
		if o.ExpectedLength < 0 {
			return nil, fmt.Errorf("%w: object %q has negative length", ErrInvalidCatalog, canonical)
		}
		out = append(out, domain.ArchiveObject{
			ObjectID:       canonical,
			CanonicalKey:   canonical,
			ExpectedLength: o.ExpectedLength,
			ExpectedRoot:   append([]byte(nil), o.ExpectedRoot...),
		})
	}

	seenNodes := make(map[string]bool)
	for _, n := range nodes {
		canonical, err := domain.ValidateNodeID(n.NodeID)
		if err != nil {
			return nil, fmt.Errorf("%w: node %q: %v", ErrInvalidCatalog, n.NodeID, err)
		}
		if seenNodes[canonical] {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrInvalidCatalog, canonical)
		}
		seenNodes[canonical] = true
		if _, err := domain.ValidateFailureDomain(n.FailureDomain); err != nil {
			return nil, fmt.Errorf("%w: node %q failure domain: %v", ErrInvalidCatalog, n.NodeID, err)
		}
	}

	for _, d := range deps {
		if !seenObjects[d.FromObject] || !seenObjects[d.ToObject] {
			return nil, fmt.Errorf("%w: dependency references unknown object %q->%q", ErrInvalidCatalog, d.FromObject, d.ToObject)
		}
	}
	return out, nil
}

// FreezeBatch validates the policy against the catalogue and atomically freezes
// the batch, generating its immutable policy digest. After freeze the policy,
// objects, nodes and thresholds can no longer be modified.
func (s *Service) FreezeBatch(ctx context.Context, batchID string, policy domain.FrozenPolicy, reviewers []string) (string, error) {
	canonical, err := domain.CanonicalizeID(batchID)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}
	if err := s.validatePolicy(policy, reviewers); err != nil {
		return "", err
	}
	batch, err := s.store.GetBatch(ctx, canonical)
	if err != nil {
		if err == store.ErrNotFound {
			return "", ErrBatchNotFound
		}
		return "", err
	}
	if batch.Status != domain.StatusDraft {
		return "", ErrAlreadyFrozen
	}

	objects, err := s.store.ListObjects(ctx, canonical)
	if err != nil {
		return "", err
	}
	if err := s.validateExpectedRoots(policy, objects); err != nil {
		return "", err
	}
	nodes, err := s.store.ListNodes(ctx, canonical)
	if err != nil {
		return "", err
	}
	if err := s.validateNodeQuorum(policy, nodes); err != nil {
		return "", err
	}
	deps, err := s.store.ListDependencies(ctx, canonical)
	if err != nil {
		return "", err
	}

	digest := policyDigest(policy, reviewers, objects, nodes, deps)
	sortedReviewers := sortedUnique(reviewers)
	if err := s.store.FreezeBatch(ctx, canonical, digest, policy, sortedReviewers, batch.Generation); err != nil {
		if err == store.ErrAlreadyFrozen {
			return "", ErrAlreadyFrozen
		}
		return "", err
	}
	return digest, nil
}

// validatePolicy enforces the frozen-policy invariants.
func (s *Service) validatePolicy(policy domain.FrozenPolicy, reviewers []string) error {
	if err := layout.ValidateChunkSize(policy.ChunkSize); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}
	if policy.HashAlgorithm.DigestSize() == 0 {
		return fmt.Errorf("%w: unsupported hash algorithm %q", ErrInvalidPolicy, policy.HashAlgorithm)
	}
	if policy.ReplicaQuorum < 1 {
		return fmt.Errorf("%w: replica quorum must be positive", ErrInvalidPolicy)
	}
	if policy.CoverageBPS < 0 || policy.CoverageBPS > 10000 {
		return fmt.Errorf("%w: coverage bps out of range [0, 10000]", ErrInvalidPolicy)
	}
	if policy.StableTicks < 0 {
		return fmt.Errorf("%w: stable ticks must be non-negative", ErrInvalidPolicy)
	}
	if len(sortedUnique(reviewers)) < 2 {
		return fmt.Errorf("%w: at least two distinct qualified reviewers are required", ErrInvalidPolicy)
	}
	return nil
}

// validateExpectedRoots checks that each object's declared root digest length
// matches the policy hash algorithm.
func (s *Service) validateExpectedRoots(policy domain.FrozenPolicy, objects []domain.ArchiveObject) error {
	size := policy.HashAlgorithm.DigestSize()
	for _, o := range objects {
		if len(o.ExpectedRoot) != size {
			return fmt.Errorf("%w: object %q expected root length %d != %d", ErrInvalidPolicy, o.ObjectID, len(o.ExpectedRoot), size)
		}
	}
	return nil
}

// validateNodeQuorum checks that at least quorum distinct enabled nodes exist.
func (s *Service) validateNodeQuorum(policy domain.FrozenPolicy, nodes []domain.StorageNode) error {
	enabled := 0
	for _, n := range nodes {
		if n.Enabled {
			enabled++
		}
	}
	if enabled < policy.ReplicaQuorum {
		return fmt.Errorf("%w: enabled nodes %d < replica quorum %d", ErrInvalidPolicy, enabled, policy.ReplicaQuorum)
	}
	return nil
}

// policyDigest computes the immutable policy digest from a canonical binary
// encoding of the policy, reviewers, objects, nodes and dependencies. Identical
// inputs always produce identical digests, satisfying restart invariance.
func policyDigest(policy domain.FrozenPolicy, reviewers []string, objects []domain.ArchiveObject, nodes []domain.StorageNode, deps []domain.ObjectDependency) string {
	h := sha256.New()
	writeU64 := func(v int64) { var b [8]byte; binary.BigEndian.PutUint64(b[:], uint64(v)); h.Write(b[:]) }
	writeBytes := func(b []byte) { writeU64(int64(len(b))); h.Write(b) }
	writeString := func(s string) { writeBytes([]byte(s)) }

	writeU64(policy.ChunkSize)
	writeString(string(policy.HashAlgorithm))
	writeU64(int64(policy.ReplicaQuorum))
	writeU64(int64(policy.CoverageBPS))
	writeU64(policy.StableTicks)
	writeString(policy.Schedule)

	rev := sortedUnique(reviewers)
	writeU64(int64(len(rev)))
	for _, r := range rev {
		writeString(r)
	}

	sortedObjects := append([]domain.ArchiveObject(nil), objects...)
	sort.Slice(sortedObjects, func(i, j int) bool { return sortedObjects[i].CanonicalKey < sortedObjects[j].CanonicalKey })
	writeU64(int64(len(sortedObjects)))
	for _, o := range sortedObjects {
		writeString(o.ObjectID)
		writeU64(o.ExpectedLength)
		writeBytes(o.ExpectedRoot)
	}

	sortedNodes := append([]domain.StorageNode(nil), nodes...)
	sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i].NodeID < sortedNodes[j].NodeID })
	writeU64(int64(len(sortedNodes)))
	for _, n := range sortedNodes {
		writeString(n.NodeID)
		writeString(n.FailureDomain)
		writeU64(boolInt64(n.Enabled))
	}

	sortedDeps := append([]domain.ObjectDependency(nil), deps...)
	sort.Slice(sortedDeps, func(i, j int) bool {
		if sortedDeps[i].FromObject != sortedDeps[j].FromObject {
			return sortedDeps[i].FromObject < sortedDeps[j].FromObject
		}
		return sortedDeps[i].ToObject < sortedDeps[j].ToObject
	})
	writeU64(int64(len(sortedDeps)))
	for _, d := range sortedDeps {
		writeString(d.FromObject)
		writeString(d.ToObject)
		writeString(d.Reason)
	}

	return hex.EncodeToString(h.Sum(nil))
}

func boolInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func sortedUnique(items []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, it := range items {
		trimmed := strings.TrimSpace(it)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}
