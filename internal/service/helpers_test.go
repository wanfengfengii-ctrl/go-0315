package service

import (
	"context"
	"path/filepath"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/layout"
	"archival-replica-integrity-recovery/internal/store"
)

// newTestService opens a file-backed store in a temp dir and returns a service
// wired with the given repair adapter.
func newTestService(t *testing.T, adapter RepairAdapter) (*Service, *store.Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewService(st, adapter), st
}

// digest32 returns a deterministic 32-byte digest filled with seed.
func digest32(seed byte) []byte {
	d := make([]byte, 32)
	for i := range d {
		d[i] = seed
	}
	return d
}

// rootOf computes the root digest of a single-chunk object of the given length.
func rootOf(length int64, chunkDigest []byte) []byte {
	root, err := layout.RootDigest(domain.HashSHA256, length, []layout.ChunkDigest{{No: 0, Digest: chunkDigest}})
	if err != nil {
		panic(err)
	}
	return root
}

// setupFrozenBatch drives a batch through create, catalog and freeze using a
// single object of the given length with chunk digest rootSeed and the given
// nodes. It returns the frozen batch record.
func setupFrozenBatch(t *testing.T, svc *Service, batchID string, objectID string, length int64, rootSeed byte, nodes []domain.StorageNode, policy domain.FrozenPolicy) {
	t.Helper()
	if err := svc.CreateBatch(context.Background(), batchID); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	expectedRoot := rootOf(length, digest32(rootSeed))
	objects := []CatalogObject{{ObjectID: objectID, ExpectedLength: length, ExpectedRoot: expectedRoot}}
	catalogNodes := make([]CatalogNode, len(nodes))
	for i, n := range nodes {
		catalogNodes[i] = CatalogNode{NodeID: n.NodeID, FailureDomain: n.FailureDomain, Enabled: n.Enabled}
	}
	if err := svc.CatalogBatch(context.Background(), batchID, objects, nil, catalogNodes); err != nil {
		t.Fatalf("catalog batch: %v", err)
	}
	if _, err := svc.FreezeBatch(context.Background(), batchID, policy, []string{"alice", "bob"}); err != nil {
		t.Fatalf("freeze batch: %v", err)
	}
}

func defaultPolicy() domain.FrozenPolicy {
	return domain.FrozenPolicy{
		ChunkSize:     8,
		HashAlgorithm: domain.HashSHA256,
		ReplicaQuorum: 2,
		CoverageBPS:   10000,
		StableTicks:   0,
		Schedule:      "daily",
	}
}

func threeNodes() []domain.StorageNode {
	return []domain.StorageNode{
		{NodeID: "n1", FailureDomain: "rack-a", Enabled: true},
		{NodeID: "n2", FailureDomain: "rack-b", Enabled: true},
		{NodeID: "n3", FailureDomain: "rack-c", Enabled: true},
	}
}

// contextB returns a background context for service calls in tests.
func contextB() context.Context { return context.Background() }
