package httpapi_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/httpapi"
	"archival-replica-integrity-recovery/internal/service"
	"archival-replica-integrity-recovery/internal/store"
)

func TestModel_CatalogReplacementIsAtomicAcrossFreeze(t *testing.T) {
	type catalogSpec struct {
		Objects      []service.CatalogObject
		Dependencies []service.CatalogDependency
		Nodes        []service.CatalogNode
	}
	type batchView struct {
		Status          domain.Status          `json:"status"`
		PolicyDigest    string                 `json:"policy_digest"`
		FrozenPolicy    *domain.FrozenPolicy   `json:"frozen_policy"`
		FrozenReviewers []string               `json:"frozen_reviewers"`
		Objects         []domain.ArchiveObject `json:"objects"`
		Nodes           []domain.StorageNode   `json:"nodes"`
	}

	root := func(seed byte) []byte {
		return bytes.Repeat([]byte{seed}, 32)
	}
	initial := catalogSpec{
		Objects: []service.CatalogObject{
			{ObjectID: "archive-a", ExpectedLength: 8, ExpectedRoot: root(0xaa)},
			{ObjectID: "archive-b", ExpectedLength: 16, ExpectedRoot: root(0xbb)},
		},
		Dependencies: []service.CatalogDependency{
			{FromObject: "archive-a", ToObject: "archive-b", Reason: "initial-order"},
		},
		Nodes: []service.CatalogNode{
			{NodeID: "node-a", FailureDomain: "rack-a", Enabled: true},
			{NodeID: "node-b", FailureDomain: "rack-b", Enabled: true},
		},
	}
	replacement := catalogSpec{
		Objects: []service.CatalogObject{
			{ObjectID: "archive-c", ExpectedLength: 24, ExpectedRoot: root(0xcc)},
			{ObjectID: "archive-d", ExpectedLength: 32, ExpectedRoot: root(0xdd)},
		},
		Dependencies: []service.CatalogDependency{
			{FromObject: "archive-d", ToObject: "archive-c", Reason: "replacement-order"},
		},
		Nodes: []service.CatalogNode{
			{NodeID: "node-c", FailureDomain: "rack-c", Enabled: true},
			{NodeID: "node-d", FailureDomain: "rack-d", Enabled: false},
		},
	}

	cases := []struct {
		name           string
		freeze         bool
		wantPutStatus  int
		wantCatalog    catalogSpec
		wantBatchState domain.Status
	}{
		{
			name:           "draft replaces objects dependencies and nodes together",
			wantPutStatus:  http.StatusOK,
			wantCatalog:    replacement,
			wantBatchState: domain.StatusDraft,
		},
		{
			name:           "frozen conflict preserves the complete catalog and summary",
			freeze:         true,
			wantPutStatus:  http.StatusConflict,
			wantCatalog:    initial,
			wantBatchState: domain.StatusFrozen,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open("")
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			handler := httpapi.NewServer(service.NewService(st, service.SuccessAdapter{}))

			doJSON := func(method, path string, body any) *httptest.ResponseRecorder {
				t.Helper()
				var encoded []byte
				if body != nil {
					encoded, err = json.Marshal(body)
					if err != nil {
						t.Fatalf("marshal %s %s: %v", method, path, err)
					}
				}
				req := httptest.NewRequest(method, path, bytes.NewReader(encoded))
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				return rec
			}
			putCatalog := func(spec catalogSpec) *httptest.ResponseRecorder {
				objects := make([]map[string]any, len(spec.Objects))
				for i, object := range spec.Objects {
					objects[i] = map[string]any{
						"object_id": object.ObjectID, "expected_length": object.ExpectedLength,
						"expected_root": hex.EncodeToString(object.ExpectedRoot),
					}
				}
				dependencies := make([]map[string]any, len(spec.Dependencies))
				for i, dependency := range spec.Dependencies {
					dependencies[i] = map[string]any{
						"from_object": dependency.FromObject, "to_object": dependency.ToObject,
						"reason": dependency.Reason,
					}
				}
				nodes := make([]map[string]any, len(spec.Nodes))
				for i, node := range spec.Nodes {
					nodes[i] = map[string]any{
						"node_id": node.NodeID, "failure_domain": node.FailureDomain, "enabled": node.Enabled,
					}
				}
				return doJSON(http.MethodPut, "/v1/batches/catalog-atomic/catalog", map[string]any{
					"objects": objects, "dependencies": dependencies, "nodes": nodes,
				})
			}
			getBatch := func() batchView {
				t.Helper()
				rec := doJSON(http.MethodGet, "/v1/batches/catalog-atomic", nil)
				if rec.Code != http.StatusOK {
					t.Fatalf("GET batch status = %d, body = %s", rec.Code, rec.Body.String())
				}
				var view batchView
				if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
					t.Fatalf("decode GET batch: %v", err)
				}
				return view
			}

			if rec := doJSON(http.MethodPost, "/v1/batches", map[string]string{"batch_id": "catalog-atomic"}); rec.Code != http.StatusCreated {
				t.Fatalf("create status = %d, body = %s", rec.Code, rec.Body.String())
			}
			if rec := putCatalog(initial); rec.Code != http.StatusOK {
				t.Fatalf("initial catalog status = %d, body = %s", rec.Code, rec.Body.String())
			}

			var frozenView batchView
			if tc.freeze {
				freeze := map[string]any{
					"chunk_size": 8, "hash_algorithm": "sha256", "replica_quorum": 2,
					"coverage_bps": 10000, "stable_ticks": 3, "schedule": "daily",
					"reviewers": []string{"alice", "bob"},
				}
				if rec := doJSON(http.MethodPost, "/v1/batches/catalog-atomic/freeze", freeze); rec.Code != http.StatusOK {
					t.Fatalf("freeze status = %d, body = %s", rec.Code, rec.Body.String())
				}
				frozenView = getBatch()
				if frozenView.PolicyDigest == "" || frozenView.FrozenPolicy == nil {
					t.Fatalf("GET omitted frozen summary: %+v", frozenView)
				}
			}

			rec := putCatalog(replacement)
			if rec.Code != tc.wantPutStatus {
				t.Fatalf("replacement status = %d, want %d, body = %s", rec.Code, tc.wantPutStatus, rec.Body.String())
			}
			if tc.freeze {
				var failure struct {
					Code httpapi.ErrorCode `json:"code"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &failure); err != nil || failure.Code != httpapi.CodeConflict {
					t.Fatalf("conflict response = %s, decode err = %v", rec.Body.String(), err)
				}
			}

			objects, err := st.ListObjects(context.Background(), "catalog-atomic")
			if err != nil {
				t.Fatalf("list objects: %v", err)
			}
			dependencies, err := st.ListDependencies(context.Background(), "catalog-atomic")
			if err != nil {
				t.Fatalf("list dependencies: %v", err)
			}
			nodes, err := st.ListNodes(context.Background(), "catalog-atomic")
			if err != nil {
				t.Fatalf("list nodes: %v", err)
			}

			wantObjects := make([]domain.ArchiveObject, len(tc.wantCatalog.Objects))
			for i, object := range tc.wantCatalog.Objects {
				wantObjects[i] = domain.ArchiveObject{
					ObjectID: object.ObjectID, CanonicalKey: object.ObjectID,
					ExpectedLength: object.ExpectedLength, ExpectedRoot: object.ExpectedRoot,
				}
			}
			wantDependencies := make([]domain.ObjectDependency, len(tc.wantCatalog.Dependencies))
			for i, dependency := range tc.wantCatalog.Dependencies {
				wantDependencies[i] = domain.ObjectDependency(dependency)
			}
			wantNodes := make([]domain.StorageNode, len(tc.wantCatalog.Nodes))
			for i, node := range tc.wantCatalog.Nodes {
				wantNodes[i] = domain.StorageNode(node)
			}
			if !reflect.DeepEqual(objects, wantObjects) {
				t.Errorf("objects after replacement = %#v, want %#v", objects, wantObjects)
			}
			if !reflect.DeepEqual(dependencies, wantDependencies) {
				t.Errorf("dependencies after replacement = %#v, want %#v", dependencies, wantDependencies)
			}
			if !reflect.DeepEqual(nodes, wantNodes) {
				t.Errorf("nodes after replacement = %#v, want %#v", nodes, wantNodes)
			}

			afterView := getBatch()
			if afterView.Status != tc.wantBatchState {
				t.Errorf("GET status = %q, want %q", afterView.Status, tc.wantBatchState)
			}
			if !reflect.DeepEqual(afterView.Objects, wantObjects) || !reflect.DeepEqual(afterView.Nodes, wantNodes) {
				t.Errorf("GET did not reconstruct expected catalog: %+v", afterView)
			}
			if tc.freeze && !reflect.DeepEqual(afterView, frozenView) {
				t.Errorf("failed frozen update changed GET state\nbefore: %+v\nafter:  %+v", frozenView, afterView)
			}
		})
	}
}
