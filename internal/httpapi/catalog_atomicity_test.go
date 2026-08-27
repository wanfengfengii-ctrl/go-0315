package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
)

func TestModel_CatalogReplacementIsAtomic(t *testing.T) {
	oldRootA := digest32(0x11)
	oldRootB := digest32(0x22)
	oldCatalog := catalogRequest{
		Objects: []catalogObjectDTO{
			{ObjectID: "old-a", ExpectedLength: 8, ExpectedRoot: hexOf(oldRootA)},
			{ObjectID: "old-b", ExpectedLength: 16, ExpectedRoot: hexOf(oldRootB)},
		},
		Dependencies: []catalogDependencyDTO{
			{FromObject: "old-a", ToObject: "old-b", Reason: "original"},
		},
		Nodes: []catalogNodeDTO{
			{NodeID: "old-node-a", FailureDomain: "rack-a", Enabled: true},
			{NodeID: "old-node-b", FailureDomain: "rack-b", Enabled: true},
		},
	}
	wantObjects := []domain.ArchiveObject{
		{ObjectID: "old-a", CanonicalKey: "old-a", ExpectedLength: 8, ExpectedRoot: oldRootA},
		{ObjectID: "old-b", CanonicalKey: "old-b", ExpectedLength: 16, ExpectedRoot: oldRootB},
	}
	wantDependencies := []domain.ObjectDependency{
		{FromObject: "old-a", ToObject: "old-b", Reason: "original"},
	}
	wantNodes := []domain.StorageNode{
		{NodeID: "old-node-a", FailureDomain: "rack-a", Enabled: true},
		{NodeID: "old-node-b", FailureDomain: "rack-b", Enabled: true},
	}

	cases := []struct {
		name        string
		prepare     func(*Server) error
		replacement catalogRequest
		wantStatus  int
	}{
		{
			name: "duplicate dependency edge",
			replacement: catalogRequest{
				Objects: []catalogObjectDTO{
					{ObjectID: "new-a", ExpectedLength: 24, ExpectedRoot: hexOf(digest32(0x33))},
					{ObjectID: "new-b", ExpectedLength: 32, ExpectedRoot: hexOf(digest32(0x44))},
				},
				Dependencies: []catalogDependencyDTO{
					{FromObject: "new-a", ToObject: "new-b", Reason: "first"},
					{FromObject: "new-a", ToObject: "new-b", Reason: "duplicate"},
				},
				Nodes: []catalogNodeDTO{
					{NodeID: "new-node-a", FailureDomain: "rack-c", Enabled: true},
					{NodeID: "new-node-b", FailureDomain: "rack-d", Enabled: true},
				},
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "node persistence error",
			prepare: func(srv *Server) error {
				_, err := srv.svc.Store().DB().Exec(`CREATE TRIGGER reject_catalog_node
					BEFORE INSERT ON nodes WHEN NEW.node_id = 'reject-node'
					BEGIN SELECT RAISE(ABORT, 'rejected test node'); END`)
				return err
			},
			replacement: catalogRequest{
				Objects: []catalogObjectDTO{
					{ObjectID: "new-a", ExpectedLength: 24, ExpectedRoot: hexOf(digest32(0x55))},
					{ObjectID: "new-b", ExpectedLength: 32, ExpectedRoot: hexOf(digest32(0x66))},
				},
				Dependencies: []catalogDependencyDTO{
					{FromObject: "new-b", ToObject: "new-a", Reason: "replacement"},
				},
				Nodes: []catalogNodeDTO{
					{NodeID: "new-node", FailureDomain: "rack-c", Enabled: true},
					{NodeID: "reject-node", FailureDomain: "rack-d", Enabled: true},
				},
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t)
			request := func(method, path string, body any) *httptest.ResponseRecorder {
				t.Helper()
				var payload []byte
				if body != nil {
					var err error
					payload, err = json.Marshal(body)
					if err != nil {
						t.Fatalf("marshal request: %v", err)
					}
				}
				req := httptest.NewRequest(method, path, bytes.NewReader(payload))
				rec := httptest.NewRecorder()
				srv.ServeHTTP(rec, req)
				return rec
			}
			putCatalog := func(batchID string, catalog catalogRequest) {
				t.Helper()
				rec := request(http.MethodPut, "/v1/batches/"+batchID+"/catalog", catalog)
				if rec.Code != http.StatusOK {
					t.Fatalf("initial catalog status = %d, body = %s", rec.Code, rec.Body.String())
				}
			}

			const batchID = "atomic-catalog"
			const controlID = "atomic-control"
			for _, id := range []string{batchID, controlID} {
				rec := request(http.MethodPost, "/v1/batches", createBatchRequest{BatchID: id})
				if rec.Code != http.StatusCreated {
					t.Fatalf("create %s status = %d, body = %s", id, rec.Code, rec.Body.String())
				}
				putCatalog(id, oldCatalog)
			}

			if tc.prepare != nil {
				if err := tc.prepare(srv); err != nil {
					t.Fatalf("prepare failure: %v", err)
				}
			}
			rec := request(http.MethodPut, "/v1/batches/"+batchID+"/catalog", tc.replacement)
			if rec.Code != tc.wantStatus {
				t.Fatalf("failed replacement status = %d, want %d; body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}

			st := srv.svc.Store()
			gotObjects, err := st.ListObjects(context.Background(), batchID)
			if err != nil {
				t.Fatalf("list objects: %v", err)
			}
			gotDependencies, err := st.ListDependencies(context.Background(), batchID)
			if err != nil {
				t.Fatalf("list dependencies: %v", err)
			}
			gotNodes, err := st.ListNodes(context.Background(), batchID)
			if err != nil {
				t.Fatalf("list nodes: %v", err)
			}
			if !reflect.DeepEqual(gotObjects, wantObjects) {
				t.Errorf("objects changed after failed replacement: got %#v, want %#v", gotObjects, wantObjects)
			}
			if !reflect.DeepEqual(gotDependencies, wantDependencies) {
				t.Errorf("dependencies changed after failed replacement: got %#v, want %#v", gotDependencies, wantDependencies)
			}
			if !reflect.DeepEqual(gotNodes, wantNodes) {
				t.Errorf("nodes changed after failed replacement: got %#v, want %#v", gotNodes, wantNodes)
			}

			freeze := freezeRequest{ChunkSize: 8, HashAlgorithm: "sha256", ReplicaQuorum: 2, CoverageBPS: 10000, Schedule: "daily", Reviewers: []string{"alice", "bob"}}
			freezeDigest := func(id string) string {
				t.Helper()
				rec := request(http.MethodPost, fmt.Sprintf("/v1/batches/%s/freeze", id), freeze)
				if rec.Code != http.StatusOK {
					t.Fatalf("freeze %s status = %d, body = %s", id, rec.Code, rec.Body.String())
				}
				var response map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatalf("decode freeze response: %v", err)
				}
				return response["policy_digest"]
			}
			if got, want := freezeDigest(batchID), freezeDigest(controlID); got != want {
				t.Errorf("freeze used mutated catalog: digest = %q, want %q", got, want)
			}
		})
	}
}
