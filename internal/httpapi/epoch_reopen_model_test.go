package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/layout"
	"archival-replica-integrity-recovery/internal/service"
	"archival-replica-integrity-recovery/internal/store"
)

func TestModel_EpochReopenConflictIsSideEffectFree(t *testing.T) {
	type epochResponse struct {
		EpochNo int64 `json:"epoch_no"`
	}
	type batchResponse struct {
		CurrentEpoch int64 `json:"current_epoch"`
	}

	digest := bytes.Repeat([]byte{0x11}, 32)
	expectedRoot, err := layout.RootDigest(domain.HashSHA256, 8, []layout.ChunkDigest{{No: 0, Digest: digest}})
	if err != nil {
		t.Fatalf("compute expected root: %v", err)
	}

	do := func(srv *Server, method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec
	}
	openEpoch := func(t *testing.T, srv *Server, want int64) {
		t.Helper()
		rec := do(srv, http.MethodPost, "/v1/batches/batch-1/epochs", "")
		if rec.Code != http.StatusCreated {
			t.Fatalf("open epoch status = %d body=%s", rec.Code, rec.Body.String())
		}
		var got epochResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode open epoch: %v", err)
		}
		if got.EpochNo != want {
			t.Fatalf("opened epoch = %d, want %d", got.EpochNo, want)
		}
	}
	submitEvidence := func(t *testing.T, srv *Server, epoch int64, node, operation string, observedTick int64, wantStatus int) {
		t.Helper()
		body := fmt.Sprintf(`{"object_id":"obj-1","node_id":%q,"chunk_no":0,"length":8,"digest":%q,"operation_id":%q,"observed_tick":%d}`,
			node, hexOf(digest), operation, observedTick)
		rec := do(srv, http.MethodPost, fmt.Sprintf("/v1/batches/batch-1/epochs/%d/evidence", epoch), body)
		if rec.Code != wantStatus {
			t.Fatalf("submit %s at tick %d status = %d, want %d body=%s", node, observedTick, rec.Code, wantStatus, rec.Body.String())
		}
	}
	closeEpoch := func(t *testing.T, srv *Server, epoch int64) {
		t.Helper()
		rec := do(srv, http.MethodPost, fmt.Sprintf("/v1/batches/batch-1/epochs/%d/close", epoch), "")
		if rec.Code != http.StatusOK {
			t.Fatalf("close epoch %d status = %d body=%s", epoch, rec.Code, rec.Body.String())
		}
	}

	cases := []struct {
		name  string
		check func(*testing.T, *Server, *store.Store)
	}{
		{
			name: "active scan rejects repeated opens without durable side effects",
			check: func(t *testing.T, srv *Server, st *store.Store) {
				openEpoch(t, srv, 1)
				for attempt := 1; attempt <= 2; attempt++ {
					rec := do(srv, http.MethodPost, "/v1/batches/batch-1/epochs", "")
					if rec.Code != http.StatusConflict {
						t.Fatalf("conflicting open %d status = %d, want %d body=%s", attempt, rec.Code, http.StatusConflict, rec.Body.String())
					}
					var envelope errorBody
					if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
						t.Fatalf("decode conflicting open %d: %v", attempt, err)
					}
					if envelope.Code != CodeConflict {
						t.Fatalf("conflicting open %d code = %q, want %q", attempt, envelope.Code, CodeConflict)
					}
				}

				rec := do(srv, http.MethodGet, "/v1/batches/batch-1", "")
				var batch batchResponse
				if rec.Code != http.StatusOK {
					t.Fatalf("get batch status = %d body=%s", rec.Code, rec.Body.String())
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &batch); err != nil {
					t.Fatalf("decode batch: %v", err)
				}
				if batch.CurrentEpoch != 1 {
					t.Fatalf("current_epoch after conflicting opens = %d, want 1", batch.CurrentEpoch)
				}
				var count, maxEpoch int64
				if err := st.DB().QueryRow(`SELECT COUNT(*), COALESCE(MAX(epoch_no), 0) FROM epochs WHERE batch_id = ?`, "batch-1").Scan(&count, &maxEpoch); err != nil {
					t.Fatalf("inspect epoch rows: %v", err)
				}
				if count != 1 || maxEpoch != 1 {
					t.Fatalf("epoch rows after conflicting opens = count %d max %d, want count 1 max 1", count, maxEpoch)
				}

				submitEvidence(t, srv, 1, "n1", "epoch-1-n1", 10, http.StatusCreated)
				submitEvidence(t, srv, 1, "n2", "epoch-1-n2", 11, http.StatusCreated)
				closeEpoch(t, srv, 1)
			},
		},
		{
			name: "closing releases the scan lease for the next epoch",
			check: func(t *testing.T, srv *Server, st *store.Store) {
				openEpoch(t, srv, 1)
				submitEvidence(t, srv, 1, "n1", "epoch-1-n1", 10, http.StatusCreated)
				submitEvidence(t, srv, 1, "n2", "epoch-1-n2", 11, http.StatusCreated)
				closeEpoch(t, srv, 1)
				openEpoch(t, srv, 2)

				var current, rows int64
				if err := st.DB().QueryRow(`SELECT current_epoch FROM batches WHERE batch_id = ?`, "batch-1").Scan(&current); err != nil {
					t.Fatalf("read current epoch: %v", err)
				}
				if err := st.DB().QueryRow(`SELECT COUNT(*) FROM epochs WHERE batch_id = ?`, "batch-1").Scan(&rows); err != nil {
					t.Fatalf("count epoch rows: %v", err)
				}
				if current != 2 || rows != 2 {
					t.Fatalf("after reopening: current_epoch=%d epoch_rows=%d, want 2 and 2", current, rows)
				}
			},
		},
		{
			name: "scan lease uses a half-open interval",
			check: func(t *testing.T, srv *Server, st *store.Store) {
				openEpoch(t, srv, 1)
				var expires int64
				if err := st.DB().QueryRow(`SELECT expires_tick FROM leases WHERE resource_type = 'scan' AND resource_key = ?`, "batch-1").Scan(&expires); err != nil {
					t.Fatalf("read scan lease expiry: %v", err)
				}
				submitEvidence(t, srv, 1, "n1", "inside-boundary", expires-1, http.StatusCreated)
				submitEvidence(t, srv, 1, "n2", "at-boundary", expires, http.StatusConflict)
				var rows int64
				if err := st.DB().QueryRow(`SELECT COUNT(*) FROM evidence WHERE batch_id = ? AND epoch_no = 1`, "batch-1").Scan(&rows); err != nil {
					t.Fatalf("count boundary evidence: %v", err)
				}
				if rows != 1 {
					t.Fatalf("evidence rows across expiry boundary = %d, want 1", rows)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "epochs.db"))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			srv := NewServer(service.NewService(st, service.SuccessAdapter{}))

			steps := []struct {
				method string
				path   string
				body   string
				want   int
			}{
				{http.MethodPost, "/v1/batches", `{"batch_id":"batch-1"}`, http.StatusCreated},
				{http.MethodPut, "/v1/batches/batch-1/catalog", fmt.Sprintf(`{"objects":[{"object_id":"obj-1","expected_length":8,"expected_root":%q}],"dependencies":[],"nodes":[{"node_id":"n1","failure_domain":"rack-a","enabled":true},{"node_id":"n2","failure_domain":"rack-b","enabled":true}]}`, hexOf(expectedRoot)), http.StatusOK},
				{http.MethodPost, "/v1/batches/batch-1/freeze", `{"chunk_size":8,"hash_algorithm":"sha256","replica_quorum":2,"coverage_bps":10000,"stable_ticks":0,"schedule":"daily","reviewers":["alice","bob"]}`, http.StatusOK},
			}
			for _, step := range steps {
				rec := do(srv, step.method, step.path, step.body)
				if rec.Code != step.want {
					t.Fatalf("%s %s status = %d, want %d body=%s", step.method, step.path, rec.Code, step.want, rec.Body.String())
				}
			}
			tc.check(t, srv, st)
		})
	}
}
