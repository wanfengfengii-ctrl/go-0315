package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"archival-replica-integrity-recovery/internal/service"
	"archival-replica-integrity-recovery/internal/store"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	st, err := store.Open("")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewServer(service.NewService(st, service.SuccessAdapter{}))
}

func TestHealthOK(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status field = %q, want %q", body["status"], "ok")
	}
}

func TestHealthRejectsNonGET(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestCreateBatchRejectsUnknownField(t *testing.T) {
	srv := newTestServer(t)
	body := []byte(`{"batch_id":"b1","bogus":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	var env errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if env.Code != CodeMalformedRequest {
		t.Fatalf("code = %q, want %q", env.Code, CodeMalformedRequest)
	}
}

func TestFullCreateFreezeFlowOverHTTP(t *testing.T) {
	srv := newTestServer(t)

	// Create batch.
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader([]byte(`{"batch_id":"batch-1"}`)))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d", rec.Code)
	}

	// Catalog.
	catalog := `{"objects":[{"object_id":"obj-1","expected_length":8,"expected_root":"` + hexOf(digest32(0x11)) + `"}],"dependencies":[],"nodes":[{"node_id":"n1","failure_domain":"rack-a","enabled":true},{"node_id":"n2","failure_domain":"rack-b","enabled":true}]}`
	req = httptest.NewRequest(http.MethodPut, "/v1/batches/batch-1/catalog", bytes.NewReader([]byte(catalog)))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("catalog status = %d body=%s", rec.Code, rec.Body.String())
	}

	// Freeze.
	freeze := `{"chunk_size":8,"hash_algorithm":"sha256","replica_quorum":2,"coverage_bps":10000,"stable_ticks":0,"schedule":"daily","reviewers":["alice","bob"]}`
	req = httptest.NewRequest(http.MethodPost, "/v1/batches/batch-1/freeze", bytes.NewReader([]byte(freeze)))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("freeze status = %d body=%s", rec.Code, rec.Body.String())
	}

	// GET reconstructs the frozen state.
	req = httptest.NewRequest(http.MethodGet, "/v1/batches/batch-1", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d", rec.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if got["status"] != "frozen" {
		t.Fatalf("status = %v, want frozen", got["status"])
	}
}

func digest32(seed byte) []byte {
	d := make([]byte, 32)
	for i := range d {
		d[i] = seed
	}
	return d
}

func hexOf(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = digits[c>>4]
		out[i*2+1] = digits[c&0xf]
	}
	return string(out)
}
