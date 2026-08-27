package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/service"
	"archival-replica-integrity-recovery/internal/store"
)

func TestModel_RepairDispatchHonorsRequestCancellation(t *testing.T) {
	tests := []struct {
		name          string
		cancelRequest bool
		wantStatus    int
		wantState     domain.RepairState
		wantAttempts  int
		wantCopies    int
	}{
		{
			name:          "cancelled before dispatch has no side effects",
			cancelRequest: true,
			wantStatus:    499,
			wantState:     domain.RepairPending,
			wantAttempts:  0,
			wantCopies:    0,
		},
		{
			name:         "live request dispatches normally",
			wantStatus:   http.StatusOK,
			wantState:    domain.RepairDispatched,
			wantAttempts: 1,
			wantCopies:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, err := store.Open("")
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })

			const repairID = "repair-context-boundary"
			task := store.RepairTaskRecord{
				ID:             repairID,
				BatchID:        "batch-context-boundary",
				Generation:     1,
				ObjectID:       "object-1",
				ChunkNo:        3,
				SourceNode:     "source-node",
				TargetNode:     "target-node",
				ExpectedDigest: []byte("expected-digest"),
				State:          domain.RepairPending,
			}
			if err := st.CreateRepairTasks(context.Background(), []store.RepairTaskRecord{task}); err != nil {
				t.Fatalf("create repair task: %v", err)
			}

			adapter := service.NewScriptedAdapter("")
			srv := NewServer(service.NewService(st, adapter))
			reqCtx, cancel := context.WithCancel(context.Background())
			if tt.cancelRequest {
				cancel()
			} else {
				defer cancel()
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/repairs/"+repairID+"/dispatch", nil).WithContext(reqCtx)
			rec := httptest.NewRecorder()

			srv.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			got, err := st.GetRepairTask(context.Background(), repairID)
			if err != nil {
				t.Fatalf("get repair task: %v", err)
			}
			if got.State != tt.wantState {
				t.Errorf("repair state = %q, want %q", got.State, tt.wantState)
			}
			if got.AttemptNo != tt.wantAttempts {
				t.Errorf("attempt count = %d, want %d", got.AttemptNo, tt.wantAttempts)
			}
			if calls := adapter.Calls(); calls != tt.wantCopies {
				t.Errorf("adapter calls = %d, want %d", calls, tt.wantCopies)
			}
		})
	}
}
