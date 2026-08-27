package service

import (
	"errors"
	"path/filepath"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/store"
)

func TestModel_RuntimeRepairRetryAtNextTick(t *testing.T) {
	cases := []struct {
		name               string
		failure            domain.RepairFailureCategory
		viaReceipt         bool
		dispatchAtDeadline bool
	}{
		{name: "timeout_via_dispatch", failure: domain.FailureTimeout, dispatchAtDeadline: true},
		{name: "rejected_via_pending", failure: domain.FailureRejected},
		{name: "disconnect_via_pending", failure: domain.FailureDisconnect},
		{name: "malformed_via_pending", failure: domain.FailureMalformed},
		{name: "digest_mismatch_via_pending", failure: domain.FailureDigestMismatch, viaReceipt: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "repair.db")
			st, err := store.Open(path)
			if err != nil {
				t.Fatalf("open store: %v", err)
			}

			firstOutcome := tc.failure
			if tc.viaReceipt {
				firstOutcome = ""
			}
			adapter := NewScriptedAdapter(firstOutcome, "")
			svc := NewService(st, adapter)
			task := store.RepairTaskRecord{
				ID: "repair-" + tc.name, BatchID: "batch-1", Generation: 1,
				ObjectID: "object-1", ChunkNo: 0, SourceNode: "source", TargetNode: "target",
				ExpectedDigest: digest32(0x5a), State: domain.RepairPending,
			}
			if err := st.CreateRepairTasks(contextB(), []store.RepairTaskRecord{task}); err != nil {
				t.Fatalf("create repair: %v", err)
			}

			failed, err := svc.DispatchRepair(contextB(), task.ID)
			if err != nil {
				t.Fatalf("first dispatch: %v", err)
			}
			if tc.viaReceipt {
				failed, err = svc.ReceiptRepair(contextB(), task.ID, digest32(0x6b))
				if err != nil {
					t.Fatalf("mismatching receipt: %v", err)
				}
			}
			if failed.State != domain.RepairFailed || failed.FailureCategory != tc.failure {
				t.Fatalf("failure view = %+v, want failed/%s", failed, tc.failure)
			}
			failureTick, err := st.CurrentTick(contextB())
			if err != nil {
				t.Fatalf("current tick: %v", err)
			}
			wantNextTick := failureTick + 1
			if tc.viaReceipt {
				wantNextTick = failureTick + 2
			}
			if failed.NextTick != wantNextTick {
				t.Fatalf("next_tick = %d, want deterministic %d", failed.NextTick, wantNextTick)
			}

			pending, err := svc.PendingRepairs(contextB())
			if err != nil {
				t.Fatalf("pending before next_tick: %v", err)
			}
			if len(pending) != 0 {
				t.Fatalf("pending before next_tick = %+v, want none", pending)
			}
			callsBeforeEarlyDispatch := adapter.Calls()
			if _, err := svc.DispatchRepair(contextB(), task.ID); !errors.Is(err, ErrQuorumConflict) {
				t.Fatalf("early dispatch error = %v, want ErrQuorumConflict", err)
			}
			if adapter.Calls() != callsBeforeEarlyDispatch {
				t.Fatalf("early dispatch invoked adapter: calls %d -> %d", callsBeforeEarlyDispatch, adapter.Calls())
			}
			stillFailed, err := st.GetRepairTask(contextB(), task.ID)
			if err != nil {
				t.Fatalf("get failed repair: %v", err)
			}
			if stillFailed.State != domain.RepairFailed || stillFailed.NextTick != failed.NextTick || stillFailed.FailureCategory != tc.failure {
				t.Fatalf("failure bookkeeping changed before deadline: %+v", stillFailed)
			}

			tick := failureTick
			for tick < failed.NextTick {
				tick, err = st.NextTick(contextB())
				if err != nil {
					t.Fatalf("advance to next_tick: %v", err)
				}
			}
			if tick != failed.NextTick {
				t.Fatalf("advanced tick = %d, want next_tick %d", tick, failed.NextTick)
			}

			if tc.dispatchAtDeadline {
				if _, err := svc.DispatchRepair(contextB(), task.ID); err != nil {
					t.Fatalf("dispatch at next_tick: %v", err)
				}
			} else {
				pending, err = svc.PendingRepairs(contextB())
				if err != nil {
					t.Fatalf("pending at next_tick: %v", err)
				}
				if len(pending) != 1 || pending[0].ID != task.ID || pending[0].State != domain.RepairPending {
					t.Fatalf("pending at next_tick = %+v, want reactivated %q", pending, task.ID)
				}
				if pending[0].NextTick != failed.NextTick || pending[0].FailureCategory != tc.failure {
					t.Fatalf("reactivation lost failure bookkeeping: %+v", pending[0])
				}
				if _, err := svc.DispatchRepair(contextB(), task.ID); err != nil {
					t.Fatalf("dispatch reactivated repair: %v", err)
				}
			}

			retried, err := st.GetRepairTask(contextB(), task.ID)
			if err != nil {
				t.Fatalf("get retried repair: %v", err)
			}
			if retried.State != domain.RepairDispatched || retried.AttemptNo != 2 {
				t.Fatalf("retried repair = %+v, want dispatched attempt 2", retried)
			}
			if adapter.Calls() != 2 {
				t.Fatalf("adapter calls = %d, want 2", adapter.Calls())
			}
			if _, err := svc.ReceiptRepair(contextB(), task.ID, task.ExpectedDigest); err != nil {
				t.Fatalf("confirm retry: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("close store: %v", err)
			}

			reopened, err := store.Open(path)
			if err != nil {
				t.Fatalf("reopen store: %v", err)
			}
			t.Cleanup(func() { reopened.Close() })
			recoveredSvc := NewService(reopened, adapter)
			confirmed, err := reopened.GetRepairTask(contextB(), task.ID)
			if err != nil {
				t.Fatalf("get confirmed repair after restart: %v", err)
			}
			if confirmed.State != domain.RepairConfirmed {
				t.Fatalf("state after restart = %q, want confirmed", confirmed.State)
			}
			pending, err = recoveredSvc.PendingRepairs(contextB())
			if err != nil {
				t.Fatalf("pending after restart: %v", err)
			}
			if len(pending) != 0 {
				t.Fatalf("confirmed repair became pending after restart: %+v", pending)
			}
			if _, err := recoveredSvc.DispatchRepair(contextB(), task.ID); !errors.Is(err, ErrQuorumConflict) {
				t.Fatalf("confirmed dispatch error = %v, want ErrQuorumConflict", err)
			}
			if adapter.Calls() != 2 {
				t.Fatalf("confirmed repair invoked adapter again: %d calls", adapter.Calls())
			}
		})
	}
}
