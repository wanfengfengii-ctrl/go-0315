package service

import (
	"errors"
	"testing"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/store"
)

func TestModel_TerminalReleaseAtomicRetry(t *testing.T) {
	cases := []struct {
		name        string
		failTrigger string
	}{
		{
			name: "credential write failure leaves release retryable",
			failTrigger: `CREATE TRIGGER fail_recovery_credential
				BEFORE INSERT ON recovery_credentials
				BEGIN
					SELECT RAISE(FAIL, 'injected credential write failure');
				END`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, st := newTestService(t, SuccessAdapter{})
			batchID, generation := setupMissingGeneration(t, svc)
			completeRecovery(t, svc, batchID, generation)

			if _, err := st.DB().Exec(tc.failTrigger); err != nil {
				t.Fatalf("install credential failure: %v", err)
			}
			if _, err := svc.Terminal(contextB(), batchID, generation, TerminalRelease); err == nil {
				t.Fatal("release with failing credential write succeeded")
			}

			if _, err := st.GetTerminal(contextB(), batchID, generation); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("terminal after failed release err = %v, want store.ErrNotFound", err)
			}
			if _, err := st.GetCredential(contextB(), batchID, generation); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("credential after failed release err = %v, want store.ErrNotFound", err)
			}
			batch, err := st.GetBatch(contextB(), batchID)
			if err != nil {
				t.Fatalf("get batch after failed release: %v", err)
			}
			if batch.Status != domain.StatusFrozen {
				t.Fatalf("batch status after failed release = %q, want %q", batch.Status, domain.StatusFrozen)
			}

			if _, err := st.DB().Exec(`DROP TRIGGER fail_recovery_credential`); err != nil {
				t.Fatalf("remove credential failure: %v", err)
			}
			outcome, err := svc.Terminal(contextB(), batchID, generation, TerminalRelease)
			if err != nil {
				t.Fatalf("retry release: %v", err)
			}
			if outcome.Credential == "" {
				t.Fatal("retry release returned an empty credential")
			}

			var terminalCount, credentialCount int
			if err := st.DB().QueryRow(`SELECT COUNT(*) FROM terminal_decisions WHERE batch_id = ? AND generation = ?`, batchID, generation).Scan(&terminalCount); err != nil {
				t.Fatalf("count terminal decisions: %v", err)
			}
			if err := st.DB().QueryRow(`SELECT COUNT(*) FROM recovery_credentials WHERE batch_id = ? AND generation = ?`, batchID, generation).Scan(&credentialCount); err != nil {
				t.Fatalf("count recovery credentials: %v", err)
			}
			if terminalCount != 1 || credentialCount != 1 {
				t.Fatalf("successful retry stored terminals=%d credentials=%d, want 1 and 1", terminalCount, credentialCount)
			}
			batch, err = st.GetBatch(contextB(), batchID)
			if err != nil {
				t.Fatalf("get batch after retry: %v", err)
			}
			if batch.Status != domain.StatusTerminal {
				t.Fatalf("batch status after retry = %q, want %q", batch.Status, domain.StatusTerminal)
			}

			competitors := []struct {
				name string
				kind TerminalDecisionKind
			}{
				{name: "quarantine", kind: TerminalQuarantine},
				{name: "retire", kind: TerminalRetire},
			}
			for _, competitor := range competitors {
				t.Run(competitor.name, func(t *testing.T) {
					if _, err := svc.Terminal(contextB(), batchID, generation, competitor.kind); !errors.Is(err, ErrTerminalConflict) {
						t.Fatalf("competing %s err = %v, want ErrTerminalConflict", competitor.kind, err)
					}
				})
			}
		})
	}
}
