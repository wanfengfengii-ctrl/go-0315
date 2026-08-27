package store

import (
	"context"
	"database/sql"
	"errors"

	"archival-replica-integrity-recovery/internal/domain"
)

// PutIsolationMembers replaces the isolation closure for a generation.
func (s *Store) PutIsolationMembers(ctx context.Context, batchID string, generation int64, members []domain.IsolationMember) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM isolation_members WHERE batch_id = ? AND generation = ?`, batchID, generation); err != nil {
			return err
		}
		for _, m := range members {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO isolation_members (batch_id, generation, object_id, reason, parent_object) VALUES (?, ?, ?, ?, ?)`,
				batchID, generation, m.ObjectID, m.Reason, m.ParentObject); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListIsolationMembers returns the isolation closure ordered by object id.
func (s *Store) ListIsolationMembers(ctx context.Context, batchID string, generation int64) ([]domain.IsolationMember, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT generation, object_id, reason, parent_object FROM isolation_members WHERE batch_id = ? AND generation = ? ORDER BY object_id`,
		batchID, generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.IsolationMember
	for rows.Next() {
		var m domain.IsolationMember
		if err := rows.Scan(&m.Generation, &m.ObjectID, &m.Reason, &m.ParentObject); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// PutSample records a post-repair verification sample (idempotent upsert).
func (s *Store) PutSample(ctx context.Context, batchID string, sample domain.VerificationSample) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO samples (batch_id, generation, object_id, node_id, root_digest, sample_tick)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT (batch_id, generation, object_id, node_id) DO UPDATE SET root_digest = excluded.root_digest, sample_tick = excluded.sample_tick`,
			batchID, sample.Generation, sample.ObjectID, sample.NodeID, sample.RootDigest, sample.SampleTick); err != nil {
			return err
		}
		return nil
	})
}

// ListSamples returns samples for a generation ordered by (object, node).
func (s *Store) ListSamples(ctx context.Context, batchID string, generation int64) ([]domain.VerificationSample, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT generation, object_id, node_id, root_digest, sample_tick FROM samples WHERE batch_id = ? AND generation = ? ORDER BY object_id, node_id`,
		batchID, generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.VerificationSample
	for rows.Next() {
		var v domain.VerificationSample
		if err := rows.Scan(&v.Generation, &v.ObjectID, &v.NodeID, &v.RootDigest, &v.SampleTick); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// PutReview records a reviewer decision (idempotent upsert).
func (s *Store) PutReview(ctx context.Context, batchID string, r domain.ReviewDecision) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO review_decisions (batch_id, generation, reviewer, approved, tick) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT (batch_id, generation, reviewer) DO UPDATE SET approved = excluded.approved, tick = excluded.tick`,
			batchID, r.Generation, r.Reviewer, boolToInt(r.Approved), r.Tick); err != nil {
			return err
		}
		return nil
	})
}

// ListReviews returns review decisions for a generation ordered by reviewer.
func (s *Store) ListReviews(ctx context.Context, batchID string, generation int64) ([]domain.ReviewDecision, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT generation, reviewer, approved, tick FROM review_decisions WHERE batch_id = ? AND generation = ? ORDER BY reviewer`,
		batchID, generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ReviewDecision
	for rows.Next() {
		var r domain.ReviewDecision
		var approved int
		if err := rows.Scan(&r.Generation, &r.Reviewer, &approved, &r.Tick); err != nil {
			return nil, err
		}
		r.Approved = approved == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

// PutTerminal records the unique terminal decision. It fails with ErrConflict
// when a decision already exists, enforcing the single-terminal race.
func (s *Store) PutTerminal(ctx context.Context, batchID string, d domain.TerminalDecision) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`INSERT INTO terminal_decisions (batch_id, generation, kind, tick) VALUES (?, ?, ?, ?)`,
			batchID, d.Generation, d.Kind, d.Tick)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrConflict
			}
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return nil
	})
}

// GetTerminal returns the terminal decision for a generation, if any.
func (s *Store) GetTerminal(ctx context.Context, batchID string, generation int64) (*domain.TerminalDecision, error) {
	var d domain.TerminalDecision
	err := s.db.QueryRowContext(ctx,
		`SELECT generation, kind, tick FROM terminal_decisions WHERE batch_id = ? AND generation = ?`, batchID, generation).
		Scan(&d.Generation, &d.Kind, &d.Tick)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// PutCredential stores the unique recovery credential for a generation.
func (s *Store) PutCredential(ctx context.Context, batchID string, c domain.RecoveryCredential) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO recovery_credentials (batch_id, generation, credential, issued_tick) VALUES (?, ?, ?, ?)`,
			batchID, c.Generation, c.Credential, c.IssuedTick); err != nil {
			if isUniqueViolation(err) {
				return ErrConflict
			}
			return err
		}
		return nil
	})
}

// GetCredential returns the recovery credential for a generation, if any.
func (s *Store) GetCredential(ctx context.Context, batchID string, generation int64) (*domain.RecoveryCredential, error) {
	var c domain.RecoveryCredential
	err := s.db.QueryRowContext(ctx,
		`SELECT generation, credential, issued_tick FROM recovery_credentials WHERE batch_id = ? AND generation = ?`, batchID, generation).
		Scan(&c.Generation, &c.Credential, &c.IssuedTick)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// FinalizeRelease atomically records the release terminal decision, the unique
// recovery credential and the batch terminal status in a single transaction.
// This enforces the documented failure boundary for terminal issuance: a
// release either completes with its credential or leaves no partial terminal
// state, so a transient credential-store failure cannot strand the generation
// in an unreleasable terminal-conflict state on retry. It fails with
// ErrConflict when a terminal decision already exists, preserving the
// single-terminal arbitration race.
func (s *Store) FinalizeRelease(ctx context.Context, batchID string, decision domain.TerminalDecision, credential domain.RecoveryCredential) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`INSERT INTO terminal_decisions (batch_id, generation, kind, tick) VALUES (?, ?, ?, ?)`,
			batchID, decision.Generation, decision.Kind, decision.Tick)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrConflict
			}
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO recovery_credentials (batch_id, generation, credential, issued_tick) VALUES (?, ?, ?, ?)`,
			batchID, credential.Generation, credential.Credential, credential.IssuedTick); err != nil {
			if isUniqueViolation(err) {
				return ErrConflict
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE batches SET status = ? WHERE batch_id = ?`, domain.StatusTerminal, batchID); err != nil {
			return err
		}
		return nil
	})
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// GenerationRecord captures the open and stable-since ticks for a generation.
type GenerationRecord struct {
	Generation      int64
	OpenedTick      int64
	StableSinceTick int64
}

// CreateGeneration records a new isolation generation.
func (s *Store) CreateGeneration(ctx context.Context, batchID string, generation, tick int64) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO generations (batch_id, generation, opened_tick, stable_since_tick) VALUES (?, ?, ?, ?)`,
			batchID, generation, tick, tick)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrAlreadyExists
			}
			return err
		}
		return nil
	})
}

// ResetStableSince moves the generation's stable-window start to tick, used
// when a divergent sample or new fork invalidates prior stability.
func (s *Store) ResetStableSince(ctx context.Context, batchID string, generation, tick int64) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE generations SET stable_since_tick = ? WHERE batch_id = ? AND generation = ?`, tick, batchID, generation)
		return err
	})
}

// GetGeneration loads a generation record.
func (s *Store) GetGeneration(ctx context.Context, batchID string, generation int64) (*GenerationRecord, error) {
	var g GenerationRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT generation, opened_tick, stable_since_tick FROM generations WHERE batch_id = ? AND generation = ?`,
		batchID, generation).Scan(&g.Generation, &g.OpenedTick, &g.StableSinceTick)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &g, nil
}
