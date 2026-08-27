package store

import (
	"context"
	"database/sql"
	"errors"

	"archival-replica-integrity-recovery/internal/domain"
)

// BatchRecord is the full persisted preservation batch including its frozen
// policy fields. Frozen policy columns are NULL while the batch is a draft.
type BatchRecord struct {
	BatchID         string
	Generation      int64
	Status          domain.Status
	PolicyDigest    string
	CurrentEpoch    int64
	TerminalVersion int64
	FrozenPolicy    *domain.FrozenPolicy
	FrozenReviewers []string
	CreatedTick     int64
}

// CreateBatch inserts a new draft batch. It fails if the batch already exists.
func (s *Store) CreateBatch(ctx context.Context, batchID string, tick int64) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`INSERT INTO batches (batch_id, generation, status, created_tick) VALUES (?, 0, ?, ?)`,
			batchID, domain.StatusDraft, tick)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrAlreadyExists
			}
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrAlreadyExists
		}
		return nil
	})
}

// GetBatch loads a batch row. It returns ErrNotFound when the batch is absent.
func (s *Store) GetBatch(ctx context.Context, batchID string) (*BatchRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT batch_id, generation, status, policy_digest, current_epoch, terminal_version,
		       frozen_chunk_size, frozen_hash_algorithm, frozen_replica_quorum,
		       frozen_coverage_bps, frozen_stable_ticks, frozen_schedule, frozen_reviewers, created_tick
		FROM batches WHERE batch_id = ?`, batchID)

	var b BatchRecord
	var chunkSize sql.NullInt64
	var algo, schedule, reviewers sql.NullString
	var quorum, coverageBPS, stableTicks sql.NullInt64
	if err := row.Scan(&b.BatchID, &b.Generation, &b.Status, &b.PolicyDigest,
		&b.CurrentEpoch, &b.TerminalVersion, &chunkSize, &algo, &quorum,
		&coverageBPS, &stableTicks, &schedule, &reviewers, &b.CreatedTick); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if chunkSize.Valid {
		b.FrozenPolicy = &domain.FrozenPolicy{
			ChunkSize:     chunkSize.Int64,
			HashAlgorithm: domain.HashAlgorithm(algo.String),
			ReplicaQuorum: int(quorum.Int64),
			CoverageBPS:   int(coverageBPS.Int64),
			StableTicks:   stableTicks.Int64,
			Schedule:      schedule.String,
		}
	}
	b.FrozenReviewers = splitReviewers(reviewers.String)
	return &b, nil
}

// FreezeBatch atomically marks a draft batch frozen and stores the immutable
// policy digest and reviewer list. It fails with ErrAlreadyFrozen if the batch
// is not a draft.
func (s *Store) FreezeBatch(ctx context.Context, batchID, policyDigest string, policy domain.FrozenPolicy, reviewers []string, generation int64) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE batches SET status = ?, policy_digest = ?, generation = ?,
				frozen_chunk_size = ?, frozen_hash_algorithm = ?, frozen_replica_quorum = ?,
				frozen_coverage_bps = ?, frozen_stable_ticks = ?, frozen_schedule = ?,
				frozen_reviewers = ?
			WHERE batch_id = ? AND status = ?`,
			domain.StatusFrozen, policyDigest, generation,
			policy.ChunkSize, policy.HashAlgorithm, policy.ReplicaQuorum,
			policy.CoverageBPS, policy.StableTicks, policy.Schedule,
			joinReviewers(reviewers), batchID, domain.StatusDraft)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrAlreadyFrozen
		}
		return nil
	})
}

// AdvanceEpoch conditionally increments the batch current_epoch and returns
// the new value. It only advances when current_epoch still equals expectPrev,
// so the open path can prove it won the epoch it leased rather than silently
// racing a concurrent writer; the scan lease already serializes opens, this
// guard makes the invariant independent of that timing. Returns ErrConflict
// when the current_epoch no longer matches expectPrev.
func (s *Store) AdvanceEpoch(ctx context.Context, batchID string, expectPrev int64) (int64, error) {
	var epoch int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx,
			`UPDATE batches SET current_epoch = current_epoch + 1 WHERE batch_id = ? AND current_epoch = ? RETURNING current_epoch`,
			batchID, expectPrev).Scan(&epoch); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrConflict
			}
			return err
		}
		return nil
	})
	return epoch, err
}

// BumpTerminalVersion increments the batch terminal_version, used for the
// single-terminal arbitration race.
func (s *Store) BumpTerminalVersion(ctx context.Context, batchID string) (int64, error) {
	var version int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx,
			`UPDATE batches SET terminal_version = terminal_version + 1 WHERE batch_id = ? RETURNING terminal_version`, batchID).Scan(&version)
	})
	return version, err
}

// MarkTerminal marks a batch terminal.
func (s *Store) MarkTerminal(ctx context.Context, batchID string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE batches SET status = ? WHERE batch_id = ?`, domain.StatusTerminal, batchID)
		return err
	})
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return containsAny(msg, "UNIQUE constraint failed", "constraint failed", "PRIMARY KEY")
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}

// joinReviewers encodes a reviewer list as a NUL-delimited string.
func joinReviewers(rs []string) string {
	out := ""
	for i, r := range rs {
		if i > 0 {
			out += "\x00"
		}
		out += r
	}
	return out
}

// splitReviewers decodes a NUL-delimited reviewer string.
func splitReviewers(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

// SetGeneration advances the batch generation to the given value. It is used
// when a close produces anomalies and a new isolation generation is opened.
func (s *Store) SetGeneration(ctx context.Context, batchID string, generation int64) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE batches SET generation = ? WHERE batch_id = ? AND generation < ?`, generation, batchID, generation)
		return err
	})
}
