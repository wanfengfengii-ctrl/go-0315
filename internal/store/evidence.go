package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"

	"archival-replica-integrity-recovery/internal/domain"
)

// OpenEpoch inserts a new scan epoch and returns its number.
func (s *Store) OpenEpoch(ctx context.Context, batchID string, epochNo, tick int64) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO epochs (batch_id, epoch_no, opened_tick) VALUES (?, ?, ?)`,
			batchID, epochNo, tick); err != nil {
			if isUniqueViolation(err) {
				return ErrAlreadyExists
			}
			return err
		}
		return nil
	})
}

// CloseEpoch records the close tick for an open epoch.
func (s *Store) CloseEpoch(ctx context.Context, batchID string, epochNo, tick int64) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE epochs SET closed_tick = ? WHERE batch_id = ? AND epoch_no = ? AND closed_tick IS NULL`,
		tick, batchID, epochNo)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrConflict
	}
	return nil
}

// EpochClosed reports whether the scan epoch has already been closed. It lets
// the service short-circuit a retried close before any isolation generation is
// advanced, so a duplicate close never opens a new generation. A missing epoch
// row is surfaced as ErrNotFound so the caller can treat it as a conflict.
func (s *Store) EpochClosed(ctx context.Context, batchID string, epochNo int64) (bool, error) {
	var closedTick sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT closed_tick FROM epochs WHERE batch_id = ? AND epoch_no = ?`, batchID, epochNo).Scan(&closedTick)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	return closedTick.Valid, nil
}

// AppendEvidence inserts a single normalized evidence row. The unique key is
// (batch, object, epoch, node, chunk). It returns ErrIdempotencyConflict when
// the same operation id re-arrives with different normalized content.
func (s *Store) AppendEvidence(ctx context.Context, batchID string, e domain.ReplicaEvidence) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		var existingOp sql.NullString
		var existingDigest []byte
		var existingLen int64
		err := tx.QueryRowContext(ctx,
			`SELECT operation_id, digest, length FROM evidence WHERE batch_id = ? AND object_id = ? AND epoch_no = ? AND node_id = ? AND chunk_no = ?`,
			batchID, e.ObjectID, e.EpochNo, e.NodeID, e.ChunkNo).Scan(&existingOp, &existingDigest, &existingLen)
		switch {
		case err == nil:
			if existingOp.String == e.OperationID && bytes.Equal(existingDigest, e.Digest) && existingLen == e.Length {
				// Idempotent replay: identical operation and normalized content.
				return nil
			}
			return ErrIdempotencyConflict
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO evidence (batch_id, object_id, epoch_no, node_id, chunk_no, length, digest, operation_id, observed_tick)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			batchID, e.ObjectID, e.EpochNo, e.NodeID, e.ChunkNo, e.Length, e.Digest, e.OperationID, e.ObservedTick)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrIdempotencyConflict
			}
			return err
		}
		return nil
	})
}

// ListEvidence returns all evidence for an epoch ordered deterministically.
func (s *Store) ListEvidence(ctx context.Context, batchID string, epochNo int64) ([]domain.ReplicaEvidence, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT object_id, epoch_no, node_id, chunk_no, length, digest, operation_id, observed_tick
		 FROM evidence WHERE batch_id = ? AND epoch_no = ? ORDER BY object_id, node_id, chunk_no`, batchID, epochNo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ReplicaEvidence
	for rows.Next() {
		var e domain.ReplicaEvidence
		if err := rows.Scan(&e.ObjectID, &e.EpochNo, &e.NodeID, &e.ChunkNo, &e.Length, &e.Digest, &e.OperationID, &e.ObservedTick); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// PutVerdicts replaces all verdicts for an epoch in one transaction.
func (s *Store) PutVerdicts(ctx context.Context, batchID string, epochNo int64, verdicts []domain.IntegrityVerdict) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM verdicts WHERE batch_id = ? AND epoch_no = ?`, batchID, epochNo); err != nil {
			return err
		}
		for _, v := range verdicts {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO verdicts (batch_id, object_id, epoch_no, winning_root, verdict_kind, threshold_tick) VALUES (?, ?, ?, ?, ?, ?)`,
				batchID, v.ObjectID, epochNo, v.WinningRoot, v.VerdictKind, v.ThresholdTick); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListVerdicts returns all verdicts for an epoch ordered by object id.
func (s *Store) ListVerdicts(ctx context.Context, batchID string, epochNo int64) ([]domain.IntegrityVerdict, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT object_id, epoch_no, winning_root, verdict_kind, threshold_tick
		 FROM verdicts WHERE batch_id = ? AND epoch_no = ? ORDER BY object_id`, batchID, epochNo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.IntegrityVerdict
	for rows.Next() {
		var v domain.IntegrityVerdict
		if err := rows.Scan(&v.ObjectID, &v.EpochNo, &v.WinningRoot, &v.VerdictKind, &v.ThresholdTick); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
