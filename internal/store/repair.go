package store

import (
	"context"
	"database/sql"
	"errors"

	"archival-replica-integrity-recovery/internal/domain"
)

// RepairTaskRecord is the persisted repair task with its retry bookkeeping.
type RepairTaskRecord struct {
	ID              string
	BatchID         string
	Generation      int64
	ObjectID        string
	ChunkNo         int64
	SourceNode      string
	TargetNode      string
	ExpectedDigest  []byte
	State           domain.RepairState
	AttemptNo       int
	NextTick        int64
	FailureCategory domain.RepairFailureCategory
}

// CreateRepairTasks inserts a batch of repair tasks. Existing tasks with the
// same natural key (batch, generation, object, chunk, target) are left alone
// so restart never duplicates an already-planned repair.
func (s *Store) CreateRepairTasks(ctx context.Context, tasks []RepairTaskRecord) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for _, t := range tasks {
			if _, err := tx.ExecContext(ctx,
				`INSERT OR IGNORE INTO repair_tasks
				 (id, batch_id, generation, object_id, chunk_no, source_node, target_node, expected_digest, state, attempt_no, next_tick, failure_category)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				t.ID, t.BatchID, t.Generation, t.ObjectID, t.ChunkNo, t.SourceNode, t.TargetNode,
				t.ExpectedDigest, t.State, t.AttemptNo, t.NextTick, t.FailureCategory); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetRepairTask loads a single repair task by id.
func (s *Store) GetRepairTask(ctx context.Context, id string) (*RepairTaskRecord, error) {
	var t RepairTaskRecord
	var failure string
	err := s.db.QueryRowContext(ctx,
		`SELECT id, batch_id, generation, object_id, chunk_no, source_node, target_node, expected_digest, state, attempt_no, next_tick, failure_category
		 FROM repair_tasks WHERE id = ?`, id).
		Scan(&t.ID, &t.BatchID, &t.Generation, &t.ObjectID, &t.ChunkNo, &t.SourceNode, &t.TargetNode,
			&t.ExpectedDigest, &t.State, &t.AttemptNo, &t.NextTick, &failure)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.FailureCategory = domain.RepairFailureCategory(failure)
	return &t, nil
}

// ListPendingRepairs returns repair tasks eligible for dispatch at or before
// tick, ordered deterministically by (batch, generation, object, chunk, target).
func (s *Store) ListPendingRepairs(ctx context.Context, tick int64) ([]RepairTaskRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, batch_id, generation, object_id, chunk_no, source_node, target_node, expected_digest, state, attempt_no, next_tick, failure_category
		 FROM repair_tasks WHERE state = ? AND next_tick <= ? ORDER BY batch_id, generation, object_id, chunk_no, target_node`,
		domain.RepairPending, tick)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepairRows(rows)
}

// ReactivateFailedRepairs transitions repair tasks whose deterministic backoff
// has elapsed from failed back to pending at the current tick. It mirrors the
// startup recovery rule (see Store.recover) so that a running service retries
// due tasks once the logical clock passes their next_tick, instead of waiting
// for a restart. The transition is idempotent: a task already pending is left
// untouched, and only its state changes (next_tick and failure_category are
// preserved for inspection until the next attempt overwrites them).
func (s *Store) ReactivateFailedRepairs(ctx context.Context, tick int64) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE repair_tasks SET state = ? WHERE state = ? AND next_tick <= ?`,
			domain.RepairPending, domain.RepairFailed, tick)
		return err
	})
}

// ListGenerationRepairs returns all repair tasks for a generation.
func (s *Store) ListGenerationRepairs(ctx context.Context, batchID string, generation int64) ([]RepairTaskRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, batch_id, generation, object_id, chunk_no, source_node, target_node, expected_digest, state, attempt_no, next_tick, failure_category
		 FROM repair_tasks WHERE batch_id = ? AND generation = ? ORDER BY object_id, chunk_no, target_node`, batchID, generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepairRows(rows)
}

func scanRepairRows(rows *sql.Rows) ([]RepairTaskRecord, error) {
	var out []RepairTaskRecord
	for rows.Next() {
		var t RepairTaskRecord
		var failure string
		if err := rows.Scan(&t.ID, &t.BatchID, &t.Generation, &t.ObjectID, &t.ChunkNo, &t.SourceNode, &t.TargetNode,
			&t.ExpectedDigest, &t.State, &t.AttemptNo, &t.NextTick, &failure); err != nil {
			return nil, err
		}
		t.FailureCategory = domain.RepairFailureCategory(failure)
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkDispatched transitions a pending task to dispatched and increments its
// attempt counter. It returns ErrConflict when the task is not pending.
func (s *Store) MarkDispatched(ctx context.Context, id string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repair_tasks SET state = ?, attempt_no = attempt_no + 1 WHERE id = ? AND state = ?`,
			domain.RepairDispatched, id, domain.RepairPending)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return nil
	})
}

// RecordRepairFailure records a failed external copy attempt with a
// deterministic retry tick and failure category.
func (s *Store) RecordRepairFailure(ctx context.Context, id string, nextTick int64, category domain.RepairFailureCategory) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repair_tasks SET state = ?, next_tick = ?, failure_category = ? WHERE id = ? AND state = ?`,
			domain.RepairFailed, nextTick, category, id, domain.RepairDispatched)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return nil
	})
}

// ConfirmRepair marks a dispatched task confirmed after its receipt digest was
// verified. It is idempotent for an already-confirmed task.
func (s *Store) ConfirmRepair(ctx context.Context, id string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repair_tasks SET state = ?, next_tick = 0, failure_category = '' WHERE id = ? AND state = ?`,
			domain.RepairConfirmed, id, domain.RepairDispatched)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 1 {
			return nil
		}
		// Already confirmed is an idempotent success.
		var state domain.RepairState
		if err := tx.QueryRowContext(ctx, `SELECT state FROM repair_tasks WHERE id = ?`, id).Scan(&state); err != nil {
			return err
		}
		if state == domain.RepairConfirmed {
			return nil
		}
		return ErrConflict
	})
}
