// Package store implements the SQLite persistence and startup-recovery layer.
// It owns every database transaction, unique constraint and version comparison
// so that the domain state machine never mutates durable state outside a
// single atomic transaction. The store also exposes the logical clock used by
// leases, verdicts and retry scheduling, and recovers unfinished work after a
// restart.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"archival-replica-integrity-recovery/internal/domain"

	_ "modernc.org/sqlite"
)

// Sentinel errors returned by the store.
var (
	ErrNotFound            = errors.New("store: record not found")
	ErrAlreadyExists       = errors.New("store: record already exists")
	ErrNotFrozen           = errors.New("store: batch is not frozen")
	ErrAlreadyFrozen       = errors.New("store: batch is already frozen")
	ErrConflict            = errors.New("store: optimistic conflict")
	ErrIdempotencyConflict = errors.New("store: idempotency conflict")
)

// Store is a SQLite-backed persistence handle. It is safe for concurrent use:
// all writes are serialized by the single SQLite writer lock and the logical
// clock increment is performed inside the same transaction that observes it.
type Store struct {
	db   *sql.DB
	mu   sync.Mutex
	path string
}

// Open opens (or creates) the SQLite database at path and applies the schema
// migration. An empty path selects a private in-memory database.
func Open(path string) (*Store, error) {
	dsn := path
	if dsn == "" {
		dsn = "file::memory:?cache=shared"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, path: dsn}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.recover(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the raw handle for tests that need to inspect rows directly.
func (s *Store) DB() *sql.DB { return s.db }

// Path returns the database path used to open the store, for restart tests.
func (s *Store) Path() string { return s.path }

// NextTick atomically advances and returns the database logical clock. It is
// the single source of monotonic ticks for observed events.
func (s *Store) NextTick(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var tick int64
	if err := tx.QueryRowContext(ctx, `UPDATE meta SET value = value + 1 WHERE key = 'tick' RETURNING value`).Scan(&tick); err != nil {
		return 0, fmt.Errorf("store: next tick: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return tick, nil
}

// CurrentTick returns the current logical clock without advancing it.
func (s *Store) CurrentTick(ctx context.Context) (int64, error) {
	var tick int64
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'tick'`).Scan(&tick); err != nil {
		return 0, fmt.Errorf("store: current tick: %w", err)
	}
	return tick, nil
}

// withTx runs fn inside a single committed transaction, rolling back on any
// error. It is the only sanctioned path for mutating domain state.
func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// migrate applies the full schema in dependency order. All statements are
// idempotent so the migration can run on every open.
func (s *Store) migrate() error {
	for _, stmt := range schemaStatements {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("store: migrate: %w", err)
		}
	}
	if _, err := s.db.Exec(`INSERT OR IGNORE INTO meta (key, value) VALUES ('tick', 0)`); err != nil {
		return fmt.Errorf("store: seed tick: %w", err)
	}
	return nil
}

// recover performs the startup recovery pass. See recover logic in
// recover.go for the full rule set.
func (s *Store) recover() error {
	ctx := context.Background()
	tick, err := s.CurrentTick(ctx)
	if err != nil {
		return err
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE expires_tick <= ?`, tick); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE repair_tasks SET state = ?, next_tick = ? WHERE state = ?`, domain.RepairPending, tick, domain.RepairDispatched); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE repair_tasks SET state = ? WHERE state = ? AND next_tick <= ?`, domain.RepairPending, domain.RepairFailed, tick); err != nil {
			return err
		}
		return nil
	})
}
