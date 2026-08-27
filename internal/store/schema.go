package store

// schemaStatements is the full SQLite DDL, ordered by dependency. Every
// statement is idempotent (CREATE TABLE IF NOT EXISTS) so migration is safe to
// run on every open. Foreign keys are declared for documentation and enforced
// by the service layer's transaction discipline.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS meta (
		key   TEXT PRIMARY KEY,
		value INTEGER NOT NULL
	)`,

	`CREATE TABLE IF NOT EXISTS batches (
		batch_id               TEXT PRIMARY KEY,
		generation             INTEGER NOT NULL DEFAULT 0,
		status                 TEXT NOT NULL,
		policy_digest          TEXT NOT NULL DEFAULT '',
		current_epoch          INTEGER NOT NULL DEFAULT 0,
		terminal_version       INTEGER NOT NULL DEFAULT 0,
		frozen_chunk_size      INTEGER,
		frozen_hash_algorithm  TEXT,
		frozen_replica_quorum  INTEGER,
		frozen_coverage_bps    INTEGER,
		frozen_stable_ticks    INTEGER,
		frozen_schedule        TEXT,
		frozen_reviewers       TEXT NOT NULL DEFAULT '',
		created_tick           INTEGER NOT NULL DEFAULT 0
	)`,

	`CREATE TABLE IF NOT EXISTS objects (
		batch_id        TEXT NOT NULL,
		object_id       TEXT NOT NULL,
		canonical_key   TEXT NOT NULL,
		expected_length INTEGER NOT NULL,
		expected_root   BLOB,
		PRIMARY KEY (batch_id, object_id)
	)`,

	`CREATE TABLE IF NOT EXISTS dependencies (
		batch_id    TEXT NOT NULL,
		from_object TEXT NOT NULL,
		to_object   TEXT NOT NULL,
		reason      TEXT NOT NULL,
		PRIMARY KEY (batch_id, from_object, to_object)
	)`,

	`CREATE TABLE IF NOT EXISTS nodes (
		batch_id       TEXT NOT NULL,
		node_id        TEXT NOT NULL,
		failure_domain TEXT NOT NULL,
		enabled        INTEGER NOT NULL,
		PRIMARY KEY (batch_id, node_id)
	)`,

	`CREATE TABLE IF NOT EXISTS epochs (
		batch_id    TEXT NOT NULL,
		epoch_no    INTEGER NOT NULL,
		opened_tick INTEGER NOT NULL,
		closed_tick INTEGER,
		PRIMARY KEY (batch_id, epoch_no)
	)`,

	`CREATE TABLE IF NOT EXISTS evidence (
		batch_id      TEXT NOT NULL,
		object_id     TEXT NOT NULL,
		epoch_no      INTEGER NOT NULL,
		node_id       TEXT NOT NULL,
		chunk_no      INTEGER NOT NULL,
		length        INTEGER NOT NULL,
		digest        BLOB NOT NULL,
		operation_id  TEXT NOT NULL,
		observed_tick INTEGER NOT NULL,
		PRIMARY KEY (batch_id, object_id, epoch_no, node_id, chunk_no)
	)`,

	`CREATE TABLE IF NOT EXISTS verdicts (
		batch_id       TEXT NOT NULL,
		object_id      TEXT NOT NULL,
		epoch_no       INTEGER NOT NULL,
		winning_root   BLOB,
		verdict_kind   TEXT NOT NULL,
		threshold_tick INTEGER NOT NULL,
		PRIMARY KEY (batch_id, object_id, epoch_no)
	)`,

	`CREATE TABLE IF NOT EXISTS isolation_members (
		batch_id      TEXT NOT NULL,
		generation    INTEGER NOT NULL,
		object_id     TEXT NOT NULL,
		reason        TEXT NOT NULL,
		parent_object TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (batch_id, generation, object_id)
	)`,

	`CREATE TABLE IF NOT EXISTS generations (
		batch_id          TEXT NOT NULL,
		generation        INTEGER NOT NULL,
		opened_tick       INTEGER NOT NULL,
		stable_since_tick INTEGER NOT NULL,
		PRIMARY KEY (batch_id, generation)
	)`,

	`CREATE TABLE IF NOT EXISTS leases (
		resource_type TEXT NOT NULL,
		resource_key  TEXT NOT NULL,
		lease_id      TEXT NOT NULL,
		holder        TEXT NOT NULL,
		start_tick    INTEGER NOT NULL,
		expires_tick  INTEGER NOT NULL,
		version       INTEGER NOT NULL,
		PRIMARY KEY (resource_type, resource_key)
	)`,

	`CREATE TABLE IF NOT EXISTS repair_tasks (
		id               TEXT PRIMARY KEY,
		batch_id         TEXT NOT NULL,
		generation       INTEGER NOT NULL,
		object_id        TEXT NOT NULL,
		chunk_no         INTEGER NOT NULL,
		source_node      TEXT NOT NULL,
		target_node      TEXT NOT NULL,
		expected_digest  BLOB NOT NULL,
		state            TEXT NOT NULL,
		attempt_no       INTEGER NOT NULL DEFAULT 0,
		next_tick        INTEGER NOT NULL DEFAULT 0,
		failure_category TEXT NOT NULL DEFAULT ''
	)`,

	`CREATE UNIQUE INDEX IF NOT EXISTS repair_unique
		ON repair_tasks (batch_id, generation, object_id, chunk_no, target_node)`,

	`CREATE TABLE IF NOT EXISTS samples (
		batch_id    TEXT NOT NULL,
		generation  INTEGER NOT NULL,
		object_id   TEXT NOT NULL,
		node_id     TEXT NOT NULL,
		root_digest BLOB NOT NULL,
		sample_tick INTEGER NOT NULL,
		PRIMARY KEY (batch_id, generation, object_id, node_id)
	)`,

	`CREATE TABLE IF NOT EXISTS review_decisions (
		batch_id   TEXT NOT NULL,
		generation INTEGER NOT NULL,
		reviewer   TEXT NOT NULL,
		approved   INTEGER NOT NULL,
		tick       INTEGER NOT NULL,
		PRIMARY KEY (batch_id, generation, reviewer)
	)`,

	`CREATE TABLE IF NOT EXISTS terminal_decisions (
		batch_id   TEXT NOT NULL,
		generation INTEGER NOT NULL,
		kind       TEXT NOT NULL,
		tick       INTEGER NOT NULL,
		PRIMARY KEY (batch_id, generation)
	)`,

	`CREATE TABLE IF NOT EXISTS recovery_credentials (
		batch_id    TEXT NOT NULL,
		generation  INTEGER NOT NULL,
		credential  BLOB NOT NULL,
		issued_tick INTEGER NOT NULL,
		PRIMARY KEY (batch_id, generation)
	)`,
}
