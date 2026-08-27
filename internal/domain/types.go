// Package domain defines the stable domain types, identifiers and state
// transitions shared across the archival replica integrity and recovery
// service. These types mirror the documented data model and are intentionally
// free of persistence and transport concerns.
package domain

// Status is the lifecycle status of a preservation batch.
type Status string

const (
	StatusDraft    Status = "draft"
	StatusFrozen   Status = "frozen"
	StatusTerminal Status = "terminal"
)

// HashAlgorithm identifies the digest algorithm declared by a frozen policy.
type HashAlgorithm string

const (
	HashSHA256 HashAlgorithm = "sha256"
)

// DigestSize returns the fixed byte length of the digest for the algorithm.
// It returns 0 for unknown algorithms so callers can reject them explicitly.
func (a HashAlgorithm) DigestSize() int {
	switch a {
	case HashSHA256:
		return 32
	default:
		return 0
	}
}

// VerdictKind classifies the single integrity verdict formed for an object.
type VerdictKind string

const (
	VerdictIntact         VerdictKind = "intact"
	VerdictMissing        VerdictKind = "missing"
	VerdictLengthMismatch VerdictKind = "length_mismatch"
	VerdictChunkCorrupt   VerdictKind = "chunk_corrupt"
	VerdictDigestMismatch VerdictKind = "digest_mismatch"
	VerdictForked         VerdictKind = "forked"
)

// ResourceType enumerates the four limited-lifetime leases managed by the
// service.
type ResourceType string

const (
	ResourceScan     ResourceType = "scan"
	ResourceRead     ResourceType = "read"
	ResourceWrite    ResourceType = "write"
	ResourceTerminal ResourceType = "terminal"
)

// RepairState is the lifecycle state of a chunk repair task.
type RepairState string

const (
	RepairPending    RepairState = "pending"
	RepairDispatched RepairState = "dispatched"
	RepairFailed     RepairState = "failed"
	RepairConfirmed  RepairState = "confirmed"
)

// RepairFailureCategory classifies the deterministic retry category recorded
// for a failed external copy attempt.
type RepairFailureCategory string

const (
	FailureTimeout        RepairFailureCategory = "timeout"
	FailureRejected       RepairFailureCategory = "rejected"
	FailureDisconnect     RepairFailureCategory = "disconnect"
	FailureMalformed      RepairFailureCategory = "malformed"
	FailureDigestMismatch RepairFailureCategory = "digest_mismatch"
)

// TerminalKind is the outcome of a terminal arbitration request.
type TerminalKind string

const (
	TerminalRelease    TerminalKind = "release"
	TerminalQuarantine TerminalKind = "quarantine"
	TerminalRetire     TerminalKind = "retire"
)

// FrozenPolicy is the immutable validation policy captured when a batch is
// frozen. Every field becomes read-only after freeze.
type FrozenPolicy struct {
	ChunkSize     int64         `json:"chunk_size"`
	HashAlgorithm HashAlgorithm `json:"hash_algorithm"`
	ReplicaQuorum int           `json:"replica_quorum"`
	CoverageBPS   int           `json:"coverage_bps"`
	StableTicks   int64         `json:"stable_ticks"`
	Schedule      string        `json:"schedule"`
}

// PreservationBatch is the top-level preservation unit.
type PreservationBatch struct {
	BatchID         string `json:"batch_id"`
	Generation      int64  `json:"generation"`
	Status          Status `json:"status"`
	PolicyDigest    string `json:"policy_digest"`
	CurrentEpoch    int64  `json:"current_epoch"`
	TerminalVersion int64  `json:"terminal_version"`
}

// ArchiveObject is a single archived object within a batch.
type ArchiveObject struct {
	ObjectID       string `json:"object_id"`
	CanonicalKey   string `json:"canonical_key"`
	ExpectedLength int64  `json:"expected_length"`
	ExpectedRoot   []byte `json:"expected_root"`
}

// ObjectDependency is a directed dependency edge between two objects.
type ObjectDependency struct {
	FromObject string `json:"from_object"`
	ToObject   string `json:"to_object"`
	Reason     string `json:"reason"`
}

// StorageNode is an independent storage node that holds replicas.
type StorageNode struct {
	NodeID        string `json:"node_id"`
	FailureDomain string `json:"failure_domain"`
	Enabled       bool   `json:"enabled"`
}

// ScanEpoch is a scan window over a batch.
type ScanEpoch struct {
	BatchID    string `json:"batch_id"`
	EpochNo    int64  `json:"epoch_no"`
	OpenedTick int64  `json:"opened_tick"`
	ClosedTick int64  `json:"closed_tick"`
}

// ReplicaEvidence is an append-only chunk observation from a node.
type ReplicaEvidence struct {
	ObjectID     string `json:"object_id"`
	EpochNo      int64  `json:"epoch_no"`
	NodeID       string `json:"node_id"`
	ChunkNo      int64  `json:"chunk_no"`
	Length       int64  `json:"length"`
	Digest       []byte `json:"digest"`
	OperationID  string `json:"operation_id"`
	ObservedTick int64  `json:"observed_tick"`
}

// IntegrityVerdict is the single winning verdict for an object in an epoch.
type IntegrityVerdict struct {
	ObjectID      string      `json:"object_id"`
	EpochNo       int64       `json:"epoch_no"`
	WinningRoot   []byte      `json:"winning_root"`
	VerdictKind   VerdictKind `json:"verdict_kind"`
	ThresholdTick int64       `json:"threshold_tick"`
}

// IsolationMember is one object in a quarantine closure.
type IsolationMember struct {
	Generation   int64  `json:"generation"`
	ObjectID     string `json:"object_id"`
	Reason       string `json:"reason"`
	ParentObject string `json:"parent_object"`
}

// ResourceLease is a limited-lifetime lease over a resource.
type ResourceLease struct {
	ResourceType ResourceType `json:"resource_type"`
	ResourceKey  string       `json:"resource_key"`
	LeaseID      string       `json:"lease_id"`
	Holder       string       `json:"holder"`
	StartTick    int64        `json:"start_tick"`
	ExpiresTick  int64        `json:"expires_tick"`
	Version      int64        `json:"version"`
}

// RepairTask is a single chunk repair instruction.
type RepairTask struct {
	Generation     int64       `json:"generation"`
	ObjectID       string      `json:"object_id"`
	ChunkNo        int64       `json:"chunk_no"`
	SourceNode     string      `json:"source_node"`
	TargetNode     string      `json:"target_node"`
	ExpectedDigest []byte      `json:"expected_digest"`
	State          RepairState `json:"state"`
	AttemptNo      int         `json:"attempt_no"`
}

// PendingAttempt records the next retry tick and failure category for a
// failed external copy attempt.
type PendingAttempt struct {
	NextTick        int64                 `json:"next_tick"`
	FailureCategory RepairFailureCategory `json:"failure_category"`
}

// VerificationSample is a post-repair re-read of a node's root digest.
type VerificationSample struct {
	Generation int64  `json:"generation"`
	ObjectID   string `json:"object_id"`
	NodeID     string `json:"node_id"`
	RootDigest []byte `json:"root_digest"`
	SampleTick int64  `json:"sample_tick"`
}

// ReviewDecision is a qualified reviewer's decision for a generation.
type ReviewDecision struct {
	Generation int64  `json:"generation"`
	Reviewer   string `json:"reviewer"`
	Approved   bool   `json:"approved"`
	Tick       int64  `json:"tick"`
}

// TerminalDecision is the unique terminal outcome for a generation.
type TerminalDecision struct {
	Generation int64        `json:"generation"`
	Kind       TerminalKind `json:"kind"`
	Tick       int64        `json:"tick"`
}

// RecoveryCredential is the unique credential issued when a quarantine is
// released.
type RecoveryCredential struct {
	Generation int64  `json:"generation"`
	Credential []byte `json:"credential"`
	IssuedTick int64  `json:"issued_tick"`
}
