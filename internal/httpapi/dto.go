package httpapi

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// maxBodyBytes bounds request bodies so malformed or oversized payloads are
// rejected before any transaction begins.
const maxBodyBytes = 1 << 20 // 1 MiB

// createBatchRequest is the body for POST /v1/batches.
type createBatchRequest struct {
	BatchID string `json:"batch_id"`
}

// catalogObjectDTO declares one archived object in the catalogue.
type catalogObjectDTO struct {
	ObjectID       string `json:"object_id"`
	ExpectedLength int64  `json:"expected_length"`
	ExpectedRoot   string `json:"expected_root"`
}

// catalogDependencyDTO declares one dependency edge.
type catalogDependencyDTO struct {
	FromObject string `json:"from_object"`
	ToObject   string `json:"to_object"`
	Reason     string `json:"reason"`
}

// catalogNodeDTO declares one storage node.
type catalogNodeDTO struct {
	NodeID        string `json:"node_id"`
	FailureDomain string `json:"failure_domain"`
	Enabled       bool   `json:"enabled"`
}

// catalogRequest is the body for PUT /v1/batches/{id}/catalog.
type catalogRequest struct {
	Objects      []catalogObjectDTO     `json:"objects"`
	Dependencies []catalogDependencyDTO `json:"dependencies"`
	Nodes        []catalogNodeDTO       `json:"nodes"`
}

// freezeRequest is the body for POST /v1/batches/{id}/freeze.
type freezeRequest struct {
	ChunkSize     int64    `json:"chunk_size"`
	HashAlgorithm string   `json:"hash_algorithm"`
	ReplicaQuorum int      `json:"replica_quorum"`
	CoverageBPS   int      `json:"coverage_bps"`
	StableTicks   int64    `json:"stable_ticks"`
	Schedule      string   `json:"schedule"`
	Reviewers     []string `json:"reviewers"`
}

// evidenceRequest is the body for POST .../evidence.
type evidenceRequest struct {
	ObjectID     string `json:"object_id"`
	NodeID       string `json:"node_id"`
	ChunkNo      int64  `json:"chunk_no"`
	Length       int64  `json:"length"`
	Digest       string `json:"digest"`
	OperationID  string `json:"operation_id"`
	ObservedTick int64  `json:"observed_tick"`
}

// leaseAcquireRequest is the body for POST /v1/leases/acquire.
type leaseAcquireRequest struct {
	ResourceType string `json:"resource_type"`
	ResourceKey  string `json:"resource_key"`
	Holder       string `json:"holder"`
	StartTick    int64  `json:"start_tick"`
	ExpiresTick  int64  `json:"expires_tick"`
}

// leaseRenewRequest is the body for POST /v1/leases/{id}/renew.
type leaseRenewRequest struct {
	Holder      string `json:"holder"`
	LogicalTick int64  `json:"logical_tick"`
	ExpiresTick int64  `json:"expires_tick"`
}

// leaseReleaseRequest is the body for POST /v1/leases/{id}/release.
type leaseReleaseRequest struct {
	Holder      string `json:"holder"`
	LogicalTick int64  `json:"logical_tick"`
}

// sampleRequest is the body for POST .../samples.
type sampleRequest struct {
	ObjectID   string `json:"object_id"`
	NodeID     string `json:"node_id"`
	RootDigest string `json:"root_digest"`
}

// reviewRequest is the body for POST .../reviews.
type reviewRequest struct {
	Reviewer string `json:"reviewer"`
	Approved bool   `json:"approved"`
}

// terminalRequest is the body for POST .../terminal.
type terminalRequest struct {
	Generation int64  `json:"generation"`
	Kind       string `json:"kind"`
}

// receiptRequest is the body for POST /v1/repairs/{id}/receipt.
type receiptRequest struct {
	ObservedDigest string `json:"observed_digest"`
}

// decodeBody strictly decodes a JSON request body, rejecting unknown fields,
// malformed JSON and oversized payloads before any domain transition.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := ensureEOF(dec); err != nil {
		return err
	}
	return nil
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func decodeHex(s string) ([]byte, error) {
	return hex.DecodeString(s)
}
