package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/service"
)

// handleBatches serves POST /v1/batches (create) and GET /v1/batches (list).
func (s *Server) handleBatches(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req createBatchRequest
		if err := decodeBody(w, r, &req); err != nil {
			writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
			return
		}
		if req.BatchID == "" {
			writeError(w, http.StatusBadRequest, CodeMalformedRequest, "batch_id is required", nil)
			return
		}
		if err := s.svc.CreateBatch(r.Context(), req.BatchID); err != nil {
			s.writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"batch_id": req.BatchID})
	case http.MethodGet:
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "list batches is not supported; use GET /v1/batches/{id}", nil)
	default:
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "method not allowed", nil)
	}
}

// handleBatchSubresource routes every /v1/batches/{id}/... path.
func (s *Server) handleBatchSubresource(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/batches/")
	segs := splitPath(rest)
	if len(segs) == 0 || segs[0] == "" {
		writeError(w, http.StatusNotFound, CodeNotFound, "batch id required", nil)
		return
	}
	batchID := segs[0]

	if len(segs) == 1 {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "method not allowed", nil)
			return
		}
		s.handleGetBatch(w, r, batchID)
		return
	}

	switch segs[1] {
	case "catalog":
		s.handleCatalog(w, r, batchID)
	case "freeze":
		s.handleFreeze(w, r, batchID)
	case "epochs":
		s.handleEpochs(w, r, batchID, segs[2:])
	case "generations":
		s.handleGenerations(w, r, batchID, segs[2:])
	case "terminal":
		s.handleTerminal(w, r, batchID)
	default:
		writeError(w, http.StatusNotFound, CodeNotFound, "unknown subresource", nil)
	}
}

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request, batchID string) {
	if r.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "catalog requires PUT", nil)
		return
	}
	var req catalogRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	objects := make([]service.CatalogObject, 0, len(req.Objects))
	for _, o := range req.Objects {
		root, err := decodeHex(o.ExpectedRoot)
		if err != nil {
			writeError(w, http.StatusBadRequest, CodeMalformedRequest, "expected_root must be hex", []string{o.ObjectID})
			return
		}
		objects = append(objects, service.CatalogObject{ObjectID: o.ObjectID, ExpectedLength: o.ExpectedLength, ExpectedRoot: root})
	}
	deps := make([]service.CatalogDependency, 0, len(req.Dependencies))
	for _, d := range req.Dependencies {
		deps = append(deps, service.CatalogDependency{FromObject: d.FromObject, ToObject: d.ToObject, Reason: d.Reason})
	}
	nodes := make([]service.CatalogNode, 0, len(req.Nodes))
	for _, n := range req.Nodes {
		nodes = append(nodes, service.CatalogNode{NodeID: n.NodeID, FailureDomain: n.FailureDomain, Enabled: n.Enabled})
	}
	if err := s.svc.CatalogBatch(r.Context(), batchID, objects, deps, nodes); err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batch_id": batchID, "objects": len(objects), "nodes": len(nodes)})
}

func (s *Server) handleFreeze(w http.ResponseWriter, r *http.Request, batchID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "freeze requires POST", nil)
		return
	}
	var req freezeRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	policy := domain.FrozenPolicy{
		ChunkSize:     req.ChunkSize,
		HashAlgorithm: domain.HashAlgorithm(req.HashAlgorithm),
		ReplicaQuorum: req.ReplicaQuorum,
		CoverageBPS:   req.CoverageBPS,
		StableTicks:   req.StableTicks,
		Schedule:      req.Schedule,
	}
	digest, err := s.svc.FreezeBatch(r.Context(), batchID, policy, req.Reviewers)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"batch_id": batchID, "policy_digest": digest})
}

func (s *Server) handleEpochs(w http.ResponseWriter, r *http.Request, batchID string, segs []string) {
	if len(segs) == 0 {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "epochs require POST", nil)
			return
		}
		epoch, err := s.svc.OpenEpoch(r.Context(), batchID)
		if err != nil {
			s.writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]int64{"epoch_no": epoch})
		return
	}
	epochNo, err := strconv.ParseInt(segs[0], 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "invalid epoch number", nil)
		return
	}
	if len(segs) < 2 {
		writeError(w, http.StatusNotFound, CodeNotFound, "epoch subresource required", nil)
		return
	}
	switch segs[1] {
	case "evidence":
		s.handleEvidence(w, r, batchID, epochNo)
	case "close":
		s.handleCloseEpoch(w, r, batchID, epochNo)
	default:
		writeError(w, http.StatusNotFound, CodeNotFound, "unknown epoch subresource", nil)
	}
}

func (s *Server) handleEvidence(w http.ResponseWriter, r *http.Request, batchID string, epochNo int64) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "evidence requires POST", nil)
		return
	}
	var req evidenceRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	digest, err := decodeHex(req.Digest)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "digest must be hex", nil)
		return
	}
	opID := req.OperationID
	if opID == "" {
		opID = r.Header.Get("Idempotency-Key")
	}
	in := service.EvidenceInput{
		ObjectID:     req.ObjectID,
		NodeID:       req.NodeID,
		ChunkNo:      req.ChunkNo,
		Length:       req.Length,
		Digest:       digest,
		OperationID:  opID,
		ObservedTick: req.ObservedTick,
	}
	if err := s.svc.SubmitEvidence(r.Context(), batchID, epochNo, in); err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"batch_id": batchID, "epoch_no": epochNo, "object_id": req.ObjectID, "node_id": req.NodeID})
}

func (s *Server) handleCloseEpoch(w http.ResponseWriter, r *http.Request, batchID string, epochNo int64) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "close requires POST", nil)
		return
	}
	result, err := s.svc.CloseEpoch(r.Context(), batchID, epochNo)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"batch_id":   result.BatchID,
		"epoch_no":   result.EpochNo,
		"generation": result.Generation,
		"anomalies":  result.Anomalies,
		"verdicts":   result.Verdicts,
	})
}

func (s *Server) handleGenerations(w http.ResponseWriter, r *http.Request, batchID string, segs []string) {
	if len(segs) < 2 {
		writeError(w, http.StatusNotFound, CodeNotFound, "generation subresource required", nil)
		return
	}
	generation, err := strconv.ParseInt(segs[0], 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "invalid generation number", nil)
		return
	}
	switch segs[1] {
	case "repairs":
		s.handlePlanRepairs(w, r, batchID, generation)
	case "samples":
		s.handleSample(w, r, batchID, generation)
	case "reviews":
		s.handleReview(w, r, batchID, generation)
	default:
		writeError(w, http.StatusNotFound, CodeNotFound, "unknown generation subresource", nil)
	}
}

func (s *Server) handlePlanRepairs(w http.ResponseWriter, r *http.Request, batchID string, generation int64) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "repairs require POST", nil)
		return
	}
	views, err := s.svc.PlanRepairs(r.Context(), batchID, generation)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"batch_id": batchID, "generation": generation, "repairs": views})
}

func (s *Server) handleSample(w http.ResponseWriter, r *http.Request, batchID string, generation int64) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "samples require POST", nil)
		return
	}
	var req sampleRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	root, err := decodeHex(req.RootDigest)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "root_digest must be hex", nil)
		return
	}
	if err := s.svc.SubmitSample(r.Context(), batchID, generation, req.ObjectID, req.NodeID, root); err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"batch_id": batchID, "generation": generation, "object_id": req.ObjectID, "node_id": req.NodeID})
}

func (s *Server) handleReview(w http.ResponseWriter, r *http.Request, batchID string, generation int64) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "reviews require POST", nil)
		return
	}
	var req reviewRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	if err := s.svc.SubmitReview(r.Context(), batchID, generation, req.Reviewer, req.Approved); err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"batch_id": batchID, "generation": generation, "reviewer": req.Reviewer, "approved": req.Approved})
}

func (s *Server) handleTerminal(w http.ResponseWriter, r *http.Request, batchID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "terminal requires POST", nil)
		return
	}
	var req terminalRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	outcome, err := s.svc.Terminal(r.Context(), batchID, req.Generation, service.TerminalDecisionKind(req.Kind))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	resp := map[string]any{
		"batch_id":   batchID,
		"generation": outcome.Generation,
		"kind":       outcome.Kind,
		"tick":       outcome.Tick,
	}
	if outcome.Credential != "" {
		resp["credential"] = outcome.Credential
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGetBatch(w http.ResponseWriter, r *http.Request, batchID string) {
	batch, err := s.svc.Store().GetBatch(r.Context(), batchID)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	objects, _ := s.svc.Store().ListObjects(r.Context(), batchID)
	nodes, _ := s.svc.Store().ListNodes(r.Context(), batchID)
	writeJSON(w, http.StatusOK, map[string]any{
		"batch_id":         batch.BatchID,
		"generation":       batch.Generation,
		"status":           batch.Status,
		"policy_digest":    batch.PolicyDigest,
		"current_epoch":    batch.CurrentEpoch,
		"terminal_version": batch.TerminalVersion,
		"frozen_policy":    batch.FrozenPolicy,
		"frozen_reviewers": batch.FrozenReviewers,
		"objects":          objects,
		"nodes":            nodes,
	})
}

// handleLeases serves POST /v1/leases/acquire.
func (s *Server) handleLeases(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/leases/acquire" {
		s.handleLeaseAcquire(w, r)
		return
	}
	writeError(w, http.StatusNotFound, CodeNotFound, "unknown lease endpoint", nil)
}

// handleLeaseSubresource serves renew/release and the acquire path.
func (s *Server) handleLeaseSubresource(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/leases/")
	segs := splitPath(rest)
	if len(segs) == 0 || segs[0] == "" {
		writeError(w, http.StatusNotFound, CodeNotFound, "lease resource required", nil)
		return
	}
	if segs[0] == "acquire" {
		s.handleLeaseAcquire(w, r)
		return
	}
	leaseID := segs[0]
	if len(segs) < 2 {
		writeError(w, http.StatusNotFound, CodeNotFound, "lease action required", nil)
		return
	}
	switch segs[1] {
	case "renew":
		s.handleLeaseRenew(w, r, leaseID)
	case "release":
		s.handleLeaseRelease(w, r, leaseID)
	default:
		writeError(w, http.StatusNotFound, CodeNotFound, "unknown lease action", nil)
	}
}

func (s *Server) handleLeaseAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "acquire requires POST", nil)
		return
	}
	var req leaseAcquireRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	lease, err := s.svc.AcquireLease(r.Context(), domain.ResourceType(req.ResourceType), req.ResourceKey, req.Holder, req.StartTick, req.ExpiresTick)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, lease)
}

func (s *Server) handleLeaseRenew(w http.ResponseWriter, r *http.Request, leaseID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "renew requires POST", nil)
		return
	}
	var req leaseRenewRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	version, err := s.svc.RenewLease(r.Context(), leaseID, req.Holder, req.LogicalTick, req.ExpiresTick)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease_id": leaseID, "version": version})
}

func (s *Server) handleLeaseRelease(w http.ResponseWriter, r *http.Request, leaseID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "release requires POST", nil)
		return
	}
	var req leaseReleaseRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	if err := s.svc.ReleaseLease(r.Context(), leaseID, req.Holder); err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"lease_id": leaseID, "status": "released"})
}

func (s *Server) handleRepairSubresource(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/repairs/")
	segs := splitPath(rest)
	if len(segs) < 2 {
		writeError(w, http.StatusNotFound, CodeNotFound, "repair action required", nil)
		return
	}
	repairID := segs[0]
	switch segs[1] {
	case "dispatch":
		s.handleRepairDispatch(w, r, repairID)
	case "receipt":
		s.handleRepairReceipt(w, r, repairID)
	default:
		writeError(w, http.StatusNotFound, CodeNotFound, "unknown repair action", nil)
	}
}

func (s *Server) handleRepairDispatch(w http.ResponseWriter, r *http.Request, repairID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "dispatch requires POST", nil)
		return
	}
	view, err := s.svc.DispatchRepair(r.Context(), repairID)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) handleRepairReceipt(w http.ResponseWriter, r *http.Request, repairID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "receipt requires POST", nil)
		return
	}
	var req receiptRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "malformed request body", []string{err.Error()})
		return
	}
	digest, err := decodeHex(req.ObservedDigest)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeMalformedRequest, "observed_digest must be hex", nil)
		return
	}
	view, err := s.svc.ReceiptRepair(r.Context(), repairID, digest)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// writeServiceError maps a service error to the stable error envelope.
func (s *Server) writeServiceError(w http.ResponseWriter, err error) {
	status, code, msg := mapError(err)
	writeError(w, status, code, msg, nil)
}

func splitPath(p string) []string {
	raw := strings.Split(strings.Trim(p, "/"), "/")
	var out []string
	for _, seg := range raw {
		if seg != "" {
			out = append(out, seg)
		}
	}
	return out
}
