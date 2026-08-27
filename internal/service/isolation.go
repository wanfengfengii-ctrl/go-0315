package service

import (
	"sort"

	"archival-replica-integrity-recovery/internal/domain"
)

// buildIsolationClosure computes the deterministic quarantine closure starting
// from the anomalous seed objects. It performs a breadth-first traversal along
// three documented edge types — object dependency, shared content root and
// common failure domain — and returns members sorted by object id with
// duplicates removed.
func buildIsolationClosure(anomalies []string, objects []domain.ArchiveObject, nodes []domain.StorageNode, deps []domain.ObjectDependency, results []verdictResult, evidence []domain.ReplicaEvidence) []domain.IsolationMember {
	depForward := make(map[string][]string)
	depBackward := make(map[string][]string)
	for _, d := range deps {
		depForward[d.FromObject] = append(depForward[d.FromObject], d.ToObject)
		depBackward[d.ToObject] = append(depBackward[d.ToObject], d.FromObject)
	}

	expectedRoot := make(map[string][]byte)
	byRoot := make(map[string][]string)
	for _, o := range objects {
		expectedRoot[o.ObjectID] = o.ExpectedRoot
		byRoot[string(o.ExpectedRoot)] = append(byRoot[string(o.ExpectedRoot)], o.ObjectID)
	}
	for k := range byRoot {
		sort.Strings(byRoot[k])
	}

	nodeDomain := make(map[string]string)
	enabledNodes := make(map[string]bool)
	for _, n := range nodes {
		nodeDomain[n.NodeID] = n.FailureDomain
		enabledNodes[n.NodeID] = n.Enabled
	}
	// An object is considered stored in a failure domain when it has evidence
	// on an enabled node in that domain.
	objectDomains := make(map[string]map[string]bool)
	for _, o := range objects {
		objectDomains[o.ObjectID] = make(map[string]bool)
	}
	for _, e := range evidence {
		if !enabledNodes[e.NodeID] {
			continue
		}
		if objectDomains[e.ObjectID] == nil {
			objectDomains[e.ObjectID] = make(map[string]bool)
		}
		if d, ok := nodeDomain[e.NodeID]; ok {
			objectDomains[e.ObjectID][d] = true
		}
	}

	resultByObject := make(map[string]verdictResult)
	for _, r := range results {
		resultByObject[r.ObjectID] = r
	}

	type discovery struct {
		reason string
		parent string
	}
	discovered := make(map[string]discovery)
	visited := make(map[string]bool)

	queue := append([]string(nil), anomalies...)
	sort.Strings(queue)
	for _, a := range anomalies {
		discovered[a] = discovery{reason: reasonForKind(resultByObject[a].Kind), parent: ""}
	}

	for len(queue) > 0 {
		obj := queue[0]
		queue = queue[1:]
		if visited[obj] {
			continue
		}
		if _, ok := expectedRoot[obj]; !ok {
			continue
		}
		visited[obj] = true

		enqueue := func(other, reason, parent string) {
			if visited[other] {
				return
			}
			if _, ok := discovered[other]; ok {
				return
			}
			if _, ok := expectedRoot[other]; !ok {
				return
			}
			discovered[other] = discovery{reason: reason, parent: parent}
			queue = append(queue, other)
		}

		// Dependency edges (both directions).
		for _, to := range depForward[obj] {
			enqueue(to, "dependency", obj)
		}
		for _, from := range depBackward[obj] {
			enqueue(from, "dependency", obj)
		}

		// Shared content root edges.
		for _, other := range byRoot[string(expectedRoot[obj])] {
			if other != obj {
				enqueue(other, "shared_content", obj)
			}
		}

		// Common failure domain edges.
		res := resultByObject[obj]
		suspectDomains := make(map[string]bool)
		for _, nid := range res.SuspectNodes {
			if d, ok := nodeDomain[nid]; ok {
				suspectDomains[d] = true
			}
		}
		for _, o := range objects {
			if o.ObjectID == obj {
				continue
			}
			for d := range suspectDomains {
				if objectDomains[o.ObjectID][d] {
					enqueue(o.ObjectID, "failure_domain", obj)
					break
				}
			}
		}
	}

	out := make([]domain.IsolationMember, 0, len(discovered))
	for obj, d := range discovered {
		out = append(out, domain.IsolationMember{
			ObjectID:     obj,
			Reason:       d.reason,
			ParentObject: d.parent,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ObjectID < out[j].ObjectID })
	return out
}

func reasonForKind(k domain.VerdictKind) string {
	switch k {
	case domain.VerdictMissing:
		return "missing"
	case domain.VerdictLengthMismatch:
		return "length_mismatch"
	case domain.VerdictChunkCorrupt:
		return "chunk_corrupt"
	case domain.VerdictDigestMismatch:
		return "digest_mismatch"
	case domain.VerdictForked:
		return "forked"
	default:
		return "anomaly"
	}
}
