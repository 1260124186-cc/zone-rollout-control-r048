package workflowprobe

import (
	"net/http"
	"reflect"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runProbeIngestion() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	node, err := enrollNode(harness, "resolver-probe-01", "central")
	if err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "probe.example", 2049, 1, 1, 50)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "probe-lease-0001", "central", 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]
	first, err := submitProbe(harness, leaseID, "probe-request-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if !first.Success || first.ObservedSerial != revision.Serial {
		return ensure(false, "probe values were not preserved")
	}
	if err := checkFirstSummary(first, plan, revision, leaseID, group.ID, node.ID); err != nil {
		return err
	}

	// Same-key replay must return the original probe together with the
	// first captured summary, not a recomputed one.
	replay, err := submitProbe(harness, leaseID, "probe-request-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if replay.ID != first.ID {
		return ensure(false, "probe replay returned a different item")
	}
	if err := checkSameContext(first.Context, replay.Context, "replay"); err != nil {
		return err
	}

	// The stable read entry re-reads the same bound context by probe id.
	reread, err := getProbeContext(harness, first.ID)
	if err != nil {
		return err
	}
	if reread.ID != first.ID {
		return ensure(false, "context read returned a different probe")
	}
	if err := checkSameContext(first.Context, reread.Context, "context read"); err != nil {
		return err
	}

	// Same key with a different payload is still a conflict and must not
	// produce a new context.
	if _, err = submitProbe(harness, leaseID, "probe-request-0001", true, revision.Serial, 25); err != nil {
		if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
			return err
		}
	} else {
		return ensure(false, "same key with different payload was accepted")
	}
	afterConflict, err := getProbeContext(harness, first.ID)
	if err != nil {
		return err
	}
	if err := checkSameContext(first.Context, afterConflict.Context, "post-conflict read"); err != nil {
		return err
	}

	if err := harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/release", nil, nil); err != nil {
		return expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState))
	}

	// Later state changes (plan completion bumps the plan version; a fresh
	// lease moves the node back to leased) must not alter the stored first
	// summary.
	var completed domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version,
	}, &completed); err != nil {
		return err
	}
	if completed.Version == plan.Version {
		return ensure(false, "plan completion did not advance the plan version")
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/leases", domain.LeaseInput{
		RequestKey:   "probe-lease-0002",
		Region:       "central",
		LeaseSeconds: 600,
		MaxNodes:     1,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	// Reopen a fresh plan so the node can be leased again, which changes
	// the node's live state after the summary was captured.
	secondPlan, _, err := createActivePlan(harness, "probe2.example", 2050, 1, 1, 50)
	if err != nil {
		return err
	}
	if _, err := allocate(harness, secondPlan.ID, "probe-lease-0003", "central", 1); err != nil {
		return err
	}

	afterChange, err := getProbeContext(harness, first.ID)
	if err != nil {
		return err
	}
	if err := checkSameContext(first.Context, afterChange.Context, "post-change read"); err != nil {
		return err
	}

	// A full process restart reloads the snapshot: the first summary still
	// resolves by probe id and stays byte-identical.
	if err := harness.restart(); err != nil {
		return err
	}
	afterRestart, err := getProbeContext(harness, first.ID)
	if err != nil {
		return err
	}
	if err := checkSameContext(first.Context, afterRestart.Context, "post-restart read"); err != nil {
		return err
	}
	if afterRestart.Context.Plan.Version != plan.Version {
		return ensure(false, "restarted context lost the captured plan version")
	}

	// A failed submission creates no context: releasing a claimed lease
	// before the probe is stored rejects the submission, and an unknown
	// probe id never resolves through the stable read entry.
	if _, err := enrollNode(harness, "resolver-probe-02", "central"); err != nil {
		return err
	}
	failedPlan, _, err := createActivePlan(harness, "probe3.example", 2051, 1, 1, 50)
	if err != nil {
		return err
	}
	failedGroup, err := allocate(harness, failedPlan.ID, "probe-lease-0004", "central", 1)
	if err != nil {
		return err
	}
	failedLeaseID := failedGroup.LeaseIDs[0]
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+failedLeaseID+"/release", nil, &released); err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease was not released")
	}
	if _, err := submitProbe(harness, failedLeaseID, "probe-request-0002", true, 2051, 5); err == nil {
		return ensure(false, "probe was accepted after lease release")
	} else if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}
	if err := harness.call(http.MethodGet, "/v1/probes/prb_missing/context", nil, nil); err != nil {
		if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
			return err
		}
	} else {
		return ensure(false, "context of a never-submitted probe was returned")
	}
	return nil
}

// checkFirstSummary validates the context captured on the first successful
// submission against the point-in-time aggregates expected at that instant.
func checkFirstSummary(
	receipt domain.ProbeReceipt,
	plan domain.RolloutPlan,
	revision domain.ZoneRevision,
	leaseID string,
	allocationID string,
	nodeID string,
) error {
	if receipt.Context.ProbeID != receipt.ID {
		return ensure(false, "context is not bound to the submitted probe id")
	}
	if receipt.Context.Plan.ID != plan.ID {
		return ensure(false, "context plan id does not match")
	}
	if receipt.Context.Plan.Version != plan.Version {
		return ensure(false, "context plan version does not match the submission instant")
	}
	if receipt.Context.Plan.State != domain.PlanActive {
		return ensure(false, "context plan state is not active")
	}
	if receipt.Context.Plan.LatestRevisionID != revision.ID ||
		receipt.Context.Plan.LatestSerial != revision.Serial {
		return ensure(false, "context latest revision and serial do not match")
	}
	if receipt.Context.Plan.MinimumSuccessRatio != plan.MinimumSuccessRatio ||
		receipt.Context.Plan.MaximumLatencyMS != plan.MaximumLatencyMS ||
		receipt.Context.Plan.ExpectedNodes != plan.ExpectedNodes {
		return ensure(false, "context plan thresholds do not match")
	}
	if receipt.Context.Lease.ID != leaseID ||
		receipt.Context.Lease.State != domain.LeaseReported ||
		receipt.Context.Lease.AllocationID != allocationID ||
		receipt.Context.Lease.NodeID != nodeID ||
		receipt.Context.Lease.PlanID != plan.ID {
		return ensure(false, "context lease terminal state or ownership is wrong")
	}
	if receipt.Context.Lease.FinishedAt == nil {
		return ensure(false, "context lease finished_at is missing")
	}
	if receipt.Context.Node.ID != nodeID || receipt.Context.Node.State != domain.NodeReady {
		return ensure(false, "context node summary is not the post-submission ready state")
	}
	if receipt.Context.CapturedAt.IsZero() {
		return ensure(false, "context capture timestamp is missing")
	}
	return nil
}

// checkSameContext compares two readings of the first summary. The capture is
// immutable, so replays, stable reads, restarts, and later state changes must
// all return exactly the first summary.
func checkSameContext(want, got domain.ProbeContext, phase string) error {
	if !reflect.DeepEqual(want, got) {
		return ensure(false, phase+" returned a different submission context")
	}
	return nil
}
