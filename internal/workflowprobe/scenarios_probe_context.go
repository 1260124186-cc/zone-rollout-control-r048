package workflowprobe

import (
	"fmt"
	"net/http"
	"reflect"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runProbeSubmissionContext() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	node, err := enrollNode(harness, "resolver-context-01", "north")
	if err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "context.example", 5001, 1, 0.75, 50)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "context-lease-0001", "north", 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]

	first, err := submitProbe(harness, leaseID, "context-request-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if err := assertContextShape(first, plan, revision, node, leaseID); err != nil {
		return err
	}
	if !first.Success || first.ObservedSerial != revision.Serial || first.LatencyMS != 12 {
		return ensure(false, "original probe fields were not preserved on submit")
	}
	firstRevision := first.StateRevision
	if firstRevision == 0 {
		return ensure(false, "submission context did not bind a state revision")
	}

	// The stable probe-identifier entry rereads the same atomically bound context.
	var reread domain.ProbeContext
	if err := harness.call(http.MethodGet, "/v1/probes/"+first.ID+"/context", nil, &reread); err != nil {
		return err
	}
	if !reflect.DeepEqual(reread, first) {
		return ensure(false, "context endpoint did not return the first submission summary")
	}

	// Same-key same-content replay returns the first summary.
	replay, err := submitProbe(harness, leaseID, "context-request-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if replay.ID != first.ID || !reflect.DeepEqual(replay, first) {
		return ensure(false, "same-key replay did not return the first submission context")
	}

	// Failed submissions must not create a context.
	_, err = submitProbe(harness, leaseID, "context-request-0001", true, revision.Serial, 99)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/probes", domain.ProbeInput{
		RequestKey:     "bad",
		Success:        true,
		ObservedSerial: revision.Serial,
		LatencyMS:      1,
		ObservedAt:     harness.clock.Now(),
	}, nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	err = harness.call(http.MethodGet, "/v1/probes/prb_unknown/context", nil, nil)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	// Drive later state changes: node is leased again and the plan completes.
	secondGroup, err := allocate(harness, plan.ID, "context-lease-0002", "north", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(harness, secondGroup.LeaseIDs[0], "context-request-0002", false, revision.Serial, 40); err != nil {
		return err
	}
	var completed domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version,
	}, &completed); err != nil {
		return err
	}
	if completed.Version != plan.Version+1 {
		return ensure(false, "completing the plan did not advance the plan version")
	}

	// After later state changes, every read path still serves the first summary.
	var afterChanges domain.ProbeContext
	if err := harness.call(http.MethodGet, "/v1/probes/"+first.ID+"/context", nil, &afterChanges); err != nil {
		return err
	}
	if !reflect.DeepEqual(afterChanges, first) {
		return ensure(false, "later state changes altered the first submission context")
	}
	replayAfterCompletion, err := submitProbe(harness, leaseID, "context-request-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(replayAfterCompletion, first) {
		return ensure(false, "replay after plan completion did not return the first summary")
	}

	// A process restart over the same snapshot keeps the identical summary.
	if err := harness.restart(); err != nil {
		return err
	}
	var afterRestart domain.ProbeContext
	if err := harness.call(http.MethodGet, "/v1/probes/"+first.ID+"/context", nil, &afterRestart); err != nil {
		return err
	}
	if !reflect.DeepEqual(afterRestart, first) {
		return ensure(false, "submission context changed across a service restart")
	}
	if afterRestart.StateRevision != firstRevision {
		return ensure(false, "bound state revision was not preserved across restart")
	}
	replayAfterRestart, err := submitProbe(harness, leaseID, "context-request-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(replayAfterRestart, first) {
		return ensure(false, "replay after restart did not return the first summary")
	}
	return nil
}

func assertContextShape(
	contextRecord domain.ProbeContext,
	plan domain.RolloutPlan,
	revision domain.ZoneRevision,
	node domain.ResolverNode,
	leaseID string,
) error {
	if contextRecord.ID == "" || contextRecord.LeaseID != leaseID || contextRecord.PlanID != plan.ID || contextRecord.NodeID != node.ID {
		return ensure(false, "context does not bind the original probe identity")
	}
	if contextRecord.Plan.ID != plan.ID ||
		contextRecord.Plan.State != domain.PlanActive ||
		contextRecord.Plan.Version != plan.Version ||
		contextRecord.Plan.LatestRevisionID != revision.ID ||
		contextRecord.Plan.LatestSerial != revision.Serial ||
		contextRecord.Plan.MinimumSuccessRatio != plan.MinimumSuccessRatio ||
		contextRecord.Plan.MaximumLatencyMS != plan.MaximumLatencyMS ||
		contextRecord.Plan.ExpectedNodes != plan.ExpectedNodes {
		return ensure(false, "context plan snapshot does not match the atomic submission point")
	}
	if contextRecord.Lease.ID != leaseID ||
		contextRecord.Lease.State != domain.LeaseReported ||
		contextRecord.Lease.ProbeID != contextRecord.ID ||
		contextRecord.Lease.FinishedAt == nil {
		return ensure(false, "context lease terminal state is not reported with the probe bound")
	}
	if contextRecord.Node.ID != node.ID ||
		contextRecord.Node.NodeName != node.NodeName ||
		contextRecord.Node.Region != node.Region ||
		contextRecord.Node.State != domain.NodeReady ||
		contextRecord.Node.Version != node.Version+2 {
		return fmt.Errorf("context node summary does not match the post-submission node")
	}
	if contextRecord.CapturedAt.IsZero() {
		return ensure(false, "context is missing its capture timestamp")
	}
	return nil
}
