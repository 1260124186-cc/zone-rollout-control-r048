package probe

import (
	"context"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type LeaseExpirer interface {
	ExpireDue(context.Context) (int, error)
}

type Service struct {
	store   *store.Store
	clock   platform.Clock
	ids     platform.IDGenerator
	expirer LeaseExpirer
}

func New(
	stateStore *store.Store,
	clock platform.Clock,
	ids platform.IDGenerator,
	expirer LeaseExpirer,
) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids, expirer: expirer}
}

func (s *Service) Submit(
	ctx context.Context,
	leaseID string,
	input domain.ProbeInput,
) (domain.ProbeReceipt, error) {
	now := s.clock.Now().UTC()
	if err := domain.ValidateProbeInput(input, now); err != nil {
		return domain.ProbeReceipt{}, err
	}
	if _, err := s.expirer.ExpireDue(ctx); err != nil {
		return domain.ProbeReceipt{}, err
	}
	fingerprint := domain.ProbeFingerprint(leaseID, input)
	var receipt domain.ProbeReceipt
	err := s.store.Write(ctx, func(state *domain.State) error {
		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		if lease.State == domain.LeaseReported && lease.ProbeID != "" {
			existing, found := state.Probes[lease.ProbeID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease references a missing probe", map[string]any{
					"probe_id": lease.ProbeID,
				})
			}
			if existing.PayloadFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different probe content", map[string]any{
					"lease_id":    leaseID,
					"request_key": input.RequestKey,
				})
			}
			summary, found := state.ProbeContexts[existing.ID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "probe references a missing submission context", map[string]any{
					"probe_id": existing.ID,
				})
			}
			receipt = domain.ProbeReceipt{Probe: existing, Context: summary}
			return nil
		}
		if lease.State != domain.LeaseClaimed {
			return domain.NewError(domain.CodeLeaseNotClaimed, "lease is not claimed", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		if !now.Before(lease.ExpiresAt) {
			return domain.NewError(domain.CodeLeaseExpired, "lease has expired", map[string]any{
				"lease_id":   leaseID,
				"expires_at": lease.ExpiresAt,
			})
		}
		plan, ok := state.Plans[lease.PlanID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing plan", map[string]any{
				"plan_id": lease.PlanID,
			})
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active to store probes", map[string]any{
				"plan_id": plan.ID,
				"state":   plan.State,
			})
		}
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		result := domain.Probe{
			ID:                 s.ids.New("prb"),
			LeaseID:            leaseID,
			PlanID:             lease.PlanID,
			NodeID:             lease.NodeID,
			RequestKey:         input.RequestKey,
			Success:            input.Success,
			ObservedSerial:     input.ObservedSerial,
			LatencyMS:          input.LatencyMS,
			ObservedAt:         input.ObservedAt.UTC(),
			StoredAt:           now,
			PayloadFingerprint: fingerprint,
		}
		state.Probes[result.ID] = result
		lease.State = domain.LeaseReported
		finishedAt := now
		lease.FinishedAt = &finishedAt
		lease.ProbeID = result.ID
		state.Leases[leaseID] = lease
		if node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		summary, err := captureContext(state, result.ID, plan, lease, node, now)
		if err != nil {
			return err
		}
		state.ProbeContexts[result.ID] = summary
		store.AddAudit(state, s.ids, "probe", result.ID, "stored", now, map[string]any{
			"lease_id": leaseID,
			"plan_id":  lease.PlanID,
		})
		receipt = domain.ProbeReceipt{Probe: result, Context: summary}
		return nil
	})
	return receipt, err
}

// Context re-reads the immutable submission summary bound to a probe. Later
// plan, lease, or node changes never affect the returned context.
func (s *Service) Context(ctx context.Context, probeID string) (domain.ProbeReceipt, error) {
	var receipt domain.ProbeReceipt
	err := s.store.Read(ctx, func(state *domain.State) error {
		result, ok := state.Probes[probeID]
		if !ok {
			return domain.NotFound("probe", probeID)
		}
		summary, found := state.ProbeContexts[probeID]
		if !found {
			return domain.NewError(domain.CodeStorageFailure, "probe references a missing submission context", map[string]any{
				"probe_id": probeID,
			})
		}
		receipt = domain.ProbeReceipt{Probe: result, Context: summary}
		return nil
	})
	return receipt, err
}

// captureContext builds the point-in-time summary from the same in-transaction
// state that stores the probe, so the summary is atomically bound to the
// submission. The plan argument is the pre-mutation plan value; probe storage
// never mutates the plan, so it remains the authoritative current aggregate.
func captureContext(
	state *domain.State,
	probeID string,
	plan domain.RolloutPlan,
	lease domain.NodeLease,
	node domain.ResolverNode,
	now time.Time,
) (domain.ProbeContext, error) {
	var latestSerial uint32
	if plan.LatestRevisionID != "" {
		revision, ok := state.Revisions[plan.LatestRevisionID]
		if !ok {
			return domain.ProbeContext{}, domain.NewError(domain.CodeStorageFailure, "plan references a missing revision", map[string]any{
				"revision_id": plan.LatestRevisionID,
			})
		}
		latestSerial = revision.Serial
	}
	return domain.ProbeContext{
		ProbeID: probeID,
		Plan: domain.ProbePlanContext{
			ID:                  plan.ID,
			State:               plan.State,
			Version:             plan.Version,
			Zone:                plan.Zone,
			LatestRevisionID:    plan.LatestRevisionID,
			LatestSerial:        latestSerial,
			MinimumSuccessRatio: plan.MinimumSuccessRatio,
			MaximumLatencyMS:    plan.MaximumLatencyMS,
			ExpectedNodes:       plan.ExpectedNodes,
		},
		Lease: domain.ProbeLeaseContext{
			ID:           lease.ID,
			State:        lease.State,
			PlanID:       lease.PlanID,
			NodeID:       lease.NodeID,
			AllocationID: lease.AllocationID,
			ClaimedAt:    lease.ClaimedAt,
			ExpiresAt:    lease.ExpiresAt,
			FinishedAt:   lease.FinishedAt,
		},
		Node: domain.ProbeNodeContext{
			ID:        node.ID,
			NodeName:  node.NodeName,
			Region:    node.Region,
			State:     node.State,
			Version:   node.Version,
			UpdatedAt: node.UpdatedAt,
		},
		CapturedAt: now,
	}, nil
}

func pointer(value time.Time) *time.Time {
	return &value
}
