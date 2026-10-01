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
) (domain.ProbeContext, error) {
	now := s.clock.Now().UTC()
	if err := domain.ValidateProbeInput(input, now); err != nil {
		return domain.ProbeContext{}, err
	}
	if _, err := s.expirer.ExpireDue(ctx); err != nil {
		return domain.ProbeContext{}, err
	}
	fingerprint := domain.ProbeFingerprint(leaseID, input)
	var result domain.ProbeContext
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
			contextRecord, found := state.ProbeContexts[existing.ID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "probe references a missing submission context", map[string]any{
					"probe_id": existing.ID,
				})
			}
			result = contextRecord
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
		if plan.LatestRevisionID == "" {
			return domain.NewError(domain.CodeStorageFailure, "active plan references no latest revision", map[string]any{
				"plan_id": plan.ID,
			})
		}
		revision, ok := state.Revisions[plan.LatestRevisionID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing revision", map[string]any{
				"revision_id": plan.LatestRevisionID,
			})
		}
		stored := domain.Probe{
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
		state.Probes[stored.ID] = stored
		lease.State = domain.LeaseReported
		finishedAt := now
		lease.FinishedAt = &finishedAt
		lease.ProbeID = stored.ID
		state.Leases[leaseID] = lease
		if node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		result = buildContext(stored, plan, revision, lease, node, state.Revision, now)
		state.ProbeContexts[stored.ID] = result
		store.AddAudit(state, s.ids, "probe", stored.ID, "stored", now, map[string]any{
			"lease_id": leaseID,
			"plan_id":  lease.PlanID,
		})
		return nil
	})
	return result, err
}

// Context returns the immutable summary captured by the first successful
// submission of the given probe. It never reflects later plan, lease, or node
// changes.
func (s *Service) Context(ctx context.Context, probeID string) (domain.ProbeContext, error) {
	var result domain.ProbeContext
	err := s.store.Read(ctx, func(state *domain.State) error {
		found, ok := state.ProbeContexts[probeID]
		if !ok {
			return domain.NotFound("probe_context", probeID)
		}
		result = found
		return nil
	})
	return result, err
}

func buildContext(
	probe domain.Probe,
	plan domain.RolloutPlan,
	revision domain.ZoneRevision,
	lease domain.NodeLease,
	node domain.ResolverNode,
	stateRevision uint64,
	now time.Time,
) domain.ProbeContext {
	return domain.ProbeContext{
		Probe: probe,
		Plan: domain.ProbePlanContext{
			ID:                  plan.ID,
			State:               plan.State,
			Version:             plan.Version,
			LatestRevisionID:    plan.LatestRevisionID,
			LatestSerial:        revision.Serial,
			MinimumSuccessRatio: plan.MinimumSuccessRatio,
			MaximumLatencyMS:    plan.MaximumLatencyMS,
			ExpectedNodes:       plan.ExpectedNodes,
		},
		Lease: domain.ProbeLeaseContext{
			ID:           lease.ID,
			PlanID:       lease.PlanID,
			NodeID:       lease.NodeID,
			AllocationID: lease.AllocationID,
			State:        lease.State,
			ClaimedAt:    lease.ClaimedAt,
			ExpiresAt:    lease.ExpiresAt,
			FinishedAt:   lease.FinishedAt,
			ProbeID:      lease.ProbeID,
		},
		Node: domain.ProbeNodeContext{
			ID:        node.ID,
			NodeName:  node.NodeName,
			Region:    node.Region,
			State:     node.State,
			Version:   node.Version,
			UpdatedAt: node.UpdatedAt,
		},
		StateRevision: stateRevision,
		CapturedAt:    now,
	}
}
