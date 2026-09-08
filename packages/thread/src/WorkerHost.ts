import type { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import type { ThreadId } from "@effect-agent/core/Identifiers";
import type { SubagentBudgetReservation } from "@effect-agent/core/SubagentContract";
import { WorkerError, type WorkerRef, type WorkerSource } from "@effect-agent/core/Worker";
import { Context, type Effect, Schema } from "effect";

import type { Principal } from "./SubmissionLedger.ts";

const Positive = Schema.Int.check(Schema.isGreaterThan(0));

/** Host ceilings apply across all declarations owned by one source Thread. */
export const WorkerHostLimits = Schema.Struct({
  maxWorkersPerSource: Positive.check(Schema.isLessThanOrEqualTo(100)),
  /** Active background workers across declarations; omission retains source Tool concurrency. */
  maxActiveWorkersPerSource: Schema.optionalKey(Positive.check(Schema.isLessThanOrEqualTo(100))),
  maxInputsPerWorker: Positive.check(Schema.isLessThanOrEqualTo(1_000)),
  maxPendingInputsPerWorker: Positive.check(Schema.isLessThanOrEqualTo(100)),
  lifetimeMillis: Positive.check(Schema.isLessThanOrEqualTo(604_800_000)),
  /** Preparation may repeat after loss before its decision commits; keep callbacks side-effect free. */
  reportPreparationTimeoutMillis: Schema.optionalKey(
    Positive.check(Schema.isLessThanOrEqualTo(30_000)),
  ),
});

export type WorkerHostLimits = typeof WorkerHostLimits.Type;

export const WorkerHostConfig = Context.Reference<WorkerHostLimits>(
  "@effect-agent/thread/WorkerHostConfig",
  {
    defaultValue: () => ({
      maxWorkersPerSource: 32,
      maxInputsPerWorker: 64,
      maxPendingInputsPerWorker: 8,
      lifetimeMillis: 86_400_000,
    }),
  },
);

/**
 * Authenticate every acquisition/operation and authorize its exact source Thread. A returned
 * Principal is host-owned admission identity. Context access authenticates source metadata;
 * read, send and control remain separate grants. WorkerRefs and caller-supplied Thread
 * identifiers alone never confer any of them.
 */
export interface WorkerHostAuthorizationRequest {
  readonly sourceThreadId: ThreadId;
  readonly principal: Principal;
  readonly operation: WorkerError["operation"];
  readonly access: "context" | "read" | "send" | "control";
  readonly worker?: WorkerRef;
}

export const WorkerHostAuthorizer = Context.Reference<{
  readonly authorize: (
    request: WorkerHostAuthorizationRequest,
  ) => Effect.Effect<Principal, WorkerError>;
}>("@effect-agent/thread/WorkerHostAuthorizer", {
  defaultValue: () => ({
    authorize: (request) =>
      WorkerError.make({
        operation: request.operation,
        reason: "denied",
      }),
  }),
});

/**
 * Deployment-owned permission to fund a root's background worker independently. The decision
 * covers the exact immutable origin and is checked again before each input admission. It never
 * changes delegation depth or Tool authority. Omission denies independent funding.
 */
export const WorkerBudgetAuthorizer = Context.Reference<{
  readonly authorize: (request: {
    readonly source: WorkerSource;
    readonly principal: Principal;
    readonly worker: WorkerRef;
    readonly policy: AgentPolicy;
    readonly budget: SubagentBudgetReservation;
  }) => Effect.Effect<void, WorkerError>;
}>("@effect-agent/thread/WorkerBudgetAuthorizer", {
  defaultValue: () => ({
    authorize: () => WorkerError.make({ operation: "start", reason: "denied" }),
  }),
});
