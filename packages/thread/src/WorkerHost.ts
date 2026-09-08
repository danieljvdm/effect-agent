import type * as Agent from "@effect-agent/core/Agent";
import type { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import type { SubmissionId, ThreadId } from "@effect-agent/core/Identifiers";
import type { SubagentBudgetReservation } from "@effect-agent/core/SubagentContract";
import { WorkerError, type WorkerRef, type WorkerSource } from "@effect-agent/core/Worker";
import { Context, Effect, Option, Schema } from "effect";

import type { DefinitionDigests, Digest, PersistedJson, WorkerOrigin } from "./Records.ts";
import type { Principal, SubmissionSnapshot } from "./SubmissionLedger.ts";

/** Host-verified source input; absence never means the latest input in the Thread. */
export interface WorkerPolicySource {
  readonly threadId: ThreadId;
  readonly definition: Agent.AnyDefinition;
  readonly definitions: DefinitionDigests;
  readonly submission?: SubmissionSnapshot;
}

/** Initial input is validated again at destination admission, including idempotent replay. */
export type WorkerPolicyTarget = {
  readonly definition: Agent.AnyDefinition;
  readonly definitions: DefinitionDigests;
  readonly source: WorkerSource;
} & (
  | {
      readonly _tag: "InitialInput";
      readonly sourceSubmission?: SubmissionSnapshot;
      readonly input: PersistedJson;
      readonly inputDigest: Digest;
    }
  | {
      readonly _tag: "RetainedWorker";
      /** Verified against canonical source history before this request reaches the resolver. */
      readonly origin: WorkerOrigin;
    }
);

/**
 * Opt-in host authority for policies captured in immutable application input. None preserves
 * the registered Definition's existing policy/override semantics. Some is a complete policy,
 * never merged with static overrides. Decode and authorize the exact captured revision; do not
 * read mutable settings or substitute a current capture for missing historical authority.
 * RetainedWorker may affirm only origin.policy, never replace a continuing worker's allowance.
 * Return WorkerError(unavailable) for temporarily unavailable authority so delivery can retry.
 * Implementations are acquired through Effect Layers; capture their dependencies in Layer R.
 */
export const WorkerPolicyResolver = Context.Reference<{
  readonly resolveSource: (
    request: WorkerPolicySource,
  ) => Effect.Effect<Option.Option<AgentPolicy>, WorkerError>;
  readonly resolveTarget: (
    request: WorkerPolicyTarget,
  ) => Effect.Effect<Option.Option<AgentPolicy>, WorkerError>;
}>("@effect-agent/thread/WorkerPolicyResolver", {
  defaultValue: () => ({
    resolveSource: () => Effect.succeed(Option.none()),
    resolveTarget: () => Effect.succeed(Option.none()),
  }),
});

const Positive = Schema.Int.check(Schema.isGreaterThan(0));

/** A host-authorized ceiling on active background workers, not retained workers or Run usage. */
export const WorkerConcurrencyLimit = Schema.Struct({
  maxActiveWorkersPerSource: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});

export type WorkerConcurrencyLimit = typeof WorkerConcurrencyLimit.Type;

/**
 * Optional source-aware operational authority. Resolution occurs inside every new source
 * reservation attempt, against the same canonical history used for the append CAS. None
 * preserves WorkerHostConfig; Some can only narrow it. The selected submission is exact,
 * never the latest input. Implementations must authorize that immutable capture and retain
 * construction dependencies in Layer R. Unavailable authority must return WorkerError(unavailable).
 *
 * One worker occupies a slot while any of its inputs remain unacknowledged. Steering an active
 * worker does not acquire another slot. Decreases (including zero) do not cancel incumbents;
 * an idle worker needs a slot again. Idempotent retained reservations do not reacquire slots.
 */
export const WorkerConcurrencyResolver = Context.Reference<{
  readonly resolve: (request: {
    readonly source: WorkerSource;
    readonly sourceSubmission?: SubmissionSnapshot;
    readonly worker: WorkerRef;
    readonly principal: Principal;
  }) => Effect.Effect<Option.Option<WorkerConcurrencyLimit>, WorkerError>;
}>("@effect-agent/thread/WorkerConcurrencyResolver", {
  defaultValue: () => ({ resolve: () => Effect.succeed(Option.none()) }),
});

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
  /** An explicitly selected owner input; authorize this locator as well as the Thread. */
  readonly sourceSubmissionId?: SubmissionId;
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
