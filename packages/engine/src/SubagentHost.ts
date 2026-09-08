import type * as Agent from "@effect-agent/core/Agent";
import type { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import type { DelegationId, RunId, SettlementId, ThreadId } from "@effect-agent/core/Identifiers";
import { type IdempotencyKey, type JoinedToHost, type Receipt } from "@effect-agent/core/Receipt";
import type { SubagentBudgetReservation } from "@effect-agent/core/SubagentContract";
import {
  WorkerError,
  type WorkerHistoryEntry,
  type WorkerContext,
  type WorkerBudgetScope,
  type WorkerSource,
  type WorkerPage,
  type WorkerRef,
  type WorkerStarted,
  type WorkerSummary,
} from "@effect-agent/core/Worker";
import type { Effect } from "effect";
import { Context, Schema, Stream } from "effect";

/** Prepared values cross this port only to be Schema-decoded before durable storage. */
export interface StartWorkerRequest {
  readonly delegationId: DelegationId;
  readonly target: Agent.AnyDefinition;
  readonly idempotencyKey: IdempotencyKey;
  readonly encodedInput: unknown;
  readonly encodedParameters: unknown;
  readonly policy: AgentPolicy;
  readonly budget: SubagentBudgetReservation;
  readonly encodedGrant: unknown;
  readonly toolCallAllowance?: number;
  /** Author-owned request; the durable host must separately authorize worker-run funding. */
  readonly budgetScope?: WorkerBudgetScope;
}

export interface FollowUpWorkerRequest {
  readonly worker: WorkerRef;
  readonly target: Agent.AnyDefinition;
  readonly idempotencyKey: IdempotencyKey;
  readonly encodedInput: unknown;
  readonly encodedParameters: unknown;
}

export interface WorkerReceiptRequest {
  readonly worker: WorkerRef;
  readonly target: Agent.AnyDefinition;
  readonly receipt: Receipt;
}

/**
 * Canonical terminal values remain encoded until the capability decodes the saved parameters
 * and target output and applies its declared result projection. Child E does not survive settlement.
 */
export type WorkerObservation =
  | { readonly _tag: "Pending"; readonly receipt: Receipt }
  | {
      readonly _tag: "Settled";
      readonly receipt: Receipt;
      /** Absent when an input failed or was cancelled before any Run started. */
      readonly runId?: RunId;
      readonly settlementId: SettlementId;
      readonly outcome: "completed" | "failed" | "aborted";
      readonly encodedParameters: unknown;
      readonly encodedResult: unknown;
      readonly budgetExhausted: boolean;
    };

/** One actual canonical Run, independent of how many input Receipts joined it. */
export interface WorkerRunReport {
  readonly worker: WorkerRef;
  readonly context: WorkerContext;
  readonly observation: Extract<WorkerObservation, { readonly _tag: "Settled" }> & {
    readonly runId: RunId;
  };
}

/** A bounded projection failure; raw application errors never enter durable report records. */
export class WorkerReportPreparationFailure extends Schema.TaggedError<WorkerReportPreparationFailure>()(
  "WorkerReportPreparationFailure",
  { stage: Schema.Literals(["projection", "input", "preparation"]) },
) {}

/**
 * Source-registration-owned conversion from a child's outcome to new source input.
 * The concrete descriptor retains its E/R; durable registration captures R and records
 * bounded preparation failure rather than serializing arbitrary application errors.
 */
export interface WorkerReporting<E = never, R = never> {
  readonly delegationId: DelegationId;
  readonly target: Agent.AnyDefinition;
  readonly input: Schema.Top;
  /** Required when the receiving coordinator is itself an established worker. */
  readonly destination?: {
    readonly delegationId: DelegationId;
    readonly target: Agent.AnyDefinition;
  };
  readonly prepare: (report: WorkerRunReport) => Effect.Effect<
    {
      readonly encodedInput: Schema.Json;
      readonly encodedParameters?: Schema.Json;
    },
    E | WorkerReportPreparationFailure,
    R
  >;
}

/**
 * Trusted, caller-bound host port. The interpreter supplies a fresh facet for each Tool Call;
 * programmatic callers acquire a separately authorized Thread facet. Request values cannot
 * choose the source identity, replace worker provenance or expand its authority and budgets.
 */
export class SubagentHost extends Context.Service<
  SubagentHost,
  {
    readonly context: Effect.Effect<WorkerContext, WorkerError>;
    readonly start: (request: StartWorkerRequest) => Effect.Effect<WorkerStarted, WorkerError>;
    readonly followUp: (request: FollowUpWorkerRequest) => Effect.Effect<Receipt, WorkerError>;
    readonly inspect: (
      request: WorkerReceiptRequest,
    ) => Effect.Effect<WorkerObservation, WorkerError>;
    /** Inspect the continuing worker using the same summary contract as discovery. */
    readonly summary: (request: {
      readonly worker: WorkerRef;
      readonly target: Agent.AnyDefinition;
    }) => Effect.Effect<WorkerSummary, WorkerError>;
    /** A finite snapshot through the captured tail; resume with the last sequence as `after`. */
    readonly observe: (request: {
      readonly worker: WorkerRef;
      readonly target: Agent.AnyDefinition;
      readonly after?: number;
    }) => Stream.Stream<WorkerHistoryEntry, WorkerError>;
    /** Interrupting this wait never requests cancellation of accepted work. */
    readonly await: (
      request: WorkerReceiptRequest,
    ) => Effect.Effect<WorkerObservation, WorkerError>;
    readonly list: (request: {
      readonly delegationId: DelegationId;
      readonly target: Agent.AnyDefinition;
      readonly limit: number;
      readonly after?: ThreadId;
    }) => Effect.Effect<WorkerPage, WorkerError>;
    /** Cancel exactly this Receipt; preserve JoinedToHost without broadening its target. */
    readonly cancel: (
      request: WorkerReceiptRequest,
    ) => Effect.Effect<void, WorkerError | JoinedToHost>;
  }
>()("@effect-agent/engine/SubagentHost") {
  static readonly unavailable: SubagentHost["Service"] = {
    context: WorkerError.make({ operation: "context", reason: "unavailable" }),
    start: () => WorkerError.make({ operation: "start", reason: "unavailable" }),
    followUp: () => WorkerError.make({ operation: "followUp", reason: "unavailable" }),
    inspect: () => WorkerError.make({ operation: "inspect", reason: "unavailable" }),
    summary: () => WorkerError.make({ operation: "inspect", reason: "unavailable" }),
    observe: () => Stream.fail(WorkerError.make({ operation: "observe", reason: "unavailable" })),
    await: () => WorkerError.make({ operation: "await", reason: "unavailable" }),
    list: () => WorkerError.make({ operation: "list", reason: "unavailable" }),
    cancel: () => WorkerError.make({ operation: "cancel", reason: "unavailable" }),
  };

  /** Runtime-owned per-call binding. Unconfigured Runs deny background operations. */
  static readonly forTool = Context.Reference<
    (source: Extract<WorkerSource, { readonly _tag: "tool" }>) => SubagentHost["Service"]
  >("@effect-agent/engine/SubagentHost/forTool", {
    defaultValue: () => () => SubagentHost.unavailable,
  });
}
