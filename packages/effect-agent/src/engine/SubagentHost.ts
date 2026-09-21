import type { Effect, Option } from "effect";
import { Context, Schema, Stream } from "effect";

import type * as Agent from "../core/Agent.ts";
import type { AgentPolicy } from "../core/AgentPolicy.ts";
import type { Update } from "../core/AgentUpdates.ts";
import type * as FailureDiagnostic from "../core/FailureDiagnostic.ts";
import type { DelegationId, RunId, SettlementId, ThreadId } from "../core/Identifiers.ts";
import type { MessageRef, MessageStatus } from "../core/Messaging.ts";
import { type IdempotencyKey, type JoinedToHost, type Receipt } from "../core/Receipt.ts";
import type { SubagentBudgetReservation } from "../core/SubagentContract.ts";
import {
  WorkerError,
  type WorkerStop,
  type WorkerStopped,
  type WorkerCompletion,
  type WorkerHistoryEntry,
  type WorkerContext,
  type WorkerBudgetScope,
  type WorkerSource,
  type WorkerPage,
  type WorkerRef,
  type WorkerStarted,
  type WorkerSummary,
} from "../core/Worker.ts";

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

/**
 * First-admission preparation stays lazy until the owner authorizes the caller and checks the
 * retained command. Replays compare declared parameters/options and reuse the original capture.
 * Prepared policy, budget, and allowance are frozen first-admission configuration, not replay
 * arguments; changing them requires a new command key. Current caller authorization still applies.
 * Preparation must be free of external effects: concurrent first admissions can prepare before
 * the retained delivery chooses its first writer.
 */
export interface DeferredStartWorkerRequest<E = never, R = never> extends Pick<
  StartWorkerRequest,
  | "delegationId"
  | "target"
  | "idempotencyKey"
  | "encodedParameters"
  | "encodedGrant"
  | "budgetScope"
> {
  readonly prepare: Effect.Effect<
    Pick<StartWorkerRequest, "encodedInput" | "policy" | "budget" | "toolCallAllowance">,
    E,
    R
  >;
}

export interface FollowUpWorkerRequest {
  readonly worker: WorkerRef;
  readonly target: Agent.AnyDefinition;
  readonly idempotencyKey: IdempotencyKey;
  readonly encodedInput: unknown;
  readonly encodedParameters: unknown;
}

/**
 * Like deferred starts, follow-ups prepare only after current authorization and retained-command
 * lookup. Replays compare declared parameters and reuse the original input and worker authority.
 * Preparation must be free of external effects because concurrent first admissions can prepare.
 */
export interface DeferredFollowUpWorkerRequest<E = never, R = never> extends Omit<
  FollowUpWorkerRequest,
  "encodedInput"
> {
  readonly prepare: Effect.Effect<Pick<FollowUpWorkerRequest, "encodedInput">, E, R>;
}

export interface WorkerReceiptRequest {
  readonly worker: WorkerRef;
  readonly target: Agent.AnyDefinition;
  readonly receipt: Receipt;
}

export interface WorkerMessageRequest {
  readonly worker: WorkerRef;
  readonly target: Agent.AnyDefinition;
  readonly message: MessageRef;
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
      /** Operator-private failed-settlement evidence; never part of a model Tool result. */
      readonly diagnostic?: FailureDiagnostic.Failure;
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
 * Source-owned standard projection discovered from background tools.
 * The concrete descriptor retains its E/R; durable registration captures R and records
 * bounded preparation failure rather than serializing arbitrary application errors.
 */
export interface WorkerReporting<E = never, R = never> {
  readonly delegationId: DelegationId;
  readonly target: Agent.AnyDefinition;
  /** Pure first-emission selection. False retains the canonical update without parent delivery. */
  readonly reportUpdate?: (update: Update) => boolean;
  readonly prepare: (report: WorkerRunReport) => Effect.Effect<
    {
      readonly message: WorkerCompletion;
    },
    E | WorkerReportPreparationFailure,
    R
  >;
}

/** Descriptor carried by background tools; registration discovers it without an app handoff. */
export const BackgroundReporting = Context.Reference<
  WorkerReporting<WorkerReportPreparationFailure> | undefined
>("@effect-agent/engine/BackgroundReporting", { defaultValue: () => undefined });

/**
 * Trusted, caller-bound host port. The interpreter supplies a fresh facet for each Tool Call;
 * programmatic callers acquire a separately authorized Thread facet. Request values cannot
 * choose the source identity, replace worker provenance or expand its authority and budgets.
 */
export class SubagentHost extends Context.Service<
  SubagentHost,
  {
    readonly context: Effect.Effect<WorkerContext, WorkerError>;
    /** Resolve prepared input through host authority; None retains legacy target inheritance. */
    readonly resolveTargetPolicy: (request: {
      readonly target: Agent.AnyDefinition;
      readonly encodedInput: unknown;
    }) => Effect.Effect<Option.Option<AgentPolicy>, WorkerError>;
    /** Prepared callers retain strict input/policy conflicts; deferred callers replay the saved capture. */
    readonly start: <E = never, R = never>(
      request: StartWorkerRequest | DeferredStartWorkerRequest<E, R>,
    ) => Effect.Effect<WorkerStarted, WorkerError | E, R>;
    /** Prepared callers retain strict input conflicts; deferred callers replay the saved capture. */
    readonly followUp: <E = never, R = never>(
      request: FollowUpWorkerRequest | DeferredFollowUpWorkerRequest<E, R>,
    ) => Effect.Effect<MessageStatus, WorkerError | E, R>;
    readonly inspect: (
      request: WorkerReceiptRequest | WorkerMessageRequest,
    ) => Effect.Effect<WorkerObservation | MessageStatus, WorkerError>;
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
    /** Seal the whole worker; retries reconcile the same command, including a lost acknowledgement. */
    readonly stop: (
      request: WorkerStop & { readonly target: Agent.AnyDefinition },
    ) => Effect.Effect<WorkerStopped, WorkerError>;
    /** Cancel exactly this Receipt; preserve JoinedToHost without broadening its target. */
    readonly cancel: (
      request: WorkerReceiptRequest,
    ) => Effect.Effect<void, WorkerError | JoinedToHost>;
  }
>()("@effect-agent/engine/SubagentHost") {
  static readonly unavailable: SubagentHost["Service"] = {
    context: WorkerError.make({ operation: "context", reason: "unavailable" }),
    resolveTargetPolicy: () => WorkerError.make({ operation: "start", reason: "unavailable" }),
    start: () => WorkerError.make({ operation: "start", reason: "unavailable" }),
    followUp: () => WorkerError.make({ operation: "followUp", reason: "unavailable" }),
    inspect: () => WorkerError.make({ operation: "inspect", reason: "unavailable" }),
    summary: () => WorkerError.make({ operation: "inspect", reason: "unavailable" }),
    observe: () => Stream.fail(WorkerError.make({ operation: "observe", reason: "unavailable" })),
    await: () => WorkerError.make({ operation: "await", reason: "unavailable" }),
    list: () => WorkerError.make({ operation: "list", reason: "unavailable" }),
    stop: () => WorkerError.make({ operation: "stop", reason: "unavailable" }),
    cancel: () => WorkerError.make({ operation: "cancel", reason: "unavailable" }),
  };

  /** Runtime-owned per-call binding. Unconfigured Runs deny background operations. */
  static readonly forTool = Context.Reference<
    (source: Extract<WorkerSource, { readonly _tag: "tool" }>) => SubagentHost["Service"]
  >("@effect-agent/engine/SubagentHost/forTool", {
    defaultValue: () => () => SubagentHost.unavailable,
  });
}
