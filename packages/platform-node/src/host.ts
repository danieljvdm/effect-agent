import type { ConversationId, SubmissionId } from "@effect-agent/core";
import {
  AgentBindingResolver,
  type ChildAdmissionAuthorizer,
  DurableAgentRuntime,
  type OperationAuthorizer,
  type AbortCommand,
  type AbortIntent,
  type CanonicalRecordEnvelope,
  type ConversationNotMaterialized,
  type ConversationStoreError,
  type DurableAbortFailure,
  type DurableAwaitFailure,
  type DurableBindingFailure,
  type DurableExplainFailure,
  type DurableObserveOptions,
  type DurableObligationFailure,
  type DurableRetryFailure,
  type DurableSubmitAgent,
  type DurableSubmitFailure,
  type DurableSubmitOptions,
  type DurableVerifyFailure,
  type DurableWorkerFailure,
  type IntegrityReport,
  type LedgerError,
  type ObligationReport,
  type ObligationThresholds,
  type OperationDenied,
  type OperationCaller,
  type Receipt,
  type RecoveryExplanation,
  type RecoveryReport,
  type RetryCommand,
  type Settlement,
} from "@effect-agent/session";
import { Context, Effect, Layer, Ref, Schema, type Stream } from "effect";

import {
  NodeDurableRuntime,
  NodeDurableRuntimeConfig,
  type NodeDurableRuntimeInitializationError,
  type NodeDurableRuntimeOptions,
  type NodeDurableRuntimeServices,
} from "./layers.ts";

/**
 * Admission is not open on this host: it is shutting down (deployment §6 step 1, DEPLOY-005).
 * Accepted work is unaffected — only NEW admissions are refused.
 */
export class AdmissionClosed extends Schema.TaggedError<AdmissionClosed>()("AdmissionClosed", {
  message: Schema.String,
}) {}

const makeHost = Effect.gen(function* () {
  const runtime = yield* DurableAgentRuntime;
  const config = yield* NodeDurableRuntimeConfig;
  const bindingResolver = yield* AgentBindingResolver;

  // Startup gate (deployment §5, plan §host): configuration decoding and storage compatibility
  // already gated this Layer's dependencies; the last gate before admission opens is recovering
  // EVERY nonterminal Submission. Work needing a live Agent Binding is reported `deferred` and
  // stays a visible obligation for `runWorkers`; lanes durably blocked on an Unknown Outcome are
  // reported `unknown` and wait for the authorized `resolveUnknown` path (DUR-017) — they consume
  // no worker permit while the settlement obligation stays owed.
  const startupRecovery = yield* runtime.runRecovery;

  const admission = yield* Ref.make(true);
  // Shutdown step 1 (DEPLOY-005): admission closes before the ownership drain in the runtime
  // Layer below releases claims and before the stores close in reverse acquisition order.
  yield* Effect.addFinalizer(() => Ref.set(admission, false));

  const requireAdmission: Effect.Effect<void, AdmissionClosed> = Ref.get(admission).pipe(
    Effect.flatMap((open) =>
      open
        ? Effect.void
        : Effect.fail(
            AdmissionClosed.make({ message: "The host is shutting down; admission is closed." }),
          ),
    ),
  );

  const submit = <InputSchema extends Schema.Top>(
    agent: DurableSubmitAgent<InputSchema>,
    input: InputSchema["Type"],
    options: DurableSubmitOptions,
  ): Effect.Effect<
    Receipt,
    AdmissionClosed | DurableSubmitFailure,
    InputSchema["EncodingServices"]
  > => requireAdmission.pipe(Effect.andThen(runtime.submit(agent, input, options)));

  const runWorkers = <A, E, R>(worker: Effect.Effect<A, E, R>): Effect.Effect<void, E, R> =>
    Effect.forEach(
      Array.from({ length: config.workerConcurrency }, (_, index) => index),
      () => worker,
      { concurrency: "unbounded", discard: true },
    );

  // S2 multi-binding pool: every claimed head resolves its exact registered Binding through
  // the host's `AgentBindingResolver` (`NodeDurableRuntimeOptions.bindings`), so ONE bounded
  // pool serves parent and child lanes — the spec §12 smallest-pool suspension/wakeup proof
  // runs `workerConcurrency: 1` over exactly this loop.
  const runResolvedWorkers = runWorkers(
    runtime.runResolvedWorker.pipe(Effect.provideService(AgentBindingResolver, bindingResolver)),
  );

  return NodeDurableHost.of({
    startupRecovery,
    admissionOpen: Ref.get(admission),
    submit,
    awaitSettlement: runtime.awaitSettlement,
    observe: runtime.observe,
    abort: runtime.abort,
    explain: runtime.explain,
    explainConversation: runtime.explainConversation,
    verify: runtime.verify,
    retry: runtime.retry,
    wake: runtime.wake,
    scanObligations: runtime.scanObligations,
    runWorkers,
    runResolvedWorkers,
  });
});

/**
 * Operational host lifecycle for the DN runtime (deployment §2/§5/§6).
 *
 * Startup gates run during Layer construction, so the service existing implies readiness:
 * configuration was schema-decoded, the SQLite file passed the exact-version compatibility check,
 * and every nonterminal Submission went through one full recovery pass BEFORE admission opened.
 * `startupRecovery` is the auditable evidence of that reconciliation pass.
 *
 * Shutdown runs in reverse Layer order when the owning Scope closes: `submit` starts refusing
 * with `AdmissionClosed` first, then the runtime Layer's ownership drain releases every claim
 * still held so another host can take over the lanes immediately, then the SQLite resources
 * close. Forced termination at any point stays safe — the durability protocol, not graceful
 * shutdown, provides correctness (DEPLOY-006).
 */
export class NodeDurableHost extends Context.Service<
  NodeDurableHost,
  {
    /**
     * The recovery decisions executed (or deferred) by this host's startup reconciliation.
     * Reports with the `unknown` disposition identify lanes blocked on Unknown Outcomes that
     * only the authorized DUR-017 resolution path can release.
     */
    readonly startupRecovery: ReadonlyArray<RecoveryReport>;
    /** Admission-role readiness (deployment §7): true until shutdown begins. */
    readonly admissionOpen: Effect.Effect<boolean>;
    /** `DurableAgentRuntime.submit` behind the host admission gate. */
    readonly submit: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: DurableSubmitOptions,
    ) => Effect.Effect<
      Receipt,
      AdmissionClosed | DurableSubmitFailure,
      InputSchema["EncodingServices"]
    >;
    readonly awaitSettlement: (
      receipt: Receipt,
      caller: OperationCaller,
    ) => Effect.Effect<Settlement, DurableAwaitFailure>;
    readonly observe: (
      receipt: Receipt,
      caller: OperationCaller,
      options?: DurableObserveOptions,
    ) => Stream.Stream<
      CanonicalRecordEnvelope,
      ConversationStoreError | ConversationNotMaterialized | LedgerError | OperationDenied
    >;
    readonly abort: (
      command: AbortCommand,
      caller: OperationCaller,
    ) => Effect.Effect<AbortIntent, DurableAbortFailure>;
    /** `DurableAgentRuntime.explain` — read-only recovery explanation of one Submission (P7). */
    readonly explain: (
      submissionId: SubmissionId,
      caller: OperationCaller,
    ) => Effect.Effect<RecoveryExplanation, DurableExplainFailure>;
    /** `DurableAgentRuntime.explainConversation` — explain every nonterminal lane member. */
    readonly explainConversation: (
      conversationId: ConversationId,
      caller: OperationCaller,
    ) => Effect.Effect<ReadonlyArray<RecoveryExplanation>, DurableExplainFailure>;
    /** `DurableAgentRuntime.verify` — read-only integrity checks, never a repair (P7). */
    readonly verify: (
      conversationId: ConversationId,
      caller: OperationCaller,
    ) => Effect.Effect<IntegrityReport, DurableVerifyFailure>;
    /** `DurableAgentRuntime.retry` — audited single-Submission re-drive with typed refusals. */
    readonly retry: (
      command: RetryCommand,
      caller: OperationCaller,
    ) => Effect.Effect<RecoveryReport, DurableRetryFailure>;
    /** `DurableAgentRuntime.wake` — the documented operator liveness nudge for one lane. */
    readonly wake: (
      conversationId: ConversationId,
      caller: OperationCaller,
    ) => Effect.Effect<void, OperationDenied>;
    /** `DurableAgentRuntime.scanObligations` — the scan-based DUR-017/OPS-001 report. */
    readonly scanObligations: (
      thresholds: ObligationThresholds,
      caller: OperationCaller,
    ) => Effect.Effect<ObligationReport, DurableObligationFailure>;
    /**
     * Run `workerConcurrency` copies of the given worker effect (typically
     * `DurableAgentRuntime.runWorker(agent)`) until the caller's Scope interrupts them. The
     * bound is the validated finite configuration value; the host never forks daemon fibers.
     */
    readonly runWorkers: <A, E, R>(worker: Effect.Effect<A, E, R>) => Effect.Effect<void, E, R>;
    /**
     * Run `workerConcurrency` copies of `DurableAgentRuntime.runResolvedWorker` over the host's
     * registered Bindings (S2): every claimed head resolves its exact stored Binding before any
     * code runs (SUB-023), so one bounded pool serves parent and attached-child lanes.
     */
    readonly runResolvedWorkers: Effect.Effect<void, DurableWorkerFailure | DurableBindingFailure>;
  }
>()("@effect-agent/platform-node/NodeDurableHost") {
  /** Host gates over an already-assembled `NodeDurableRuntime` stack. */
  static readonly layer: Layer.Layer<
    NodeDurableHost,
    DurableWorkerFailure,
    DurableAgentRuntime | NodeDurableRuntimeConfig | AgentBindingResolver
  > = Layer.effect(NodeDurableHost)(makeHost);

  /**
   * The complete DN host: `NodeDurableRuntime.layer(options)` plus the host lifecycle gates.
   * Authorization policies remain requirements for the application composition root.
   */
  static layerStack(
    options: NodeDurableRuntimeOptions,
  ): Layer.Layer<
    NodeDurableHost | NodeDurableRuntimeServices,
    DurableWorkerFailure | NodeDurableRuntimeInitializationError,
    OperationAuthorizer | ChildAdmissionAuthorizer
  > {
    return NodeDurableHost.layer.pipe(Layer.provideMerge(NodeDurableRuntime.layer(options)));
  }
}
