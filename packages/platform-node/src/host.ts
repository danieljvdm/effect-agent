import {
  DurableAgentRuntime,
  type AbortCommand,
  type AbortIntent,
  type CanonicalRecordEnvelope,
  type ConversationNotMaterialized,
  type ConversationStoreError,
  type DurableAbortFailure,
  type DurableAwaitFailure,
  type DurableObserveOptions,
  type DurableSubmitAgent,
  type DurableSubmitFailure,
  type DurableSubmitOptions,
  type DurableWorkerFailure,
  type Receipt,
  type RecoveryReport,
  type Settlement,
} from "@effect-agent/session";
import { Context, Effect, Layer, Ref, Schema, Stream } from "effect";

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
export class AdmissionClosed extends Schema.TaggedErrorClass<AdmissionClosed>()("AdmissionClosed", {
  message: Schema.String,
}) {}

const makeHost = Effect.gen(function* () {
  const runtime = yield* DurableAgentRuntime;
  const config = yield* NodeDurableRuntimeConfig;

  // Startup gate (deployment §5, plan §host): configuration decoding and storage compatibility
  // already gated this Layer's dependencies; the last gate before admission opens is recovering
  // EVERY nonterminal Submission. Work needing a live Agent Binding is reported `deferred` and
  // stays a visible obligation for `runWorkers`.
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

  return NodeDurableHost.of({
    startupRecovery,
    admissionOpen: Ref.get(admission),
    submit,
    awaitSettlement: runtime.awaitSettlement,
    observe: runtime.observe,
    abort: runtime.abort,
    runWorkers,
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
    /** The recovery decisions executed (or deferred) by this host's startup reconciliation. */
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
    readonly awaitSettlement: (receipt: Receipt) => Effect.Effect<Settlement, DurableAwaitFailure>;
    readonly observe: (
      receipt: Receipt,
      options?: DurableObserveOptions,
    ) => Stream.Stream<
      CanonicalRecordEnvelope,
      ConversationStoreError | ConversationNotMaterialized
    >;
    readonly abort: (command: AbortCommand) => Effect.Effect<AbortIntent, DurableAbortFailure>;
    /**
     * Run `workerConcurrency` copies of the given worker effect (typically
     * `DurableAgentRuntime.runWorker(agent)`) until the caller's Scope interrupts them. The
     * bound is the validated finite configuration value; the host never forks daemon fibers.
     */
    readonly runWorkers: <A, E, R>(worker: Effect.Effect<A, E, R>) => Effect.Effect<void, E, R>;
  }
>()("@effect-agent/platform-node/NodeDurableHost") {
  /** Host gates over an already-assembled `NodeDurableRuntime` stack. */
  static readonly layer: Layer.Layer<
    NodeDurableHost,
    DurableWorkerFailure,
    DurableAgentRuntime | NodeDurableRuntimeConfig
  > = Layer.effect(NodeDurableHost)(makeHost);

  /** The complete DN host: `NodeDurableRuntime.layer(options)` plus the host lifecycle gates. */
  static layerStack(
    options: NodeDurableRuntimeOptions,
  ): Layer.Layer<
    NodeDurableHost | NodeDurableRuntimeServices,
    DurableWorkerFailure | NodeDurableRuntimeInitializationError
  > {
    return NodeDurableHost.layer.pipe(Layer.provideMerge(NodeDurableRuntime.layer(options)));
  }
}
