import { type ThreadId } from "@effect-agent/core/Identifiers";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  Receipt,
  type DurableAwaitFailure,
  type DurableSubmitAgent,
  type DurableSubmitFailure,
  type DurableSubmitOptions,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DeploymentId } from "@effect-agent/thread/Records";
import { SubmissionLedger, SubmissionLookupById } from "@effect-agent/thread/SubmissionLedger";
import {
  Cause,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  RcMap,
  Ref,
  Result,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import { Workflow, WorkflowEngine } from "effect/unstable/workflow";

import {
  WorkflowDispatchError,
  WorkflowDispatchFailpoint,
  WorkflowDispatchIntent,
  WorkflowDispatchScan,
  WorkflowDispatchStore,
  WorkflowRepairReport,
  WorkflowRepairTrigger,
  WorkflowSettlementReference,
  WorkflowSubmission,
} from "./WorkflowDispatch.ts";

const WorkflowHostConfig = Schema.Struct({
  deploymentId: DeploymentId,
  workflowName: Schema.NonEmptyString,
  executionConcurrency: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })),
  repairBatchSize: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
  dispatchTimeoutMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 300_000 })),
});

export class WorkflowHostConfigError extends Schema.TaggedError<WorkflowHostConfigError>()(
  "WorkflowHostConfigError",
  { message: Schema.String },
) {}

export class WorkflowAdmissionClosed extends Schema.TaggedError<WorkflowAdmissionClosed>()(
  "WorkflowAdmissionClosed",
  { message: Schema.String },
) {}

export interface WorkflowAgentHostOptions {
  readonly deploymentId: string;
  /** Stable versioned name prefix. The native name also includes deploymentId. */
  readonly workflowName?: string;
  /** Concurrent Attempts within this host Layer instance, not a fleet-wide limit. Default 1. */
  readonly executionConcurrency?: number;
  /** Maximum entries per scan per repair invocation. Default 32. */
  readonly repairBatchSize?: number;
  /** Bound each dispatch or scan, including a retrying native engine. Default 10000 ms. */
  readonly dispatchTimeoutMillis?: number;
}

export type WorkflowRepairFailure = WorkflowDispatchError | DurableAwaitFailure;

const nativeOperation = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchDefect((cause) =>
      Effect.fail(
        new WorkflowDispatchError({
          operation,
          message: "Workflow engine operation failed",
          cause,
        }),
      ),
    ),
  );

const makeHost = Effect.fn("WorkflowAgentHost.make")(function* (options: WorkflowAgentHostOptions) {
  const config = yield* Schema.decodeUnknownEffect(WorkflowHostConfig)({
    deploymentId: options.deploymentId,
    workflowName: options.workflowName ?? "effect-agent/Submission/v1",
    executionConcurrency: options.executionConcurrency ?? 1,
    repairBatchSize: options.repairBatchSize ?? 32,
    dispatchTimeoutMillis: options.dispatchTimeoutMillis ?? 10_000,
  }).pipe(Effect.mapError((error) => new WorkflowHostConfigError({ message: error.message })));

  const runtime = yield* DurableAgentRuntime;
  const runtimeConfig = yield* DurableRuntimeConfig;
  const ledger = yield* SubmissionLedger;
  const engine = yield* WorkflowEngine.WorkflowEngine;
  const dispatch = yield* WorkflowDispatchStore;
  const trigger = yield* WorkflowRepairTrigger;
  const failpoint = yield* WorkflowDispatchFailpoint;

  if (runtimeConfig.deploymentId !== config.deploymentId) {
    return yield* new WorkflowHostConfigError({
      message: "Workflow and durable runtime deploymentId must match",
    });
  }
  const permits = yield* Semaphore.make(config.executionConcurrency);
  const lanes = yield* RcMap.make({ lookup: (_threadId: ThreadId) => Semaphore.make(1) });
  const repairPermit = yield* Semaphore.make(1);
  const admission = yield* Ref.make(true);
  const nativeName = `${config.workflowName}/deployment/${config.deploymentId.length}:${config.deploymentId}`;

  const workflow = Workflow.make(nativeName, {
    payload: WorkflowSubmission,
    success: WorkflowSettlementReference,
    idempotencyKey: ({ deploymentId, receipt }) =>
      `v1/${deploymentId.length}:${deploymentId}/${receipt.submissionId}`,
  }).annotate(Workflow.SuspendOnFailure, true);

  const bounded = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
    nativeOperation(operation, effect).pipe(
      Effect.timeout(config.dispatchTimeoutMillis),
      Effect.catchTag("TimeoutError", () =>
        Effect.fail(
          WorkflowDispatchError.make({
            operation,
            message: "Workflow dispatch operation timed out; accepted work remains recoverable",
          }),
        ),
      ),
    );

  const validateReceipt = Effect.fn("WorkflowAgentHost.validateReceipt")(function* (
    receipt: Receipt,
  ) {
    const found = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: receipt.submissionId }),
    );

    if (
      Option.isNone(found) ||
      found.value.deploymentId !== config.deploymentId ||
      found.value.threadId !== receipt.threadId ||
      found.value.receiptId !== receipt.receiptId ||
      found.value.queueSequence !== receipt.queueSequence
    ) {
      return yield* WorkflowDispatchError.make({
        operation: "identity",
        message: "Workflow Receipt does not match its authoritative deployment and Submission",
      });
    }
  });

  // No Activities: rerun journal recovery directly on every native resume. The upstream
  // failure annotation suspends infrastructure failures and defects instead of settling them.
  yield* engine.register(
    workflow,
    Effect.fn("WorkflowAgentHost.execute")(function* (payload, executionId) {
      yield* Effect.annotateCurrentSpan({
        "workflow.execution.id": executionId,
        "agent.submission.id": payload.receipt.submissionId,
        "agent.thread.id": payload.receipt.threadId,
      });
      if (payload.deploymentId !== config.deploymentId) {
        return yield* Effect.die("Workflow payload belongs to another deployment");
      }
      if (executionId !== (yield* workflow.executionId(payload))) {
        return yield* Effect.die("Workflow execution identity does not match its Submission");
      }
      yield* validateReceipt(payload.receipt).pipe(Effect.orDie);
      const initial = yield* runtime.inspectSubmissionStatus(payload.receipt).pipe(Effect.orDie);

      if (initial._tag === "settled") {
        return new WorkflowSettlementReference({
          version: 1,
          submissionId: initial.settlement.submissionId,
          threadId: payload.receipt.threadId,
          settlementId: initial.settlement.settlementId,
        });
      }

      const status = yield* Effect.scoped(
        Effect.gen(function* () {
          // Serialize recovery and processing for one Thread even when a ledger allows
          // same-producer takeover. Followers wait without consuming a global permit.
          const lane = yield* RcMap.get(lanes, payload.receipt.threadId);

          return yield* lane.withPermit(
            permits.withPermit(
              Effect.scoped(
                Effect.gen(function* () {
                  yield* runtime.recoverSubmission(payload.receipt.submissionId);
                  const recovered = yield* runtime.inspectSubmissionStatus(payload.receipt);

                  if (recovered._tag === "settled") return recovered;
                  yield* runtime.processThreadHead(payload.receipt.threadId);

                  return yield* runtime.inspectSubmissionStatus(payload.receipt);
                }),
              ),
            ),
          );
        }),
      ).pipe(Effect.orDie);

      if (status._tag === "settled") {
        return new WorkflowSettlementReference({
          version: 1,
          submissionId: status.settlement.submissionId,
          threadId: payload.receipt.threadId,
          settlementId: status.settlement.settlementId,
        });
      }
      // Release the permit and Attempt Scope before native suspension, whose lifetime may
      // outlast this process. Completion is never inferred from an empty processing result.
      const instance = yield* WorkflowEngine.WorkflowInstance;

      return yield* Workflow.suspend(instance);
    }),
  );

  const intentFor = Effect.fn("WorkflowAgentHost.intentFor")(function* (receipt: Receipt) {
    const payload = new WorkflowSubmission({
      version: 1,
      deploymentId: config.deploymentId,
      receipt,
    });

    const executionId = yield* workflow.executionId(payload);

    return new WorkflowDispatchIntent({
      ...payload,
      workflowName: nativeName,
      executionId,
    });
  });

  const dispatchIntent = Effect.fn("WorkflowAgentHost.dispatch")(
    function* (intent: WorkflowDispatchIntent): Effect.fn.Return<boolean, WorkflowRepairFailure> {
      const expected = yield* intentFor(intent.receipt);

      if (
        intent.executionId !== expected.executionId ||
        intent.deploymentId !== expected.deploymentId ||
        intent.workflowName !== expected.workflowName
      ) {
        return yield* new WorkflowDispatchError({
          operation: "dispatch",
          message: "Dispatch identity does not match the configured Workflow",
        });
      }
      yield* validateReceipt(intent.receipt);
      yield* failpoint.hit("intent:before-persist", intent);
      yield* dispatch.put(intent);
      yield* failpoint.hit("intent:after-persist", intent);
      yield* failpoint.hit("launch:before", intent);
      yield* nativeOperation(
        "execute",
        engine.execute(workflow, {
          executionId: intent.executionId,
          payload: new WorkflowSubmission(intent),
          discard: true,
        }),
      );
      yield* failpoint.hit("launch:after", intent);
      // Resume before suspension may be a no-op. Keep the intent and retry on a later trigger.
      yield* nativeOperation("resume", engine.resume(workflow, intent.executionId));
      yield* failpoint.hit("completion:before-observe", intent);
      const result = yield* nativeOperation("poll", engine.poll(workflow, intent.executionId));

      yield* failpoint.hit("completion:after-observe", intent);
      if (Option.isNone(result) || result.value._tag !== "Complete") return false;
      if (Exit.isFailure(result.value.exit)) {
        return yield* new WorkflowDispatchError({
          operation: "completion",
          message: "Native Workflow completed without a Settlement reference; intent retained",
          cause: result.value.exit.cause,
        });
      }
      const status = yield* runtime.inspectSubmissionStatus(intent.receipt);
      const reference = result.value.exit.value;

      if (
        status._tag !== "settled" ||
        reference.version !== 1 ||
        reference.submissionId !== intent.receipt.submissionId ||
        reference.threadId !== intent.receipt.threadId ||
        reference.settlementId !== status.settlement.settlementId
      ) {
        return yield* new WorkflowDispatchError({
          operation: "completion",
          message: "Native completion disagrees with canonical Settlement; intent retained",
        });
      }
      yield* failpoint.hit("cleanup:before", intent);
      yield* dispatch.remove(intent);
      yield* failpoint.hit("cleanup:after", intent);

      return true;
    },
    (effect) => bounded("dispatch", effect),
  );

  let submissionOffset = 0;
  let intentCursor: string | undefined;

  const repair = repairPermit.withPermit(
    Effect.gen(function* () {
      // Respect adapter ordering, which may differ from JavaScript string comparison.
      // Deletions may defer a row until the next wrap; restarts repeat idempotent work.
      const discovery = yield* bounded(
        "scanSubmissions",
        ledger.scanNonterminal.pipe(
          Stream.filter((row) => row.deploymentId === config.deploymentId),
          Stream.drop(submissionOffset),
          Stream.take(config.repairBatchSize),
          Stream.runCollect,
        ),
      ).pipe(Effect.result);

      const submissions = Result.isSuccess(discovery) ? discovery.success : [];

      if (Result.isSuccess(discovery)) {
        submissionOffset =
          submissions.length < config.repairBatchSize ? 0 : submissionOffset + submissions.length;
      }

      const discovered = yield* Effect.forEach(submissions, (row) =>
        Effect.flatMap(intentFor(new Receipt(row)), dispatchIntent).pipe(Effect.result),
      );

      const outstanding = yield* bounded(
        "scanIntents",
        dispatch.scan(
          new WorkflowDispatchScan({
            deploymentId: config.deploymentId,
            workflowName: nativeName,
            ...(intentCursor === undefined ? {} : { after: intentCursor }),
            limit: config.repairBatchSize,
          }),
        ),
      ).pipe(Effect.result);

      const intents = Result.isSuccess(outstanding) ? outstanding.success : [];

      if (Result.isSuccess(outstanding))
        intentCursor =
          intents.length < config.repairBatchSize
            ? undefined
            : intents[intents.length - 1]?.executionId;

      const inspected = yield* Effect.forEach(intents, (intent) =>
        dispatchIntent(intent).pipe(Effect.result),
      );

      const results = [...discovered, ...inspected];

      if (Result.isFailure(discovery)) return yield* discovery.failure;
      if (Result.isFailure(outstanding)) return yield* outstanding.failure;
      const failure = results.find(Result.isFailure);

      if (failure !== undefined) return yield* Effect.fail(failure.failure);

      return new WorkflowRepairReport({
        discovered: submissions.length,
        inspected: intents.length,
        completed: results.filter((result) => Result.isSuccess(result) && result.success).length,
      });
    }),
  );

  yield* trigger.register(
    repair.pipe(
      Effect.asVoid,
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.interrupt
          : Effect.logError("Workflow dispatch repair failed; durable obligations remain", cause),
      ),
    ),
  );
  yield* Effect.addFinalizer(() => Ref.set(admission, false));

  const submit = Effect.fn("WorkflowAgentHost.submit")(function* <InputSchema extends Schema.Top>(
    agent: DurableSubmitAgent<InputSchema>,
    input: InputSchema["Type"],
    options: DurableSubmitOptions,
  ): Effect.fn.Return<
    Receipt,
    DurableSubmitFailure | WorkflowRepairFailure | WorkflowAdmissionClosed,
    InputSchema["EncodingServices"]
  > {
    if (!(yield* Ref.get(admission))) {
      return yield* new WorkflowAdmissionClosed({ message: "The Workflow host is shutting down" });
    }
    const receipt = yield* runtime.submit(agent, input, options);

    yield* dispatchIntent(yield* intentFor(receipt));

    return receipt;
  });

  return WorkflowAgentHost.of({
    submit,
    awaitSettlement: runtime.awaitSettlement,
    observe: runtime.observe,
    abort: runtime.abort,
    resolveApproval: runtime.resolveApproval,
    resolveUnknown: runtime.resolveUnknown,
    submissionStatus: runtime.submissionStatus,
    repair,
    executionId: Effect.fn("WorkflowAgentHost.executionId")((receipt: Receipt) =>
      intentFor(receipt).pipe(Effect.map((intent) => intent.executionId)),
    ),
  });
});

/**
 * Optional engine-independent host. Supply the upstream WorkflowEngine, the existing
 * durable runtime and ledger, durable dispatch storage, and a host-owned repair trigger.
 * Do not also start the ordinary Node worker loop. Waiter interruption only detaches;
 * abort and resolutions retain the runtime's authorization and durable intent protocol.
 */
export class WorkflowAgentHost extends Context.Service<
  WorkflowAgentHost,
  {
    readonly submit: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: DurableSubmitOptions,
    ) => Effect.Effect<
      Receipt,
      DurableSubmitFailure | WorkflowRepairFailure | WorkflowAdmissionClosed,
      InputSchema["EncodingServices"]
    >;
    readonly awaitSettlement: DurableAgentRuntime["Service"]["awaitSettlement"];
    readonly observe: DurableAgentRuntime["Service"]["observe"];
    readonly abort: DurableAgentRuntime["Service"]["abort"];
    readonly resolveApproval: DurableAgentRuntime["Service"]["resolveApproval"];
    readonly resolveUnknown: DurableAgentRuntime["Service"]["resolveUnknown"];
    readonly submissionStatus: DurableAgentRuntime["Service"]["submissionStatus"];
    readonly repair: Effect.Effect<WorkflowRepairReport, WorkflowRepairFailure>;
    readonly executionId: (receipt: Receipt) => Effect.Effect<string>;
  }
>()("@effect-agent/workflow/WorkflowAgentHost") {
  /** Drive the injected runtime, whose Layer owns executable registrations and their services. */
  static layer(options: WorkflowAgentHostOptions) {
    return Layer.effect(WorkflowAgentHost)(makeHost(options));
  }
}
