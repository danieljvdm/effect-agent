import type { AnyDefinition } from "@effect-agent/core/Agent";
import { RunId, SettlementId, ThreadId } from "@effect-agent/core/Identifiers";
import { IdempotencyKey, JoinedToHost, Receipt } from "@effect-agent/core/Receipt";
import {
  BackgroundSpawnTool,
  narrowSubagentGrant,
  SubagentBudgetReservation,
  SubagentGrant,
} from "@effect-agent/core/SubagentContract";
import {
  WorkerContext,
  type WorkerBudgetScope,
  WorkerError,
  WorkerOperationTool,
  WorkerPage,
  WorkerRef,
  WorkerStarted,
  WorkerSummary,
} from "@effect-agent/core/Worker";
import {
  SubagentHost,
  BackgroundReporting,
  WorkerReportPreparationFailure,
  type WorkerReporting,
  type WorkerObservation as HostObservation,
} from "@effect-agent/engine/SubagentHost";
import type { Scope } from "effect";
import { Context, Crypto, Effect, Encoding, Layer, Option, Schema, Stream } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import type { SubagentDefineOptions, SubagentPrepareContext } from "../Subagent.ts";
import {
  SubagentExecutionFailure,
  SubagentPrestartDenied,
  SubagentProjectionFailure,
} from "./subagent-contract.ts";
import { resolveSubagentPolicy, resolveToolCallAllowance } from "./subagent-policy.ts";
import { automaticReporting } from "./subagent-reporting.ts";
import { utf8ByteLength, utf8Bytes } from "./utf8.ts";

export type Declaration<
  Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
> = Pick<
  SubagentDefineOptions<
    Input,
    Output,
    unknown,
    Record<string, Tool.Any>,
    Parameters,
    Success,
    Failure,
    Prepare,
    Project
  >,
  | "parameters"
  | "success"
  | "failure"
  | "prepareInput"
  | "projectResult"
  | "policy"
  | "toolCallAllowance"
  | "needsApproval"
> & {
  readonly name: Name;
  readonly delegationId: WorkerRef["delegationId"];
  readonly target: AnyDefinition & { readonly input: Input; readonly output: Output };
  readonly grant: SubagentGrant;
};

type WorkerDeclaration<Name extends string> = {
  readonly name: Name;
  readonly delegationId: WorkerRef["delegationId"];
  readonly target: Pick<AnyDefinition, "id">;
};

/** Portable identity checked against this declaration; possession grants no authority. */
export const Worker = <const Name extends string>(declaration: WorkerDeclaration<Name>) =>
  WorkerRef.check(
    Schema.makeFilter(
      (worker) =>
        worker.delegationId === declaration.delegationId &&
        worker.targetAgentId === declaration.target.id,
    ),
  ).pipe(Schema.brand(`@effect-agent/Worker/${declaration.name}`));

export type Worker<Name extends string> = ReturnType<typeof Worker<Name>>["Type"];

/** Observes one exact Receipt. Terminal failure data never includes raw child output. */
export const WorkerObservation = <Success extends Schema.Top>(success: Success) =>
  Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Pending"), receipt: Receipt }),
    Schema.Struct({
      _tag: Schema.Literal("Settled"),
      receipt: Receipt,
      runId: RunId,
      settlementId: SettlementId,
      outcome: Schema.Literal("completed"),
      result: success,
    }),
    Schema.Struct({
      _tag: Schema.Literal("Settled"),
      receipt: Receipt,
      runId: Schema.optionalKey(RunId),
      settlementId: SettlementId,
      outcome: Schema.Literals(["failed", "aborted"]),
      failure: SubagentExecutionFailure,
    }),
  ]);

export type WorkerObservation<Success extends Schema.Top> = ReturnType<
  typeof WorkerObservation<Success>
>["Type"];

const host: Effect.Effect<SubagentHost["Service"], WorkerError, SubagentHost> =
  Effect.serviceOption(SubagentHost).pipe(
    Effect.flatMap((service) =>
      Option.match(service, {
        onNone: () => WorkerError.make({ operation: "context", reason: "unavailable" }),
        onSome: Effect.succeed,
      }),
    ),
  );

const receiptEqual = (left: Receipt, right: Receipt): boolean =>
  left.threadId === right.threadId &&
  left.submissionId === right.submissionId &&
  left.receiptId === right.receiptId &&
  left.queueSequence === right.queueSequence;

const operations = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
) => {
  const workerSchema = Worker(declaration);

  const projectionFailure = (stage: "input" | "result") =>
    SubagentProjectionFailure.make({
      delegationId: declaration.delegationId,
      stage,
      message:
        stage === "input"
          ? "Worker input did not satisfy the declaration Schema"
          : "Worker result did not satisfy the declaration Schema",
    });

  const validateWorker = (worker: unknown, operation: WorkerError["operation"]) =>
    Schema.decodeUnknownEffect(workerSchema)(worker).pipe(
      Effect.mapError(() => WorkerError.make({ operation, reason: "worker-mismatch" })),
    );

  const validateReceipt = (
    worker: WorkerRef,
    receipt: Receipt,
    operation: WorkerError["operation"],
  ) =>
    Schema.decodeEffect(
      Receipt.check(Schema.makeFilter((value) => value.threadId === worker.threadId)),
    )(receipt).pipe(
      Effect.mapError(() => WorkerError.make({ operation, reason: "receipt-mismatch" })),
    );

  const context: Effect.Effect<WorkerContext, WorkerError, SubagentHost> = host.pipe(
    Effect.flatMap((service) => service.context),
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.toType(WorkerContext))),
    Effect.mapError((error) =>
      Schema.is(WorkerError)(error)
        ? error
        : WorkerError.make({ operation: "context", reason: "corrupt" }),
    ),
  );

  const prepare = Effect.fn("Subagent.prepareWorkerInput")(function* (
    parameters: Parameters["Type"],
    caller: WorkerContext,
  ) {
    const encodedParameters = yield* Schema.encodeEffect(declaration.parameters)(parameters).pipe(
      Effect.mapError(() => projectionFailure("input")),
    );

    const source = caller.source;

    const preparation: SubagentPrepareContext =
      source._tag === "tool"
        ? {
            source: "tool",
            delegationId: declaration.delegationId,
            toolCallId: source.toolCallId,
            parent: { agentId: source.agentId, threadId: source.threadId, runId: source.runId },
          }
        : {
            source: "programmatic",
            delegationId: declaration.delegationId,
            parent: { agentId: source.agentId, threadId: source.threadId },
          };

    const input = yield* declaration.prepareInput(parameters, preparation);

    const encodedInput = yield* Schema.encodeEffect(declaration.target.input)(input).pipe(
      Effect.mapError(() => projectionFailure("input")),
    );

    return { encodedParameters, encodedInput };
  });

  const validateKey = (key: IdempotencyKey, operation: "start" | "followUp") =>
    Schema.decodeEffect(IdempotencyKey)(key).pipe(
      Effect.mapError(() => WorkerError.make({ operation, reason: "idempotency-conflict" })),
    );

  const start = Effect.fn("Subagent.start")(function* (
    parameters: Parameters["Type"],
    options: { readonly idempotencyKey: IdempotencyKey; readonly budgetScope?: WorkerBudgetScope },
  ) {
    const service = yield* host;
    const caller = yield* context;
    const key = yield* validateKey(options.idempotencyKey, "start");

    const grant = narrowSubagentGrant(declaration.grant, caller.grant);

    if (caller.depth + 1 > grant.maxDepth) {
      return yield* SubagentPrestartDenied.make({
        delegationId: declaration.delegationId,
        targetAgentId: declaration.target.id,
        reason: "nested-delegation",
        message: "Worker start exceeds the inherited depth ceiling",
      });
    }
    if (!(caller.grant?.childLifetimes ?? ["attached", "background"]).includes("background")) {
      return yield* SubagentPrestartDenied.make({
        delegationId: declaration.delegationId,
        targetAgentId: declaration.target.id,
        reason: "grant-violation",
        message: "The inherited grant does not permit background children",
      });
    }

    const prepared = yield* prepare(parameters, caller);

    const effectiveTarget = yield* service.resolveTargetPolicy({
      target: declaration.target,
      encodedInput: prepared.encodedInput,
    });

    const resolvedTarget = Option.getOrUndefined(effectiveTarget);

    const resolved = resolveSubagentPolicy(
      declaration,
      options.budgetScope === "worker-run"
        ? (resolvedTarget ?? declaration.target.policy)
        : caller.policy,
      undefined,
      options.budgetScope === "worker-run" ? "root-attached" : "conserved",
      resolvedTarget,
    );

    const encodedGrant = yield* Schema.encodeEffect(SubagentGrant)(grant).pipe(
      Effect.mapError(() => projectionFailure("input")),
    );

    const started = yield* service.start({
      ...prepared,
      delegationId: declaration.delegationId,
      target: declaration.target,
      idempotencyKey: key,
      ...(options.budgetScope === undefined ? {} : { budgetScope: options.budgetScope }),
      encodedGrant,
      policy: resolved.childPolicy,
      budget: SubagentBudgetReservation.make({
        caps: resolved.caps,
        allocation: resolved.allocation,
        ...(resolved.policy.descendantInvocations === undefined
          ? {}
          : { descendantInvocations: resolved.policy.descendantInvocations }),
      }),
      toolCallAllowance: resolveToolCallAllowance(
        declaration.toolCallAllowance,
        parameters,
        resolved.policy,
        resolved.childPolicy,
      ),
    });

    const validated = yield* Schema.decodeEffect(WorkerStarted)(started).pipe(
      Effect.mapError(() => WorkerError.make({ operation: "start", reason: "corrupt" })),
    );

    return { worker: yield* validateWorker(validated.worker, "start"), receipt: validated.receipt };
  });

  const followUp = Effect.fn("Subagent.followUp")(function* (
    worker: Worker<Name>,
    parameters: Parameters["Type"],
    options: { readonly idempotencyKey: IdempotencyKey },
  ) {
    const service = yield* host;
    const validated = yield* validateWorker(worker, "followUp");
    const caller = yield* context;
    const key = yield* validateKey(options.idempotencyKey, "followUp");
    const prepared = yield* prepare(parameters, caller);

    return yield* validateReceipt(
      validated,
      yield* service.followUp({
        ...prepared,
        worker: validated,
        target: declaration.target,
        idempotencyKey: key,
      }),
      "followUp",
    );
  });

  const observe = Effect.fn("Subagent.observeWorker")(function* (
    operation: "inspect" | "await",
    worker: Worker<Name>,
    receipt: Receipt,
  ): Effect.fn.Return<
    WorkerObservation<Success>,
    WorkerError | SubagentProjectionFailure | Failure["Type"],
    | SubagentHost
    | Parameters["DecodingServices"]
    | Output["DecodingServices"]
    | Success["EncodingServices"]
    | Project
  > {
    const service = yield* host;
    const validated = yield* validateWorker(worker, operation);
    const requested = yield* validateReceipt(validated, receipt, operation);

    const observed: HostObservation = yield* service[operation]({
      worker: validated,
      target: declaration.target,
      receipt: requested,
    });

    const returned = yield* validateReceipt(validated, observed.receipt, operation);

    if (!receiptEqual(returned, requested))
      return yield* WorkerError.make({ operation, reason: "receipt-mismatch" });
    if (observed._tag === "Pending") {
      if (operation === "await") return yield* WorkerError.make({ operation, reason: "corrupt" });

      return { _tag: "Pending", receipt: returned };
    }

    const base = {
      _tag: "Settled" as const,
      receipt: returned,
      ...(observed.runId === undefined
        ? {}
        : {
            runId: yield* Schema.decodeEffect(RunId)(observed.runId).pipe(
              Effect.mapError(() => WorkerError.make({ operation, reason: "corrupt" })),
            ),
          }),
      settlementId: yield* Schema.decodeEffect(SettlementId)(observed.settlementId).pipe(
        Effect.mapError(() => WorkerError.make({ operation, reason: "corrupt" })),
      ),
    };

    if (observed.outcome === "failed" || observed.outcome === "aborted") {
      return {
        ...base,
        outcome: observed.outcome,
        failure: SubagentExecutionFailure.make({
          delegationId: declaration.delegationId,
          targetAgentId: declaration.target.id,
          classification: observed.outcome === "failed" ? "child-failed" : "child-aborted",
          childThreadId: worker.threadId,
          childSubmissionId: returned.submissionId,
          ...(base.runId === undefined ? {} : { childRunId: base.runId }),
          errorTag: observed.outcome === "failed" ? "WorkerFailed" : "WorkerAborted",
          message:
            observed.outcome === "failed" ? "Worker input failed" : "Worker input was aborted",
        }),
      };
    }
    if (
      observed.outcome !== "completed" ||
      base.runId === undefined ||
      typeof observed.budgetExhausted !== "boolean"
    ) {
      return yield* WorkerError.make({ operation, reason: "corrupt" });
    }

    const parameters = yield* Schema.decodeUnknownEffect(declaration.parameters)(
      observed.encodedParameters,
    ).pipe(Effect.mapError(() => projectionFailure("result")));

    const output = yield* Schema.decodeUnknownEffect(declaration.target.output)(
      observed.encodedResult,
    ).pipe(Effect.mapError(() => projectionFailure("result")));

    const result = yield* declaration.projectResult(
      output,
      { budgetExhausted: observed.budgetExhausted },
      parameters,
    );

    const encoded = yield* Schema.encodeEffect(declaration.success)(result).pipe(
      Effect.mapError(() => projectionFailure("result")),
    );

    const json = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
      Effect.mapError(() => projectionFailure("result")),
    );

    const caller = yield* context;

    if (
      utf8ByteLength(JSON.stringify(json)) >
      (declaration.policy?.maxResultBytes ?? caller.policy.toolResultBounds.maxBytes)
    ) {
      return yield* projectionFailure("result");
    }

    return yield* Schema.decodeUnknownEffect(Schema.toType(WorkerObservation(declaration.success)))(
      { ...base, outcome: "completed", result },
    ).pipe(Effect.mapError(() => projectionFailure("result")));
  });

  const list = Effect.fn("Subagent.list")(function* (
    options: { readonly limit?: number; readonly after?: ThreadId } = {},
  ) {
    const service = yield* host;

    const limit = yield* Schema.decodeEffect(
      Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
    )(options.limit ?? 20).pipe(
      Effect.mapError(() => WorkerError.make({ operation: "list", reason: "capacity" })),
    );

    const page = yield* service.list({
      delegationId: declaration.delegationId,
      target: declaration.target,
      limit,
      ...(options.after === undefined ? {} : { after: options.after }),
    });

    const validated = yield* Schema.decodeEffect(WorkerPage)(page).pipe(
      Effect.mapError(() => WorkerError.make({ operation: "list", reason: "corrupt" })),
    );

    if (validated.items.length > limit)
      return yield* WorkerError.make({ operation: "list", reason: "corrupt" });
    const items = [];

    for (const item of validated.items) {
      const worker = yield* validateWorker(item.worker, "list");

      const latestReceipt =
        item.latestReceipt === null
          ? null
          : yield* validateReceipt(worker, item.latestReceipt, "list");

      items.push({ ...item, worker, latestReceipt });
    }

    return { items, next: validated.next };
  });

  const cancel = Effect.fn("Subagent.cancel")(function* (worker: Worker<Name>, receipt: Receipt) {
    const service = yield* host;
    const validated = yield* validateWorker(worker, "cancel");
    const requested = yield* validateReceipt(validated, receipt, "cancel");

    yield* service.cancel({ worker: validated, target: declaration.target, receipt: requested });
  });

  return { start, followUp, observe, list, cancel };
};

/** Start a host-managed worker. Retrying the same explicit key reuses the accepted input. */
export const start = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  parameters: Parameters["Type"],
  options: { readonly idempotencyKey: IdempotencyKey; readonly budgetScope?: WorkerBudgetScope },
) => operations(declaration).start(parameters, options);

/** Admit typed follow-up parameters to the same worker Thread. */
export const followUp = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  worker: Worker<Name>,
  parameters: Parameters["Type"],
  options: { readonly idempotencyKey: IdempotencyKey },
) => operations(declaration).followUp(worker, parameters, options);

/** Inspect a continuing worker with the same summary contract returned by list. */
export const summary = <const Name extends string>(
  declaration: WorkerDeclaration<Name> & { readonly target: AnyDefinition },
  worker: Worker<Name>,
) =>
  Effect.gen(function* () {
    const service = yield* host;

    const validated = yield* Schema.decodeEffect(Worker(declaration))(worker).pipe(
      Effect.mapError(() => WorkerError.make({ operation: "inspect", reason: "worker-mismatch" })),
    );

    const result = yield* service.summary({ worker: validated, target: declaration.target });

    const value = yield* Schema.decodeEffect(WorkerSummary)(result).pipe(
      Effect.mapError(() => WorkerError.make({ operation: "inspect", reason: "corrupt" })),
    );

    if (
      value.worker.threadId !== validated.threadId ||
      value.worker.delegationId !== validated.delegationId ||
      value.worker.targetAgentId !== validated.targetAgentId ||
      (value.latestReceipt !== null && value.latestReceipt.threadId !== validated.threadId)
    )
      return yield* WorkerError.make({ operation: "inspect", reason: "worker-mismatch" });

    return { ...value, worker: validated };
  });

/** Inspect the worker, or pass a Receipt to observe that exact accepted input. */
export function inspect<
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  worker: Worker<Name>,
): ReturnType<typeof summary<Name>>;

export function inspect<
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  worker: Worker<Name>,
  receipt: Receipt,
): ReturnType<
  ReturnType<
    typeof operations<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>
  >["observe"]
>;

export function inspect<
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  worker: Worker<Name>,
  receipt?: Receipt,
) {
  return receipt === undefined
    ? summary(declaration, worker)
    : operations(declaration).observe("inspect", worker, receipt);
}

/** Read an authorized finite history snapshot; pass the last emitted sequence to resume. */
export const observe = <const Name extends string>(
  declaration: WorkerDeclaration<Name> & { readonly target: AnyDefinition },
  worker: Worker<Name>,
  options: { readonly after?: number } = {},
) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const service = yield* host;

      const validated = yield* Schema.decodeEffect(Worker(declaration))(worker).pipe(
        Effect.mapError(() =>
          WorkerError.make({ operation: "observe", reason: "worker-mismatch" }),
        ),
      );

      const after =
        options.after === undefined
          ? undefined
          : yield* Schema.decodeEffect(Schema.Natural)(options.after).pipe(
              Effect.mapError(() => WorkerError.make({ operation: "observe", reason: "corrupt" })),
            );

      return service.observe({
        worker: validated,
        target: declaration.target,
        ...(after === undefined ? {} : { after }),
      });
    }),
  );

/** Wait for the exact Receipt. Interrupting this Effect never cancels accepted work. */
export const awaitWorker = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  worker: Worker<Name>,
  receipt: Receipt,
) => operations(declaration).observe("await", worker, receipt);

/** List only workers visible through the caller-bound host facet. */
export const list = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  options: { readonly limit?: number; readonly after?: ThreadId } = {},
) => operations(declaration).list(options);

/** Request cancellation of exactly one Receipt; JoinedToHost remains a typed conflict. */
export const cancel = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  worker: Worker<Name>,
  receipt: Receipt,
) => operations(declaration).cancel(worker, receipt);

/** Opt in to each model-facing operation. Waiting remains programmatic only. */
export interface BackgroundOptions {
  /** Deliver a standard completion, or use an optional application input mapper. */
  readonly reportToParent?: true | WorkerReporting<unknown, unknown>;
  readonly start?: true;
  readonly followUp?: true;
  readonly inspect?: true;
  readonly summary?: true;
  readonly list?: true;
  readonly cancel?: true;
  /** Author-owned funding request for starts; every native admission separately authorizes it. */
  readonly budgetScope?: WorkerBudgetScope;
}

type Operation = Exclude<keyof BackgroundOptions, "budgetScope" | "reportToParent">;
type Suffix = {
  start: "start";
  followUp: "follow_up";
  inspect: "inspect";
  summary: "summary";
  list: "list";
  cancel: "cancel";
};

const ReceiptParameters = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({ worker: Worker(declaration), receipt: Receipt });

const WorkerParameters = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({ worker: Worker(declaration) });

const Summary = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({
    worker: Worker(declaration),
    latestReceipt: Schema.NullOr(Receipt),
    state: Schema.Literals(["starting", "active", "idle"]),
  });

const FollowUpParameters = <Name extends string, Parameters extends Schema.Top>(
  declaration: WorkerDeclaration<Name>,
  parameters: Parameters,
) => Schema.Struct({ worker: Worker(declaration), parameters });

const ListParameters = Schema.Struct({
  limit: Schema.optionalKey(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
  ),
  after: Schema.optionalKey(ThreadId),
});

const Started = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({ worker: Worker(declaration), receipt: Receipt }).check(
    Schema.makeFilter((value) => value.worker.threadId === value.receipt.threadId),
  );

const Page = <Name extends string>(declaration: WorkerDeclaration<Name>) =>
  Schema.Struct({
    items: Schema.Array(
      Schema.Struct({
        worker: Worker(declaration),
        latestReceipt: Schema.NullOr(Receipt),
        state: Schema.Literals(["starting", "active", "idle"]),
      }),
    ).check(Schema.isMaxLength(100)),
    next: Schema.NullOr(ThreadId),
  });

const preparationFailure = <Failure extends Schema.Top>(failure: Failure) =>
  Schema.Union([failure, SubagentProjectionFailure, WorkerError]);

const startFailure = <Failure extends Schema.Top>(failure: Failure) =>
  Schema.Union([failure, SubagentProjectionFailure, SubagentPrestartDenied, WorkerError]);

const cancelFailure = Schema.Union([WorkerError, JoinedToHost]);

type ToolSchemas<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
> = {
  start: {
    parameters: Parameters;
    success: ReturnType<typeof Started<Name>>;
    failure: ReturnType<typeof startFailure<Failure>>;
  };
  followUp: {
    parameters: ReturnType<typeof FollowUpParameters<Name, Parameters>>;
    success: typeof Receipt;
    failure: ReturnType<typeof preparationFailure<Failure>>;
  };
  inspect: {
    parameters: ReturnType<typeof ReceiptParameters<Name>>;
    success: ReturnType<typeof WorkerObservation<Success>>;
    failure: ReturnType<typeof preparationFailure<Failure>>;
  };
  summary: {
    parameters: ReturnType<typeof WorkerParameters<Name>>;
    success: ReturnType<typeof Summary<Name>>;
    failure: typeof WorkerError;
  };
  list: {
    parameters: typeof ListParameters;
    success: ReturnType<typeof Page<Name>>;
    failure: typeof WorkerError;
  };
  cancel: {
    parameters: ReturnType<typeof ReceiptParameters<Name>>;
    success: typeof Schema.Void;
    failure: typeof cancelFailure;
  };
};

type BackgroundToolMap<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
> = {
  start: Tool.Tool<
    `${Name}_start`,
    ToolSchemas<Name, Parameters, Success, Failure>["start"] & { readonly failureMode: "error" },
    SubagentHost | Crypto.Crypto
  >;
  followUp: Tool.Tool<
    `${Name}_follow_up`,
    ToolSchemas<Name, Parameters, Success, Failure>["followUp"] & { readonly failureMode: "error" },
    SubagentHost | Crypto.Crypto
  >;
  inspect: Tool.Tool<
    `${Name}_inspect`,
    ToolSchemas<Name, Parameters, Success, Failure>["inspect"] & { readonly failureMode: "error" },
    SubagentHost
  >;
  summary: Tool.Tool<
    `${Name}_summary`,
    ToolSchemas<Name, Parameters, Success, Failure>["summary"] & { readonly failureMode: "error" },
    SubagentHost
  >;
  list: Tool.Tool<
    `${Name}_list`,
    ToolSchemas<Name, Parameters, Success, Failure>["list"] & { readonly failureMode: "error" },
    SubagentHost
  >;
  cancel: Tool.Tool<
    `${Name}_cancel`,
    ToolSchemas<Name, Parameters, Success, Failure>["cancel"] & { readonly failureMode: "error" },
    SubagentHost
  >;
};

/**
 * Native Tool names and schemas expose statically guaranteed selections. The handler Layer also
 * requires services for operations whose conditional flags may enable them at runtime.
 */
export type BackgroundTools<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Selected extends BackgroundOptions,
> = {
  readonly [
    Op in Operation as Selected[Op] extends true ? `${Name}_${Suffix[Op]}` : never
  ]: BackgroundToolMap<Name, Parameters, Success, Failure>[Op];
};

const modelKey = Effect.fn("Subagent.workerToolKey")(function* (operation: "start" | "followUp") {
  const service = yield* host;
  const caller = yield* service.context;

  if (caller.source._tag !== "tool")
    return yield* WorkerError.make({ operation, reason: "denied" });
  const source = caller.source;
  const crypto = yield* Crypto.Crypto;

  const bytes = utf8Bytes(
    JSON.stringify([
      "worker-tool-v1",
      operation,
      source.agentId,
      source.threadId,
      source.runId,
      source.toolCallId,
    ]),
  );

  const digest = yield* crypto
    .digest("SHA-256", bytes)
    .pipe(Effect.mapError(() => WorkerError.make({ operation, reason: "unavailable" })));

  return yield* Schema.decodeEffect(IdempotencyKey)(`worker:${Encoding.encodeHex(digest)}`).pipe(
    Effect.mapError(() => WorkerError.make({ operation, reason: "unavailable" })),
  );
});

// Construction-local service keys distinguish projections for co-registered parent versions.
// These keys are never persisted; worker/run identity and registration digests remain canonical.
let reportingServiceId = 0;

/**
 * Derive optional native AI Tools and their handler Layer from one immutable declaration.
 * Projection services are captured by the Layer; the host and stable-key Crypto service remain
 * invocation dependencies. The attached `.tool` and its suspension behavior are unchanged.
 */
export const background = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
  const Selected extends BackgroundOptions,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  selected: Selected,
) => {
  const report =
    selected.reportToParent === true ? automaticReporting(declaration) : selected.reportToParent;

  if (
    report !== undefined &&
    (report.delegationId !== declaration.delegationId || report.target !== declaration.target)
  )
    throw new TypeError("Background reporting must use the same subagent declaration and target");

  const reportService = Context.Service<WorkerReporting<WorkerReportPreparationFailure>>(
    `@effect-agent/capabilities/BackgroundReport/${declaration.name}/${reportingServiceId++}`,
  );

  const descriptor: WorkerReporting<WorkerReportPreparationFailure> | undefined =
    report === undefined
      ? undefined
      : {
          ...report,
          prepare: (value) =>
            Effect.flatMap(Effect.serviceOption(reportService), (service) =>
              Option.isSome(service)
                ? service.value.prepare(value)
                : WorkerReportPreparationFailure.make({ stage: "preparation" }),
            ),
        };

  const ops = operations(declaration);
  const failure = preparationFailure(declaration.failure);
  const receiptParameters = ReceiptParameters(declaration);

  const startTool = Tool.make(`${declaration.name}_start` as const, {
    description: `Start ${declaration.name} in the background and return its worker identity and Receipt.`,
    parameters: declaration.parameters,
    success: Started(declaration),
    failure: startFailure(declaration.failure),
  })
    .annotate(WorkerOperationTool, true)
    .annotate(BackgroundSpawnTool, true)
    .addDependency(SubagentHost)
    .addDependency(Crypto.Crypto);

  const followUpTool = Tool.make(`${declaration.name}_follow_up` as const, {
    description: `Send typed follow-up input to an existing ${declaration.name} worker.`,
    parameters: FollowUpParameters(declaration, declaration.parameters),
    success: Receipt,
    failure,
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost)
    .addDependency(Crypto.Crypto);

  const inspectTool = Tool.make(`${declaration.name}_inspect` as const, {
    description: `Inspect one exact ${declaration.name} Receipt without waiting.`,
    parameters: receiptParameters,
    success: WorkerObservation(declaration.success),
    failure,
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost);

  const summaryTool = Tool.make(`${declaration.name}_summary` as const, {
    description: `Inspect ${declaration.name}'s continuing worker state and latest accepted input.`,
    parameters: WorkerParameters(declaration),
    success: Summary(declaration),
    failure: WorkerError,
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost);

  const listTool = Tool.make(`${declaration.name}_list` as const, {
    description: `List visible ${declaration.name} workers.`,
    parameters: ListParameters,
    success: Page(declaration),
    failure: WorkerError,
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost);

  const cancelTool = Tool.make(`${declaration.name}_cancel` as const, {
    description: `Request cancellation of one exact ${declaration.name} Receipt.`,
    parameters: receiptParameters,
    success: Schema.Void,
    failure: cancelFailure,
  })
    .annotate(WorkerOperationTool, true)
    .addDependency(SubagentHost);

  const available = {
    start: startTool,
    followUp: followUpTool,
    inspect: inspectTool,
    summary: summaryTool,
    list: listTool,
    cancel: cancelTool,
  };

  const chosen = Object.entries(available)
    .filter(([operation]) => selected[operation as Operation] === true)
    .map(([, tool]) => tool.annotate(BackgroundReporting, descriptor));

  type Tools = BackgroundTools<Name, Parameters, Success, Failure, Selected>;
  // The mapped keys depend on literal selection flags; the runtime filter applies exactly those flags.
  const toolkit = Toolkit.make(...chosen) as unknown as Toolkit.Toolkit<Tools>;

  type PrepareServices = Prepare | Parameters["EncodingServices"] | Input["EncodingServices"];
  type ProjectServices =
    | Project
    | Parameters["DecodingServices"]
    | Output["DecodingServices"]
    | Success["EncodingServices"];
  type ReportServices<Report> = Report extends true
    ? Exclude<ProjectServices, Scope.Scope>
    : Report extends WorkerReporting<infer _E, infer R>
      ? Exclude<R, Scope.Scope>
      : never;
  type Services = PrepareServices | ProjectServices;
  type SelectionServices<Flag, Requirements> = Flag extends true ? Requirements : never;

  const build = Effect.gen(function* () {
    const captured = yield* Effect.context<Services>();

    const handlers = {
      [startTool.name]: (parameters: Parameters["Type"]) =>
        Effect.gen(function* () {
          const service = yield* host;
          const idempotencyKey = yield* modelKey("start");

          return yield* ops
            .start(parameters, {
              idempotencyKey,
              ...(selected.budgetScope === undefined ? {} : { budgetScope: selected.budgetScope }),
            })
            .pipe(Effect.provideService(SubagentHost, service), Effect.provide(captured));
        }),
      [followUpTool.name]: (parameters: {
        readonly worker: Worker<Name>;
        readonly parameters: Parameters["Type"];
      }) =>
        Effect.gen(function* () {
          const service = yield* host;
          const idempotencyKey = yield* modelKey("followUp");

          return yield* ops
            .followUp(parameters.worker, parameters.parameters, { idempotencyKey })
            .pipe(Effect.provideService(SubagentHost, service), Effect.provide(captured));
        }),
      [inspectTool.name]: (parameters: {
        readonly worker: Worker<Name>;
        readonly receipt: Receipt;
      }) =>
        Effect.gen(function* () {
          const service = yield* host;

          return yield* ops
            .observe("inspect", parameters.worker, parameters.receipt)
            .pipe(Effect.provideService(SubagentHost, service), Effect.provide(captured));
        }),
      [listTool.name]: (parameters: typeof ListParameters.Type) => ops.list(parameters),
      [summaryTool.name]: (parameters: { readonly worker: Worker<Name> }) =>
        summary(declaration, parameters.worker),
      [cancelTool.name]: (parameters: {
        readonly worker: Worker<Name>;
        readonly receipt: Receipt;
      }) => ops.cancel(parameters.worker, parameters.receipt),
    };

    // Each handler above is paired with its native schema; computed generic names need this bridge.
    return handlers as unknown as Toolkit.HandlersFrom<Tools>;
  });

  type SelectedServices =
    | SelectionServices<Selected["start"], PrepareServices>
    | SelectionServices<Selected["followUp"], PrepareServices>
    | SelectionServices<Selected["inspect"], ProjectServices>
    | ReportServices<Selected["reportToParent"]>;

  // Unselected handlers are never installed. Only selected operations consume projection services.
  const reportingLayer =
    report === undefined
      ? Layer.empty
      : Layer.effect(
          reportService,
          Effect.map(
            Effect.context<Effect.Services<ReturnType<typeof report.prepare>>>(),
            (context): WorkerReporting<WorkerReportPreparationFailure> => ({
              ...report,
              prepare: (value) =>
                Effect.scoped(Effect.suspend(() => report.prepare(value))).pipe(
                  Effect.provide(context),
                  Effect.mapError((error) =>
                    Schema.is(WorkerReportPreparationFailure)(error)
                      ? error
                      : WorkerReportPreparationFailure.make({ stage: "preparation" }),
                  ),
                ),
            }),
          ),
        );

  const layer = Layer.merge(toolkit.toLayer(build), reportingLayer) as Layer.Layer<
    Tool.HandlersFor<Tools>,
    never,
    SelectedServices
  >;

  return Object.freeze({ tools: toolkit.tools, toolkit, layer });
};
