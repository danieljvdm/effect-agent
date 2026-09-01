import { AgentId, AgentInputError, type ThreadId } from "@effect-agent/core";
import {
  AbortCommand,
  AbortIntent,
  AdmissionConflict,
  AppendConflict,
  ApprovalConflict,
  ApprovalDecisionCommand,
  ApprovalDecisionIntent,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ThreadNotMaterialized,
  ThreadStoreError,
  DefinitionDigests,
  DigestError,
  DurableRuntimeFailpointError,
  FenceRejected,
  IdempotencyKey,
  JoinedToHost,
  LedgerError,
  OperationDenied,
  PersistedJson,
  Principal,
  Receipt,
  Settlement,
  SettlementConflict,
  UnknownResolutionCommand,
  UnknownResolutionConflict,
  UnknownResolutionIntent,
  type DurableSubmitAgent,
  type DurableSubmitOptions,
} from "@effect-agent/thread";
import { Context, Crypto, Duration, Effect, Layer, Schema } from "effect";
import { RpcTracing } from "effect-cf";

import { DurableAlarmError } from "./alarm.ts";
import { ThreadObjectNamespace, type ThreadObjectRpc } from "./bindings.ts";
import { cloudflareFailureSignals, safeCauseMessage } from "./boundary.ts";
import { AdmissionLimitExceeded } from "./config.ts";

/**
 * The Worker↔Thread-Object host protocol (plan §1.4): Schema envelopes for the host
 * entry points (`submitEncoded`, `awaitSettlementEncoded`, `observePage`, `abortEncoded`,
 * `resolveApprovalEncoded`, `resolveUnknownEncoded`) plus the Worker-side client that speaks
 * it. Mirrors the WP2 port protocol: requests and responses are closed Schema unions, typed
 * failures travel as their own tagged classes and RE-DECODE to identical tags on the caller
 * (error-tag fidelity), and protocol anomalies are answered typed instead of thrown.
 */

/** Ceiling for host protocol diagnostic strings. */
const MAX_HOST_DIAGNOSTIC_LENGTH = 4_096;

const BoundedDiagnostic = Schema.String.check(Schema.isMaxLength(MAX_HOST_DIAGNOSTIC_LENGTH));

/** Truncate a diagnostic string to the host protocol's bounded length. */
export const boundHostDiagnostic = (value: string): string =>
  value.length > MAX_HOST_DIAGNOSTIC_LENGTH
    ? `${value.slice(0, MAX_HOST_DIAGNOSTIC_LENGTH - 3)}...`
    : value;

/**
 * The envelope itself could not be honored: the Object could not decode the request, or a
 * response could not be encoded/decoded. Never carries operation semantics.
 */
export class HostProtocolError extends Schema.TaggedError<HostProtocolError>()(
  "HostProtocolError",
  {
    message: BoundedDiagnostic,
  },
) {}

/** The Worker-side stub call itself failed (RPC rejection, overload, eviction mid-call). */
export class ThreadClientError extends Schema.TaggedError<ThreadClientError>()(
  "ThreadClientError",
  {
    threadId: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
    /** Cloudflare's own classification for a failure safe to retry with a fresh stub. */
    retryable: Schema.optionalKey(Schema.Boolean),
    /** Cloudflare overloads are surfaced immediately instead of adding retry pressure. */
    overloaded: Schema.optionalKey(Schema.Boolean),
  },
) {}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/**
 * One durable submission, input ALREADY encoded by the caller through the Agent Binding's
 * input schema (the Worker bundles the same Agent definitions as the Object, so schema
 * validation happens client-side; the resolved Binding re-validates at claim time). The
 * Thread identity is deliberately absent — the addressed Object IS the lane.
 */
export class SubmitRequest extends Schema.Class<SubmitRequest>(
  "@effect-agent/platform-cloudflare/SubmitRequest",
)({
  agentId: AgentId,
  principal: Principal,
  idempotencyKey: IdempotencyKey,
  definitions: DefinitionDigests,
  inputPayload: PersistedJson,
}) {}

/** One bounded page of canonical records after an optional sequence. */
export class ObservePageRequest extends Schema.Class<ObservePageRequest>(
  "@effect-agent/platform-cloudflare/ObservePageRequest",
)({
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

/** One event-driven wait for canonical progress strictly after this sequence. */
export class AwaitProgressRequest extends Schema.Class<AwaitProgressRequest>(
  "@effect-agent/platform-cloudflare/AwaitProgressRequest",
)({
  afterSequence: CanonicalSequence,
  waiterId: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
}) {}

/** Best-effort cancellation of one in-flight progress RPC. */
export class CancelProgressRequest extends Schema.Class<CancelProgressRequest>(
  "@effect-agent/platform-cloudflare/CancelProgressRequest",
)({
  waiterId: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
}) {}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/**
 * Every typed failure a host entry point can produce, plus the protocol's own errors. Same
 * closed-union discipline as the WP2 `PortFailure`: members re-decode to the SAME tagged
 * classes on the Worker side; `cause` chains travel as Schema defects without instance
 * fidelity (plan §2.8).
 */
export const HostFailure = Schema.Union([
  AgentInputError,
  DigestError,
  AdmissionConflict,
  SettlementConflict,
  ApprovalConflict,
  UnknownResolutionConflict,
  JoinedToHost,
  LedgerError,
  ThreadStoreError,
  ThreadNotMaterialized,
  AppendConflict,
  FenceRejected,
  DurableRuntimeFailpointError,
  AdmissionLimitExceeded,
  DurableAlarmError,
  OperationDenied,
  HostProtocolError,
]);
export type HostFailure = typeof HostFailure.Type;

export class SubmitSucceeded extends Schema.TaggedClass<SubmitSucceeded>(
  "@effect-agent/platform-cloudflare/SubmitSucceeded",
)("SubmitSucceeded", {
  receipt: Receipt,
}) {}

export class SettlementReached extends Schema.TaggedClass<SettlementReached>(
  "@effect-agent/platform-cloudflare/SettlementReached",
)("SettlementReached", {
  settlement: Settlement,
}) {}

export class ObservedPage extends Schema.TaggedClass<ObservedPage>(
  "@effect-agent/platform-cloudflare/ObservedPage",
)("ObservedPage", {
  records: Schema.Array(CanonicalRecordEnvelope).check(Schema.isMaxLength(1_024)),
}) {}

/** A record was already committed or an incarnation-local hint says the caller should re-read. */
export class ProgressObserved extends Schema.TaggedClass<ProgressObserved>(
  "@effect-agent/platform-cloudflare/ProgressObserved",
)("ProgressObserved", {}) {}

export class ProgressCancelled extends Schema.TaggedClass<ProgressCancelled>(
  "@effect-agent/platform-cloudflare/ProgressCancelled",
)("ProgressCancelled", {}) {}

export class AbortRecorded extends Schema.TaggedClass<AbortRecorded>(
  "@effect-agent/platform-cloudflare/AbortRecorded",
)("AbortRecorded", {
  intent: AbortIntent,
}) {}

export class ApprovalRecorded extends Schema.TaggedClass<ApprovalRecorded>(
  "@effect-agent/platform-cloudflare/ApprovalRecorded",
)("ApprovalRecorded", {
  intent: ApprovalDecisionIntent,
}) {}

export class UnknownResolutionRecorded extends Schema.TaggedClass<UnknownResolutionRecorded>(
  "@effect-agent/platform-cloudflare/UnknownResolutionRecorded",
)("UnknownResolutionRecorded", {
  intent: UnknownResolutionIntent,
}) {}

/** The entry point failed TYPED on the Object; the failure re-decodes verbatim. */
export class HostFailed extends Schema.TaggedClass<HostFailed>(
  "@effect-agent/platform-cloudflare/HostFailed",
)("HostFailed", {
  failure: HostFailure,
}) {}

/** The uniform answer of one host entry point. Callers narrow by the tag their call implies. */
export const HostResponse = Schema.Union([
  SubmitSucceeded,
  SettlementReached,
  ObservedPage,
  ProgressObserved,
  ProgressCancelled,
  AbortRecorded,
  ApprovalRecorded,
  UnknownResolutionRecorded,
  HostFailed,
]);
export type HostResponse = typeof HostResponse.Type;

// ---------------------------------------------------------------------------
// Codecs (shared by the Object endpoints and the Worker client)
// ---------------------------------------------------------------------------

export const decodeSubmitRequest = Schema.decodeUnknownEffect(SubmitRequest);
export const encodeSubmitRequest = Schema.encodeEffect(SubmitRequest);
export const decodeReceipt = Schema.decodeUnknownEffect(Receipt);
export const encodeReceipt = Schema.encodeEffect(Receipt);
export const decodeObservePageRequest = Schema.decodeUnknownEffect(ObservePageRequest);
export const encodeObservePageRequest = Schema.encodeEffect(ObservePageRequest);
export const decodeAwaitProgressRequest = Schema.decodeUnknownEffect(AwaitProgressRequest);
export const encodeAwaitProgressRequest = Schema.encodeEffect(AwaitProgressRequest);
export const decodeCancelProgressRequest = Schema.decodeUnknownEffect(CancelProgressRequest);
export const encodeCancelProgressRequest = Schema.encodeEffect(CancelProgressRequest);
export const decodeAbortCommand = Schema.decodeUnknownEffect(AbortCommand);
export const encodeAbortCommand = Schema.encodeEffect(AbortCommand);
export const decodeApprovalDecisionCommand = Schema.decodeUnknownEffect(ApprovalDecisionCommand);
export const encodeApprovalDecisionCommand = Schema.encodeEffect(ApprovalDecisionCommand);
export const decodeUnknownResolutionCommand = Schema.decodeUnknownEffect(UnknownResolutionCommand);
export const encodeUnknownResolutionCommand = Schema.encodeEffect(UnknownResolutionCommand);
export const encodeHostResponse = Schema.encodeEffect(HostResponse);
export const decodeHostResponse = Schema.decodeUnknownEffect(HostResponse);

// ---------------------------------------------------------------------------
// Worker-side client
// ---------------------------------------------------------------------------

/** Failure surface of `CloudflareThreadClient.submit`. */
const ClientSubmitHostFailure = Schema.Union([
  AgentInputError,
  DigestError,
  AdmissionConflict,
  LedgerError,
  ThreadStoreError,
  ThreadNotMaterialized,
  AppendConflict,
  FenceRejected,
  DurableRuntimeFailpointError,
  AdmissionLimitExceeded,
  DurableAlarmError,
  HostProtocolError,
]);
const ClientAwaitHostFailure = Schema.Union([
  LedgerError,
  SettlementConflict,
  OperationDenied,
  HostProtocolError,
]);
const ClientObserveHostFailure = Schema.Union([
  ThreadStoreError,
  ThreadNotMaterialized,
  OperationDenied,
  HostProtocolError,
]);
const ClientAbortHostFailure = Schema.Union([
  OperationDenied,
  LedgerError,
  SettlementConflict,
  JoinedToHost,
  DurableRuntimeFailpointError,
  DurableAlarmError,
  HostProtocolError,
]);
const ClientApprovalHostFailure = Schema.Union([
  LedgerError,
  SettlementConflict,
  ApprovalConflict,
  OperationDenied,
  DurableAlarmError,
  HostProtocolError,
]);
const ClientUnknownHostFailure = Schema.Union([
  LedgerError,
  SettlementConflict,
  UnknownResolutionConflict,
  JoinedToHost,
  DurableRuntimeFailpointError,
  OperationDenied,
  DurableAlarmError,
  HostProtocolError,
]);

export type ClientSubmitFailure = typeof ClientSubmitHostFailure.Type | ThreadClientError;
export type ClientAwaitFailure = typeof ClientAwaitHostFailure.Type | ThreadClientError;
export type ClientObserveFailure = typeof ClientObserveHostFailure.Type | ThreadClientError;
export type ClientProgressFailure = ClientObserveFailure;
export type ClientAbortFailure = typeof ClientAbortHostFailure.Type | ThreadClientError;
export type ClientApprovalFailure = typeof ClientApprovalHostFailure.Type | ThreadClientError;
export type ClientUnknownFailure = typeof ClientUnknownHostFailure.Type | ThreadClientError;

const outOfContract = (threadId: string, operation: string, observed: string): ThreadClientError =>
  ThreadClientError.make({
    threadId,
    message: boundHostDiagnostic(
      `The Thread Object answered ${operation} with the out-of-contract ${observed}.`,
    ),
  });

const hostRpcMethods = {
  submit: "submitEncoded",
  awaitSettlement: "awaitSettlementEncoded",
  awaitProgress: "awaitProgressEncoded",
  cancelProgress: "cancelProgressEncoded",
  observePage: "observePage",
  abort: "abortEncoded",
  resolveApproval: "resolveApprovalEncoded",
  resolveUnknown: "resolveUnknownEncoded",
} as const satisfies Record<string, keyof ThreadObjectRpc>;

/** Worker-side client over the Thread Object namespace (DEPLOY-010). */
export class CloudflareThreadClient extends Context.Service<
  CloudflareThreadClient,
  {
    /** Encode the input client-side, then durably submit to the owning Object. */
    readonly submit: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: DurableSubmitOptions,
    ) => Effect.Effect<Receipt, ClientSubmitFailure, InputSchema["EncodingServices"]>;
    /** Wake-hinted, poll-guaranteed settlement wait executed inside the owning Object. */
    readonly awaitSettlement: (receipt: Receipt) => Effect.Effect<Settlement, ClientAwaitFailure>;
    /**
     * Wait without polling until progress after `afterSequence` is already durable or hinted.
     * The result is deliberately void: canonical records remain authoritative and must be read.
     */
    readonly awaitProgress: (
      threadId: ThreadId,
      afterSequence: CanonicalSequence,
    ) => Effect.Effect<void, ClientProgressFailure>;
    /** One bounded page of canonical records. */
    readonly readPage: (
      threadId: ThreadId,
      options?: {
        readonly afterSequence?: CanonicalSequence | undefined;
        readonly limit?: number | undefined;
      },
    ) => Effect.Effect<ReadonlyArray<CanonicalRecordEnvelope>, ClientObserveFailure>;
    /**
     * Every canonical record up to the CURRENT committed tail, via repeated pages. A
     * snapshot read, not a live observation — callers wanting liveness re-read after
     * `awaitSettlement`.
     */
    readonly readAll: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadonlyArray<CanonicalRecordEnvelope>, ClientObserveFailure>;
    /**
     * Submission-addressed operations take the owning Thread explicitly (from the
     * Receipt): minted Submission identities stay OPAQUE outside the storage adapter that
     * minted them (D-P6-5), so the client never parses one to find the lane.
     */
    readonly abort: (
      threadId: ThreadId,
      command: AbortCommand,
    ) => Effect.Effect<AbortIntent, ClientAbortFailure>;
    readonly resolveApproval: (
      threadId: ThreadId,
      command: ApprovalDecisionCommand,
    ) => Effect.Effect<ApprovalDecisionIntent, ClientApprovalFailure>;
    readonly resolveUnknown: (
      threadId: ThreadId,
      command: UnknownResolutionCommand,
    ) => Effect.Effect<UnknownResolutionIntent, ClientUnknownFailure>;
  }
>()("@effect-agent/platform-cloudflare/CloudflareThreadClient") {
  static readonly layer: Layer.Layer<
    CloudflareThreadClient,
    never,
    ThreadObjectNamespace | Crypto.Crypto
  > = Layer.effect(CloudflareThreadClient)(
    Effect.gen(function* () {
      const { namespace, rpcTracing } = yield* ThreadObjectNamespace;
      const crypto = yield* Crypto.Crypto;

      const call = Effect.fn(
        function* (
          threadId: string,
          operation: keyof typeof hostRpcMethods,
          encoded: unknown,
        ): Effect.fn.Return<HostResponse, ThreadClientError | HostProtocolError> {
          // Empty arguments preserve native arity. Passing `undefined` still adds an argument.
          const traceArgs =
            rpcTracing === undefined ? [] : yield* RpcTracing.withRpcTraceContext([]);
          const raw = yield* Effect.tryPromise({
            try: () => {
              const stub = namespace.get(namespace.idFromName(threadId));
              return stub[hostRpcMethods[operation]](encoded, ...traceArgs);
            },
            catch: (cause) =>
              ThreadClientError.make({
                threadId,
                message: boundHostDiagnostic(
                  `${operation} did not reach the Thread Object: ${safeCauseMessage(
                    cause,
                    "the RPC failed without a diagnostic",
                  )}`,
                ),
                cause,
                ...cloudflareFailureSignals(cause),
              }),
          });
          return yield* decodeHostResponse(raw).pipe(
            Effect.mapError((error): HostProtocolError =>
              HostProtocolError.make({
                message: boundHostDiagnostic(
                  `The ${operation} answer could not be decoded: ${error.message}`,
                ),
              }),
            ),
          );
        },
        (effect, threadId, operation) =>
          rpcTracing === undefined
            ? Effect.withSpan(effect, "CloudflareThreadClient.call", {
                attributes: { threadId, operation },
              })
            : RpcTracing.withRpcClientSpan(effect, rpcTracing, hostRpcMethods[operation]),
      );

      const expect = <ResultSchema extends Schema.Top, FailureSchema extends Schema.Top>(
        threadId: string,
        operation: string,
        resultSchema: ResultSchema,
        failureSchema: FailureSchema,
      ) => {
        const isExpectedResult = Schema.is(resultSchema);
        const isExpectedFailure = Schema.is(failureSchema);
        return (
          response: HostResponse,
        ): Effect.Effect<ResultSchema["Type"], FailureSchema["Type"] | ThreadClientError> => {
          if (response._tag === "HostFailed") {
            const failure = response.failure;
            return isExpectedFailure(failure)
              ? Effect.fail(failure)
              : Effect.fail(outOfContract(threadId, operation, `failure ${failure._tag}`));
          }
          if (!isExpectedResult(response)) {
            return Effect.fail(outOfContract(threadId, operation, `result ${response._tag}`));
          }
          return Effect.succeed(response);
        };
      };

      const readPage = (
        threadId: ThreadId,
        options?: {
          readonly afterSequence?: CanonicalSequence | undefined;
          readonly limit?: number | undefined;
        },
      ) =>
        Effect.gen(function* () {
          const request = ObservePageRequest.make({
            ...(options?.afterSequence === undefined
              ? {}
              : { afterSequence: options.afterSequence }),
            limit: options?.limit ?? 256,
          });
          const encoded = yield* encodeObservePageRequest(request).pipe(
            Effect.mapError((error) =>
              HostProtocolError.make({
                message: boundHostDiagnostic(`observePage request encode failed: ${error.message}`),
              }),
            ),
          );
          const response = yield* call(threadId, "observePage", encoded);
          const page = yield* expect(
            threadId,
            "observePage",
            ObservedPage,
            ClientObserveHostFailure,
          )(response);
          return page.records;
        });

      const cancelProgress = (threadId: ThreadId, waiterId: string): Effect.Effect<void> =>
        encodeCancelProgressRequest(CancelProgressRequest.make({ waiterId })).pipe(
          Effect.mapError(() => undefined),
          Effect.flatMap((encoded) => call(threadId, "cancelProgress", encoded)),
          Effect.asVoid,
          Effect.ignore,
        );

      return CloudflareThreadClient.of({
        submit: <InputSchema extends Schema.Top>(
          agent: DurableSubmitAgent<InputSchema>,
          input: InputSchema["Type"],
          options: DurableSubmitOptions,
        ) =>
          Effect.gen(function* () {
            // Client-side half of `DurableAgentRuntime.submit`'s input boundary: encode
            // through the Agent's input schema and prove the canonical persistence bounds.
            const encodedInput = yield* Schema.encodeEffect(agent.definition.input)(input).pipe(
              Effect.mapError((cause) =>
                AgentInputError.make({ message: `Unable to encode Agent input: ${cause.message}` }),
              ),
            );
            const inputPayload = yield* Schema.decodeUnknownEffect(PersistedJson)(
              encodedInput,
            ).pipe(
              Effect.mapError(() =>
                AgentInputError.make({
                  message: "Agent input does not satisfy the canonical persistence bounds",
                }),
              ),
            );
            const request = SubmitRequest.make({
              agentId: agent.definition.id,
              principal: options.principal,
              idempotencyKey: options.idempotencyKey,
              definitions: options.definitions,
              inputPayload,
            });
            const encoded = yield* encodeSubmitRequest(request).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(`submit request encode failed: ${error.message}`),
                }),
              ),
            );
            const response = yield* call(options.threadId, "submit", encoded);
            const succeeded = yield* expect(
              options.threadId,
              "submit",
              SubmitSucceeded,
              ClientSubmitHostFailure,
            )(response);
            return succeeded.receipt;
          }),

        awaitSettlement: (receipt) =>
          Effect.gen(function* () {
            const encoded = yield* encodeReceipt(receipt).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(`receipt encode failed: ${error.message}`),
                }),
              ),
            );
            const response = yield* call(receipt.threadId, "awaitSettlement", encoded);
            const settled = yield* expect(
              receipt.threadId,
              "awaitSettlement",
              SettlementReached,
              ClientAwaitHostFailure,
            )(response);
            return settled.settlement;
          }),

        awaitProgress: (threadId, afterSequence) =>
          Effect.gen(function* () {
            const waiterId = yield* crypto.randomUUIDv4.pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(
                    `awaitProgress cancellation identity generation failed: ${error.message}`,
                  ),
                }),
              ),
            );
            const request = AwaitProgressRequest.make({ afterSequence, waiterId });
            const encoded = yield* encodeAwaitProgressRequest(request).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(
                    `awaitProgress request encode failed: ${error.message}`,
                  ),
                }),
              ),
            );

            const attempt = (retry: number): Effect.Effect<void, ClientProgressFailure> =>
              call(threadId, "awaitProgress", encoded).pipe(
                Effect.flatMap(
                  expect(threadId, "awaitProgress", ProgressObserved, ClientObserveHostFailure),
                ),
                Effect.asVoid,
                Effect.catchTag("ThreadClientError", (error) =>
                  error.retryable === true && error.overloaded !== true && retry < 5
                    ? Effect.sleep(Duration.millis(10 * 2 ** retry)).pipe(
                        Effect.andThen(attempt(retry + 1)),
                      )
                    : Effect.fail(error),
                ),
              );

            yield* attempt(0).pipe(Effect.onInterrupt(() => cancelProgress(threadId, waiterId)));
          }),

        readPage,

        readAll: (threadId) =>
          Effect.gen(function* () {
            const all: Array<CanonicalRecordEnvelope> = [];
            let after: CanonicalSequence | undefined;
            for (;;) {
              const page = yield* readPage(threadId, { afterSequence: after, limit: 1_024 });
              all.push(...page);
              const last = page.at(-1);
              if (page.length < 1_024 || last === undefined) return all;
              after = last.sequence;
            }
          }),

        abort: (threadId, command) =>
          Effect.gen(function* () {
            const encoded = yield* encodeAbortCommand(command).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(`abort command encode failed: ${error.message}`),
                }),
              ),
            );
            const response = yield* call(threadId, "abort", encoded);
            const recorded = yield* expect(
              threadId,
              "abort",
              AbortRecorded,
              ClientAbortHostFailure,
            )(response);
            return recorded.intent;
          }),

        resolveApproval: (threadId, command) =>
          Effect.gen(function* () {
            const encoded = yield* encodeApprovalDecisionCommand(command).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(`approval command encode failed: ${error.message}`),
                }),
              ),
            );
            const response = yield* call(threadId, "resolveApproval", encoded);
            const recorded = yield* expect(
              threadId,
              "resolveApproval",
              ApprovalRecorded,
              ClientApprovalHostFailure,
            )(response);
            return recorded.intent;
          }),

        resolveUnknown: (threadId, command) =>
          Effect.gen(function* () {
            const encoded = yield* encodeUnknownResolutionCommand(command).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(
                    `resolution command encode failed: ${error.message}`,
                  ),
                }),
              ),
            );
            const response = yield* call(threadId, "resolveUnknown", encoded);
            const recorded = yield* expect(
              threadId,
              "resolveUnknown",
              UnknownResolutionRecorded,
              ClientUnknownHostFailure,
            )(response);
            return recorded.intent;
          }),
      });
    }),
  );
}
