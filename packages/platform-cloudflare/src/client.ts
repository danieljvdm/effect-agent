import { AgentId, AgentInputError, type ConversationId } from "@effect-agent/core";
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
  ConversationNotMaterialized,
  ConversationStoreError,
  DefinitionDigests,
  DigestError,
  DurableRuntimeFailpointError,
  FenceRejected,
  IdempotencyKey,
  JoinedToHost,
  LedgerError,
  OperationCaller,
  OperationDenied,
  OperationMutationPreparationError,
  PersistedJson,
  Principal,
  Receipt,
  RunJournalError,
  Settlement,
  SettlementConflict,
  UnknownResolutionCommand,
  UnknownResolutionConflict,
  UnknownResolutionIntent,
  type DurableSubmitAgent,
  type DurableSubmitOptions,
} from "@effect-agent/session";
import { Context, Effect, Layer, Schema } from "effect";

import { DurableAlarmError } from "./alarm.ts";
import { ConversationObjectNamespace, type ConversationObjectRpc } from "./bindings.ts";
import { AdmissionLimitExceeded } from "./config.ts";

/**
 * The Worker↔Conversation-Object host protocol (plan §1.4): Schema envelopes for the host
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
export class ConversationClientError extends Schema.TaggedError<ConversationClientError>()(
  "ConversationClientError",
  {
    conversationId: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** `readAll` reached its explicit materialization bound before the committed tail. */
export class ConversationReadLimitExceeded extends Schema.TaggedError<ConversationReadLimitExceeded>()(
  "ConversationReadLimitExceeded",
  {
    conversationId: Schema.String,
    maximum: Schema.Int.check(Schema.isGreaterThan(0)),
    observed: Schema.Int.check(Schema.isGreaterThan(0)),
  },
) {
  override get message() {
    return (
      `Conversation ${this.conversationId} contains more than the readAll limit of ` +
      `${this.maximum} records; at least ${this.observed} were observed. Use readPage for ` +
      "larger histories."
    );
  }
}

/** Conservative default preventing accidental unbounded history materialization. */
export const DEFAULT_READ_ALL_MAX_RECORDS = 4_096;

const ReadAllMaximum = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(1_000_000),
);
const decodeReadAllMaximum = Schema.decodeUnknownEffect(ReadAllMaximum);

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/**
 * One durable submission, input ALREADY encoded by the caller through the Agent Binding's
 * input schema (the Worker bundles the same Agent definitions as the Object, so schema
 * validation happens client-side; the resolved Binding re-validates at claim time). The
 * Conversation identity is deliberately absent — the addressed Object IS the lane.
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

export class AwaitSettlementRequest extends Schema.Class<AwaitSettlementRequest>(
  "@effect-agent/platform-cloudflare/AwaitSettlementRequest",
)({
  receipt: Receipt,
  caller: OperationCaller,
}) {}

/** One bounded page of canonical records after an optional sequence. */
export class ObservePageRequest extends Schema.Class<ObservePageRequest>(
  "@effect-agent/platform-cloudflare/ObservePageRequest",
)({
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
  caller: OperationCaller,
}) {}

export class AbortHostRequest extends Schema.Class<AbortHostRequest>(
  "@effect-agent/platform-cloudflare/AbortHostRequest",
)({
  command: AbortCommand,
  caller: OperationCaller,
}) {}

export class ApprovalHostRequest extends Schema.Class<ApprovalHostRequest>(
  "@effect-agent/platform-cloudflare/ApprovalHostRequest",
)({
  command: ApprovalDecisionCommand,
  caller: OperationCaller,
}) {}

export class UnknownResolutionHostRequest extends Schema.Class<UnknownResolutionHostRequest>(
  "@effect-agent/platform-cloudflare/UnknownResolutionHostRequest",
)({
  command: UnknownResolutionCommand,
  caller: OperationCaller,
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
  OperationDenied,
  OperationMutationPreparationError,
  AgentInputError,
  DigestError,
  AdmissionConflict,
  SettlementConflict,
  ApprovalConflict,
  UnknownResolutionConflict,
  JoinedToHost,
  LedgerError,
  ConversationStoreError,
  RunJournalError,
  ConversationNotMaterialized,
  AppendConflict,
  FenceRejected,
  DurableRuntimeFailpointError,
  AdmissionLimitExceeded,
  DurableAlarmError,
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
export const decodeAwaitSettlementRequest = Schema.decodeUnknownEffect(AwaitSettlementRequest);
export const encodeAwaitSettlementRequest = Schema.encodeEffect(AwaitSettlementRequest);
export const decodeObservePageRequest = Schema.decodeUnknownEffect(ObservePageRequest);
export const encodeObservePageRequest = Schema.encodeEffect(ObservePageRequest);
export const decodeAbortHostRequest = Schema.decodeUnknownEffect(AbortHostRequest);
export const encodeAbortHostRequest = Schema.encodeEffect(AbortHostRequest);
export const decodeApprovalHostRequest = Schema.decodeUnknownEffect(ApprovalHostRequest);
export const encodeApprovalHostRequest = Schema.encodeEffect(ApprovalHostRequest);
export const decodeUnknownResolutionHostRequest = Schema.decodeUnknownEffect(
  UnknownResolutionHostRequest,
);
export const encodeUnknownResolutionHostRequest = Schema.encodeEffect(UnknownResolutionHostRequest);
export const encodeHostResponse = Schema.encodeEffect(HostResponse);
export const decodeHostResponse = Schema.decodeUnknownEffect(HostResponse);

// ---------------------------------------------------------------------------
// Worker-side client
// ---------------------------------------------------------------------------

/** Failure surface of `CloudflareConversationClient.submit`. */
export type ClientSubmitFailure =
  | AgentInputError
  | DigestError
  | AdmissionConflict
  | LedgerError
  | ConversationStoreError
  | ConversationNotMaterialized
  | AppendConflict
  | FenceRejected
  | DurableRuntimeFailpointError
  | AdmissionLimitExceeded
  | DurableAlarmError
  | HostProtocolError
  | ConversationClientError;

export type ClientAwaitFailure =
  | LedgerError
  | SettlementConflict
  | OperationDenied
  | HostProtocolError
  | ConversationClientError;

export type ClientObserveFailure =
  | ConversationStoreError
  | ConversationNotMaterialized
  | OperationDenied
  | HostProtocolError
  | ConversationClientError;

/** `readAll` adds only its local materialization bound to the paged observation failures. */
export type ClientReadAllFailure = ClientObserveFailure | ConversationReadLimitExceeded;

export type ClientAbortFailure =
  | LedgerError
  | SettlementConflict
  | JoinedToHost
  | OperationDenied
  | OperationMutationPreparationError
  | DurableRuntimeFailpointError
  | HostProtocolError
  | ConversationClientError;

export type ClientApprovalFailure =
  | LedgerError
  | SettlementConflict
  | ApprovalConflict
  | ConversationStoreError
  | RunJournalError
  | OperationDenied
  | OperationMutationPreparationError
  | HostProtocolError
  | ConversationClientError;

export type ClientUnknownFailure =
  | LedgerError
  | SettlementConflict
  | UnknownResolutionConflict
  | JoinedToHost
  | OperationDenied
  | OperationMutationPreparationError
  | DurableRuntimeFailpointError
  | HostProtocolError
  | ConversationClientError;

const SUBMIT_FAILURE_TAGS: ReadonlySet<string> = new Set([
  "AgentInputError",
  "DigestError",
  "AdmissionConflict",
  "LedgerError",
  "ConversationStoreError",
  "ConversationNotMaterialized",
  "AppendConflict",
  "FenceRejected",
  "DurableRuntimeFailpointError",
  "AdmissionLimitExceeded",
  "DurableAlarmError",
  "HostProtocolError",
]);
const AWAIT_FAILURE_TAGS: ReadonlySet<string> = new Set([
  "LedgerError",
  "SettlementConflict",
  "OperationDenied",
  "HostProtocolError",
]);
const OBSERVE_FAILURE_TAGS: ReadonlySet<string> = new Set([
  "ConversationStoreError",
  "ConversationNotMaterialized",
  "OperationDenied",
  "HostProtocolError",
]);
const ABORT_FAILURE_TAGS: ReadonlySet<string> = new Set([
  "LedgerError",
  "SettlementConflict",
  "JoinedToHost",
  "OperationDenied",
  "OperationMutationPreparationError",
  "DurableRuntimeFailpointError",
  "HostProtocolError",
]);
const APPROVAL_FAILURE_TAGS: ReadonlySet<string> = new Set([
  "LedgerError",
  "SettlementConflict",
  "ApprovalConflict",
  "ConversationStoreError",
  "RunJournalError",
  "OperationDenied",
  "OperationMutationPreparationError",
  "HostProtocolError",
]);
const UNKNOWN_FAILURE_TAGS: ReadonlySet<string> = new Set([
  "LedgerError",
  "SettlementConflict",
  "UnknownResolutionConflict",
  "JoinedToHost",
  "OperationDenied",
  "OperationMutationPreparationError",
  "DurableRuntimeFailpointError",
  "HostProtocolError",
]);

const outOfContract = (
  conversationId: string,
  operation: string,
  observed: string,
): ConversationClientError =>
  ConversationClientError.make({
    conversationId,
    message: boundHostDiagnostic(
      `The Conversation Object answered ${operation} with the out-of-contract ${observed}.`,
    ),
  });

/**
 * Narrow one decoded `HostFailed.failure` to the operation's declared failure family; an
 * out-of-contract tag folds into `ConversationClientError` instead of being erased or
 * re-thrown raw (the WP2 discipline). The predicate is the single documented narrowing over
 * the closed `HostFailure` union.
 */
const narrowFailure =
  <Failure extends HostFailure>(tags: ReadonlySet<string>) =>
  (failure: HostFailure): failure is Failure =>
    tags.has(failure._tag);

/** Worker-side client over the Conversation Object namespace (DEPLOY-010). */
export class CloudflareConversationClient extends Context.Service<
  CloudflareConversationClient,
  {
    /** Encode the input client-side, then durably submit to the owning Object. */
    readonly submit: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: DurableSubmitOptions,
    ) => Effect.Effect<Receipt, ClientSubmitFailure, InputSchema["EncodingServices"]>;
    /** Wake-hinted, poll-guaranteed settlement wait executed inside the owning Object. */
    readonly awaitSettlement: (
      receipt: Receipt,
      caller: OperationCaller,
    ) => Effect.Effect<Settlement, ClientAwaitFailure>;
    /** One bounded page of canonical records. */
    readonly readPage: (
      conversationId: ConversationId,
      caller: OperationCaller,
      options?: {
        readonly afterSequence?: CanonicalSequence | undefined;
        readonly limit?: number | undefined;
      },
    ) => Effect.Effect<ReadonlyArray<CanonicalRecordEnvelope>, ClientObserveFailure>;
    /**
     * Canonical records up to the CURRENT committed tail, via repeated pages and bounded by
     * `maxRecords`. A snapshot read, not a live observation — callers wanting liveness
     * re-read after `awaitSettlement`. Use `readPage` for histories above the bound.
     */
    readonly readAll: (
      conversationId: ConversationId,
      caller: OperationCaller,
      options?: { readonly maxRecords?: number | undefined },
    ) => Effect.Effect<ReadonlyArray<CanonicalRecordEnvelope>, ClientReadAllFailure>;
    /**
     * Submission-addressed operations take the owning Conversation explicitly (from the
     * Receipt): minted Submission identities stay OPAQUE outside the storage adapter that
     * minted them (D-P6-5), so the client never parses one to find the lane.
     */
    readonly abort: (
      conversationId: ConversationId,
      command: AbortCommand,
      caller: OperationCaller,
    ) => Effect.Effect<AbortIntent, ClientAbortFailure>;
    readonly resolveApproval: (
      conversationId: ConversationId,
      command: ApprovalDecisionCommand,
      caller: OperationCaller,
    ) => Effect.Effect<ApprovalDecisionIntent, ClientApprovalFailure>;
    readonly resolveUnknown: (
      conversationId: ConversationId,
      command: UnknownResolutionCommand,
      caller: OperationCaller,
    ) => Effect.Effect<UnknownResolutionIntent, ClientUnknownFailure>;
  }
>()("@effect-agent/platform-cloudflare/CloudflareConversationClient") {
  static readonly layer: Layer.Layer<
    CloudflareConversationClient,
    never,
    ConversationObjectNamespace
  > = Layer.effect(CloudflareConversationClient)(
    Effect.gen(function* () {
      const { namespace } = yield* ConversationObjectNamespace;

      const call = (
        conversationId: string,
        operation: string,
        invoke: (stub: DurableObjectStub<ConversationObjectRpc>) => Promise<unknown>,
      ): Effect.Effect<HostResponse, ConversationClientError | HostProtocolError> =>
        Effect.tryPromise({
          try: () => invoke(namespace.get(namespace.idFromName(conversationId))),
          catch: (cause) =>
            ConversationClientError.make({
              conversationId,
              message: boundHostDiagnostic(
                `${operation} did not reach the Conversation Object: ${
                  cause instanceof Error ? cause.message : String(cause)
                }`,
              ),
              cause,
            }),
        }).pipe(
          Effect.flatMap((raw) =>
            decodeHostResponse(raw).pipe(
              Effect.mapError(
                (error): HostProtocolError =>
                  HostProtocolError.make({
                    message: boundHostDiagnostic(
                      `The ${operation} answer could not be decoded: ${error.message}`,
                    ),
                  }),
              ),
            ),
          ),
          Effect.withSpan("CloudflareConversationClient.call", {
            attributes: { conversationId, operation },
          }),
        );

      const expect = <
        Result extends Exclude<HostResponse, HostFailed>,
        Failure extends HostFailure,
      >(
        conversationId: string,
        operation: string,
        resultTag: Result["_tag"],
        tags: ReadonlySet<string>,
      ) => {
        const isExpected = narrowFailure<Failure>(tags);
        return (
          response: HostResponse,
        ): Effect.Effect<Result, Failure | ConversationClientError> => {
          if (response._tag === "HostFailed") {
            const failure = response.failure;
            return isExpected(failure)
              ? Effect.fail(failure)
              : Effect.fail(outOfContract(conversationId, operation, `failure ${failure._tag}`));
          }
          if (response._tag !== resultTag) {
            return Effect.fail(outOfContract(conversationId, operation, `result ${response._tag}`));
          }
          return Effect.succeed(response as Result);
        };
      };

      const readPage = (
        conversationId: ConversationId,
        caller: OperationCaller,
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
            caller,
          });
          const encoded = yield* encodeObservePageRequest(request).pipe(
            Effect.mapError((error) =>
              HostProtocolError.make({
                message: boundHostDiagnostic(`observePage request encode failed: ${error.message}`),
              }),
            ),
          );
          const response = yield* call(conversationId, "observePage", (stub) =>
            stub.observePage(encoded),
          );
          const page = yield* expect<ObservedPage, ClientObserveFailure & HostFailure>(
            conversationId,
            "observePage",
            "ObservedPage",
            OBSERVE_FAILURE_TAGS,
          )(response);
          return page.records;
        });

      return CloudflareConversationClient.of({
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
            const response = yield* call(options.conversationId, "submit", (stub) =>
              stub.submitEncoded(encoded),
            );
            const succeeded = yield* expect<SubmitSucceeded, ClientSubmitFailure & HostFailure>(
              options.conversationId,
              "submit",
              "SubmitSucceeded",
              SUBMIT_FAILURE_TAGS,
            )(response);
            return succeeded.receipt;
          }),

        awaitSettlement: (receipt, caller) =>
          Effect.gen(function* () {
            const encoded = yield* encodeAwaitSettlementRequest(
              AwaitSettlementRequest.make({ receipt, caller }),
            ).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(`receipt encode failed: ${error.message}`),
                }),
              ),
            );
            const response = yield* call(receipt.conversationId, "awaitSettlement", (stub) =>
              stub.awaitSettlementEncoded(encoded),
            );
            const settled = yield* expect<SettlementReached, ClientAwaitFailure & HostFailure>(
              receipt.conversationId,
              "awaitSettlement",
              "SettlementReached",
              AWAIT_FAILURE_TAGS,
            )(response);
            return settled.settlement;
          }),

        readPage,

        readAll: (conversationId, caller, options) =>
          Effect.gen(function* () {
            const maximum = yield* decodeReadAllMaximum(
              options?.maxRecords ?? DEFAULT_READ_ALL_MAX_RECORDS,
            ).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(
                    `readAll maxRecords must be an integer from 1 through 1000000: ${error.message}`,
                  ),
                }),
              ),
            );
            const all: Array<CanonicalRecordEnvelope> = [];
            let after: CanonicalSequence | undefined;
            for (;;) {
              const remaining = maximum - all.length;
              const pageLimit = Math.min(1_024, remaining + 1);
              const page = yield* readPage(conversationId, caller, {
                afterSequence: after,
                limit: pageLimit,
              });
              if (page.length > remaining) {
                return yield* ConversationReadLimitExceeded.make({
                  conversationId,
                  maximum,
                  observed: all.length + page.length,
                });
              }
              all.push(...page);
              const last = page.at(-1);
              if (page.length < pageLimit || last === undefined) return all;
              after = last.sequence;
            }
          }),

        abort: (conversationId, command, caller) =>
          Effect.gen(function* () {
            const encoded = yield* encodeAbortHostRequest(
              AbortHostRequest.make({ command, caller }),
            ).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(`abort command encode failed: ${error.message}`),
                }),
              ),
            );
            const response = yield* call(conversationId, "abort", (stub) =>
              stub.abortEncoded(encoded),
            );
            const recorded = yield* expect<AbortRecorded, ClientAbortFailure & HostFailure>(
              conversationId,
              "abort",
              "AbortRecorded",
              ABORT_FAILURE_TAGS,
            )(response);
            return recorded.intent;
          }),

        resolveApproval: (conversationId, command, caller) =>
          Effect.gen(function* () {
            const encoded = yield* encodeApprovalHostRequest(
              ApprovalHostRequest.make({ command, caller }),
            ).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(`approval command encode failed: ${error.message}`),
                }),
              ),
            );
            const response = yield* call(conversationId, "resolveApproval", (stub) =>
              stub.resolveApprovalEncoded(encoded),
            );
            const recorded = yield* expect<ApprovalRecorded, ClientApprovalFailure & HostFailure>(
              conversationId,
              "resolveApproval",
              "ApprovalRecorded",
              APPROVAL_FAILURE_TAGS,
            )(response);
            return recorded.intent;
          }),

        resolveUnknown: (conversationId, command, caller) =>
          Effect.gen(function* () {
            const encoded = yield* encodeUnknownResolutionHostRequest(
              UnknownResolutionHostRequest.make({ command, caller }),
            ).pipe(
              Effect.mapError((error) =>
                HostProtocolError.make({
                  message: boundHostDiagnostic(
                    `resolution command encode failed: ${error.message}`,
                  ),
                }),
              ),
            );
            const response = yield* call(conversationId, "resolveUnknown", (stub) =>
              stub.resolveUnknownEncoded(encoded),
            );
            const recorded = yield* expect<
              UnknownResolutionRecorded,
              ClientUnknownFailure & HostFailure
            >(
              conversationId,
              "resolveUnknown",
              "UnknownResolutionRecorded",
              UNKNOWN_FAILURE_TAGS,
            )(response);
            return recorded.intent;
          }),
      });
    }),
  );
}
