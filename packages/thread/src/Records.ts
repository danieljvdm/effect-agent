import { PolicyLimit } from "@effect-agent/core/AgentError";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import {
  AgentId,
  AttemptId,
  ThreadId,
  DelegationId,
  ReceiptId,
  RunId,
  SettlementId,
  SubmissionId,
  ToolCallId,
  TurnId,
} from "@effect-agent/core/Identifiers";
import { ExhaustedLimit } from "@effect-agent/core/RunEvent";
import { RunPolicyUsage } from "@effect-agent/core/RunPolicyUsage";
import {
  SubagentBudgetReservation,
  SubagentParentLink,
  ToolExecutionKind,
} from "@effect-agent/core/SubagentContract";
import { ModelCallUsage, RunUsageSummary } from "@effect-agent/core/Usage";
import { Encoding, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";

const identifier = <const Name extends string>(name: Name) =>
  Schema.NonEmptyString.pipe(Schema.brand(`@effect-agent/thread/${name}`));

/** Stable identity of one canonical record. */
export const RecordId = identifier("RecordId");
export type RecordId = typeof RecordId.Type;

/** Stable idempotency identity of one atomic append. */
export const BatchId = identifier("BatchId");
export type BatchId = typeof BatchId.Type;

/** Identity of the deployment that produced a record. */
export const DeploymentId = identifier("DeploymentId");
export type DeploymentId = typeof DeploymentId.Type;

/** Identity of a fenced canonical-log producer. */
export const ProducerId = identifier("ProducerId");
export type ProducerId = typeof ProducerId.Type;

/** SHA-256 digest encoded as lowercase hexadecimal text. */
export const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)).pipe(
  Schema.brand("@effect-agent/thread/Digest"),
);

export type Digest = typeof Digest.Type;

/** Adapter-owned resume cursor. Callers must not parse or synthesize it. */
export const ObservationOffset = identifier("ObservationOffset");
export type ObservationOffset = typeof ObservationOffset.Type;

/** Gap-free position in one Thread's canonical sequence. */
export const CanonicalSequence = Schema.Natural.pipe(
  Schema.brand("@effect-agent/thread/CanonicalSequence"),
);

export type CanonicalSequence = typeof CanonicalSequence.Type;

/** Monotonic fencing epoch for a Thread producer. */
export const ProducerEpoch = Schema.Natural.pipe(
  Schema.brand("@effect-agent/thread/ProducerEpoch"),
);

export type ProducerEpoch = typeof ProducerEpoch.Type;

const BoundedText = Schema.String.check(Schema.isMaxLength(64 * 1024));
const BoundedName = Schema.NonEmptyString.check(Schema.isMaxLength(256));

/** Stable, non-empty failure classification carried by a failed durable Settlement. */
const SettlementFailureTag = Schema.NonEmptyString.check(Schema.isMaxLength(256));

/** Diagnostic text is bounded independently from the broader persisted JSON envelope. */
const SettlementFailureMessage = Schema.String.check(Schema.isMaxLength(16 * 1024));

/**
 * The complete generic diagnostic allowed on a failed durable Settlement. It deliberately has
 * no raw Cause, stack, provider payload, or application-specific fields; those values can carry
 * secrets and do not form a stable cross-process contract.
 */
export const SettlementFailureDiagnostic = Schema.Struct({
  errorTag: SettlementFailureTag,
  message: SettlementFailureMessage,
}).pipe(
  Schema.annotate({
    identifier: "@effect-agent/thread/SettlementFailureDiagnostic",
    parseOptions: { onExcessProperty: "error" },
  }),
);

export type SettlementFailureDiagnostic = typeof SettlementFailureDiagnostic.Type;

/** Positive canonical (Run-relative, Attempt-independent) Turn number. */
const TurnNumber = Schema.Int.check(Schema.isGreaterThan(0));

export const MAX_PERSISTED_JSON_DEPTH = 64;
export const MAX_PERSISTED_JSON_COLLECTION_LENGTH = 4_096;
export const MAX_PERSISTED_JSON_NODES = 65_536;
export const MAX_PERSISTED_JSON_BYTES = 1024 * 1024;

/**
 * Iteratively preflights an unknown value before Schema's recursive JSON validation. This is the
 * one narrow `Schema.declare` exception in the persistence model: Effect v4's `Unknown.decodeTo`
 * preserves `unknown` as the encoded type, which would leak through every nested record codec.
 * Schema.Json still owns the accepted value shape after this resource preflight succeeds.
 */
const isJson = Schema.is(Schema.Json);

const isPersistedJson = (input: unknown): input is Schema.Json => {
  const pending: Array<{ readonly value: unknown; readonly depth: number }> = [
    { value: input, depth: 0 },
  ];

  const visited = new WeakSet<object>();
  let nodes = 0;
  let textUnits = 0;

  try {
    while (pending.length > 0) {
      const current = pending.pop();

      if (current === undefined) return false;
      if (current.depth > MAX_PERSISTED_JSON_DEPTH || ++nodes > MAX_PERSISTED_JSON_NODES) {
        return false;
      }

      const value = current.value;

      if (value === null || typeof value === "boolean") continue;
      if (typeof value === "number") {
        if (!Number.isFinite(value)) return false;
        continue;
      }
      if (typeof value === "string") {
        textUnits += value.length;
        if (textUnits > MAX_PERSISTED_JSON_BYTES) return false;
        continue;
      }
      if (typeof value !== "object" || visited.has(value)) return false;
      visited.add(value);

      const entries = Array.isArray(value)
        ? value.map((entry, index) => [index, entry] as const)
        : Object.entries(value);

      if (entries.length > MAX_PERSISTED_JSON_COLLECTION_LENGTH) return false;
      for (const [key, entry] of entries) {
        textUnits += typeof key === "string" ? key.length : 0;
        if (textUnits > MAX_PERSISTED_JSON_BYTES) return false;
        pending.push({ value: entry, depth: current.depth + 1 });
      }
    }

    if (!isJson(input)) return false;
    const encoded = JSON.stringify(input);

    return (
      encoded !== undefined && Encoding.encodeHex(encoded).length / 2 <= MAX_PERSISTED_JSON_BYTES
    );
  } catch {
    return false;
  }
};

/** Canonical JSON admitted to persisted records and checkpoints under explicit resource limits. */
export const PersistedJson = Schema.declare(isPersistedJson, {
  identifier: "@effect-agent/thread/PersistedJson",
  description: "JSON bounded by canonical persistence depth, collection, node, and byte limits",
});

export type PersistedJson = typeof PersistedJson.Type;

/** Schema-owned replay inputs whose individual definitions are incorporated into digests. */
export class DefinitionDigestInput extends Schema.Class<DefinitionDigestInput>(
  "@effect-agent/thread/DefinitionDigestInput",
)({
  agent: PersistedJson,
  model: PersistedJson,
  tools: PersistedJson,
}) {}

/** Digests make a replay-visible definition/configuration change explicit. */
export class DefinitionDigests extends Schema.Class<DefinitionDigests>(
  "@effect-agent/thread/DefinitionDigests",
)({
  agent: Digest,
  model: Digest,
  tools: Digest,
}) {}

export class ThreadCreated extends Schema.TaggedClass<ThreadCreated>(
  "@effect-agent/thread/ThreadCreated",
)("ThreadCreated", {
  agentId: AgentId,
  definitions: DefinitionDigests,
}) {}

/**
 * One recorded Thread input. `submissionId` is present only for durably accepted work;
 * retained history has no admission or settlement obligation. `kind` identifies the input seam.
 */
export class UserInputRecorded extends Schema.TaggedClass<UserInputRecorded>(
  "@effect-agent/thread/UserInputRecorded",
)("UserInputRecorded", {
  submissionId: Schema.optionalKey(SubmissionId),
  kind: Schema.Literals(["user", "steering", "follow-up"]),
  runId: Schema.optionalKey(RunId),
  input: PersistedJson,
}) {}

/** Immutable clock and duration allowance for one logical Run, before any agent execution. */
export class RunStartedRecord extends Schema.TaggedClass<RunStartedRecord>(
  "@effect-agent/thread/RunStartedRecord",
)("RunStarted", {
  runId: RunId,
  /** Older private histories cannot prove programmatic accounting and must be reset. */
  policyAccountingVersion: Schema.Literal(1),
  maxDurationMillis: Schema.Finite.check(Schema.isGreaterThan(0)),
}) {}

const PersistedPromptMessages = Schema.toEncoded(Prompt.Prompt);
const isPersistedPromptMessages = Schema.is(PersistedPromptMessages);

/**
 * Final model output. Immediate history may include the exact encoded Prompt suffix of the
 * successful Run; durable execution instead journals individual ModelResponseRecorded Turns.
 */
export class ModelCompleted extends Schema.TaggedClass<ModelCompleted>(
  "@effect-agent/thread/ModelCompleted",
)(
  "ModelCompleted",
  Schema.Struct({
    runId: RunId,
    output: PersistedJson,
    messages: Schema.optionalKey(PersistedJson),
  }).check(
    Schema.makeFilter(
      (record) => record.messages === undefined || isPersistedPromptMessages(record.messages),
      { title: "Retained Run messages are Schema-encoded Effect AI Prompt messages" },
    ),
  ),
) {}

export class ToolCallSettled extends Schema.TaggedClass<ToolCallSettled>(
  "@effect-agent/thread/ToolCallSettled",
)("ToolCallSettled", {
  runId: RunId,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  result: PersistedJson,
  isFailure: Schema.Boolean,
  /** Explicit engine evidence, never inferred from Tool result data. */
  budgetRejected: Schema.optionalKey(Schema.Literal(true)),
}) {}

/**
 * One committed model Turn. `messages` carries the Schema-encoded Effect AI Prompt messages this
 * Turn appended (assistant response plus any tool-call declarations), committed atomically at the
 * Turn boundary so a recovering Attempt can rebuild the next Prompt from canonical records alone.
 * `messagesDigest` pins the exact encoded content.
 */
const ModelResponseRecordedFields = Schema.Struct({
  runId: RunId,
  turnId: TurnId,
  turn: TurnNumber,
  messages: PersistedJson,
  messagesDigest: Digest,
  /**
   * Number of leading messages that belong only to this Run's evaluated instructions and wake
   * input. They remain canonical and are visible while recovering this Run, but later Runs omit
   * them from their model-facing history. Records without this field retain their full history.
   */
  runScopedPrefixLength: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  /**
   * Exact normalized usage for every model call staged into this Turn. A
   * summarizer and the Turn response are distinct entries. Absent on legacy
   * records, whose aggregate totals below retain the old resume behavior.
   */
  modelUsage: Schema.optionalKey(Schema.Array(ModelCallUsage)),
  /** Aggregate compatibility fields used by older projections. */
  inputTokens: Schema.optionalKey(Schema.Natural),
  outputTokens: Schema.optionalKey(Schema.Natural),
  /** Estimated spend staged with the usage; recovery re-seeds the cost budget (RUN-023). */
  costMicrousd: Schema.optionalKey(Schema.Natural),
}).check(
  Schema.makeFilter(
    (response) =>
      response.runScopedPrefixLength === undefined ||
      (isPersistedPromptMessages(response.messages) &&
        response.runScopedPrefixLength < response.messages.content.length &&
        response.messages.content
          .slice(0, response.runScopedPrefixLength)
          .every((message) => message.role === "system" || message.role === "user")),
    {
      title:
        "Run-scoped Prompt prefix contains only instruction/wake messages and leaves one response",
    },
  ),
);

export class ModelResponseRecorded extends Schema.TaggedClass<ModelResponseRecorded>(
  "@effect-agent/thread/ModelResponseRecorded",
)("ModelResponseRecorded", ModelResponseRecordedFields) {}

/**
 * One approved uncertain/idempotent ordinary Tool Call made durable BEFORE any handler starts
 * (durability §10). `parameters` is the Schema-encoded wire form of the declared call and
 * `parametersDigest` pins it; recovery that sees this record without a matching `ToolCallSettled`
 * (or `ToolCallUnknown` → `ToolCallResolved`) must reconcile or mark unknown, never silently
 * replay (DUR-009). Ordinary `readonly`-class Tools never produce this record. Delegation
 * classification is always prepared, including for a Tool annotated `readonly`.
 */
export class ToolCallPrepared extends Schema.TaggedClass<ToolCallPrepared>(
  "@effect-agent/thread/ToolCallPrepared",
)("ToolCallPrepared", {
  runId: RunId,
  turnId: TurnId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  parameters: PersistedJson,
  parametersDigest: Digest,
  /** Absent legacy evidence grants no delegation replay authority. */
  executionKind: Schema.optionalKey(ToolExecutionKind),
}) {}

/** Monotonic reservations charged before programmatic execution or grace finalization. */
export class RunPolicyUsageReserved extends Schema.TaggedClass<RunPolicyUsageReserved>()(
  "RunPolicyUsageReserved",
  {
    runId: RunId,
    programmaticToolCalls: RunPolicyUsage.fields.programmaticToolCalls,
    finalizationUsed: RunPolicyUsage.fields.finalizationUsed,
  },
) {}

/**
 * A durable Unknown Outcome: the external effect of one prepared ordinary Tool Call may have
 * happened but was not confirmed canonically (DUR-009/DUR-017). It is neither success nor
 * ordinary failure; automatic continuation stops until an authorized resolution arrives.
 */
export class ToolCallUnknown extends Schema.TaggedClass<ToolCallUnknown>(
  "@effect-agent/thread/ToolCallUnknown",
)("ToolCallUnknown", {
  runId: RunId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  reason: BoundedText,
}) {}

/** How one open/unknown Tool Call was authoritatively closed (DUR-017 resolution audit). */
export const ToolCallResolution = Schema.Literals([
  "completed-with-result",
  "failed-with-error",
  "never-started",
  "safe-retry",
]);

export type ToolCallResolution = typeof ToolCallResolution.Type;

/**
 * The canonical audit record closing one open or unknown Tool Call: reconciler-recovered supplier
 * truth or an authorized `resolveUnknown` command. `author`/`reason` make every resolution an
 * attributable decision (DUR-017); a `completed-with-result` resolution is accompanied by the
 * per-call `ToolCallSettled` record carrying the recovered result.
 */
export class ToolCallResolved extends Schema.TaggedClass<ToolCallResolved>(
  "@effect-agent/thread/ToolCallResolved",
)("ToolCallResolved", {
  runId: RunId,
  toolCallId: ToolCallId,
  resolution: ToolCallResolution,
  author: BoundedName,
  reason: BoundedText,
}) {}

/**
 * One accepted Durable Step result (durability §11): exactly-once-recorded while the Step's
 * external side effect stays honestly at-least-once-executed. Only success is recorded — a failing
 * Step body fails into the handler's error channel and re-executes on re-entry. `output` is the
 * Schema-encoded Step output; `outputDigest` pins it for replay-divergence detection.
 */
export class ToolStepSettled extends Schema.TaggedClass<ToolStepSettled>(
  "@effect-agent/thread/ToolStepSettled",
)("ToolStepSettled", {
  runId: RunId,
  toolCallId: ToolCallId,
  stepName: BoundedName,
  output: PersistedJson,
  outputDigest: Digest,
}) {}

/**
 * A canonical approval request for one declared Tool Call (CAP-006, durability §8): with this
 * record durable, "waiting for explicit approval" is a safe suspension boundary — the resumed
 * Attempt replays the declared batch instead of re-invoking the model.
 */
export class ToolApprovalRequested extends Schema.TaggedClass<ToolApprovalRequested>(
  "@effect-agent/thread/ToolApprovalRequested",
)("ToolApprovalRequested", {
  runId: RunId,
  turnId: TurnId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  parametersDigest: Digest,
}) {}

/** The two-valued approval decision family shared by canonical records and ledger intents. */
export const ApprovalDecision = Schema.Literals(["approved", "denied"]);
export type ApprovalDecision = typeof ApprovalDecision.Type;

/**
 * The canonical decision for one requested approval. Appended by the deciding Attempt (policy
 * auto-decisions) or by the resuming Attempt after a durable `resolveApproval` intent; it is the
 * deterministic decision authority for every later Attempt of the same Run.
 */
export class ToolApprovalDecided extends Schema.TaggedClass<ToolApprovalDecided>(
  "@effect-agent/thread/ToolApprovalDecided",
)("ToolApprovalDecided", {
  runId: RunId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  decision: ApprovalDecision,
  resolver: BoundedName,
  reason: BoundedText,
}) {}

/**
 * First-class interruption audit (durability §9): appended by a superseding Attempt before it
 * re-invokes the model for a Turn whose prior owner died without a complete canonical response.
 * Duplicate provider cost is thereby possible AND observable in canonical history.
 */
export class ModelResponseInterrupted extends Schema.TaggedClass<ModelResponseInterrupted>(
  "@effect-agent/thread/ModelResponseInterrupted",
)("ModelResponseInterrupted", {
  runId: RunId,
  supersededEpoch: ProducerEpoch,
  attemptId: AttemptId,
  reason: BoundedText,
}) {}

/**
 * One engine-native compaction committed before the pre-Turn view changes (RUN-026).
 * `coversThrough` is a Thread record sequence: the projection
 * renders records at or below it as the summary (kind `summarize`) or with
 * cleared tool results (kind `clear-tool-results`), never erasing source
 * history. The record carries no digest by decision: it is appended by the
 * fenced owner into the very log it covers, and re-verifying a digest would
 * re-read the covered range on every wake — the O(history) work compaction
 * exists to remove. Host-supplied ContextCompactor decisions use this same commit path.
 * Summaries exceeding BoundedText are rejected before append, never truncated.
 * `summary` is present exactly for `summarize` records; the projection
 * treats a summarize record without one as invalid and ignores it fail-safe.
 */
export class CompactionCreated extends Schema.TaggedClass<CompactionCreated>(
  "@effect-agent/thread/CompactionCreated",
)("CompactionCreated", {
  runId: RunId,
  turn: TurnNumber,
  kind: Schema.Literals(["clear-tool-results", "summarize"]),
  coversThrough: CanonicalSequence,
  summary: Schema.optionalKey(BoundedText),
}) {}

export class RunFailed extends Schema.TaggedClass<RunFailed>("@effect-agent/thread/RunFailed")(
  "RunFailed",
  {
    runId: RunId,
    failure: PersistedJson,
  },
) {}

const RunCompletedFields = Schema.Struct({
  runId: RunId,
  output: PersistedJson,
  /** Application disposition captured with an ordinary completion. */
  runDisposition: Schema.optionalKey(PersistedJson),
  /** Honest soft-landing marker, present exactly when `exhausted` is present. */
  finishReason: Schema.optionalKey(Schema.Literal("budget-exhausted")),
  /** Budget dimension paired with `finishReason` on a soft landing. */
  exhausted: Schema.optionalKey(ExhaustedLimit),
}).check(
  Schema.makeFilter(
    (completed) =>
      (completed.finishReason === undefined) === (completed.exhausted === undefined) &&
      (completed.runDisposition === undefined || completed.finishReason === undefined),
    { title: "Run completion metadata matches its terminal family" },
  ),
);

export class RunCompleted extends Schema.TaggedClass<RunCompleted>(
  "@effect-agent/thread/RunCompleted",
)("RunCompleted", RunCompletedFields) {}

export class RepairAnnotated extends Schema.TaggedClass<RepairAnnotated>(
  "@effect-agent/thread/RepairAnnotated",
)("RepairAnnotated", {
  reason: BoundedText,
  details: PersistedJson,
}) {}

/** Terminal outcome family for one accepted Submission (DUR-002). */
export const SettlementOutcome = Schema.Literals(["completed", "failed", "aborted"]);
export type SettlementOutcome = typeof SettlementOutcome.Type;

/**
 * A durable abort command made canonical before the active worker is interrupted (DUR-012).
 * Repeating the same abort command is idempotent; abort never rewrites a prior terminal outcome.
 */
export class AbortRequested extends Schema.TaggedClass<AbortRequested>(
  "@effect-agent/thread/AbortRequested",
)("AbortRequested", {
  submissionId: SubmissionId,
  author: BoundedName,
  reason: BoundedText,
}) {}

/**
 * The single canonical settlement record owed to one accepted Submission (DUR-002, DUR-011).
 * Canonical history is the outcome authority: the ledger row is finalized from this record and
 * never the other way around (DUR-015).
 */
const RawSubmissionSettled = Schema.Struct({
  submissionId: SubmissionId,
  settlementId: SettlementId,
  receiptId: ReceiptId,
  outcome: SettlementOutcome,
  runId: Schema.optionalKey(RunId),
  result: Schema.optionalKey(PersistedJson),
  /**
   * Application-defined, Schema-encoded disposition for an ordinary completed
   * Run. Absent for budget exhaustion and every non-completed or run-less
   * settlement; consumers decode it with the application definition's Schema.
   */
  runDisposition: Schema.optionalKey(PersistedJson),
  /**
   * Present only when a `completed` Run settled through the final-answer
   * exhaustion resolution (RUN-011, RUN-018): the durable log must be able to
   * distinguish honest-exhaustion completion from ordinary completion without
   * the live event stream. Absent for every ordinary settlement, keeping
   * existing histories and goldens byte-stable (additive, schemaVersion 1).
   */
  finishReason: Schema.optionalKey(Schema.Literal("budget-exhausted")),
  /**
   * The dimension that bound a budget-exhausted completion (RUN-011,
   * RUN-025), carried verbatim from the live `RunCompleted` event so
   * consumers never reconstruct it from message text. Valid only alongside
   * `finishReason: "budget-exhausted"`; absent on histories persisted before
   * the dimension became durable (additive, schemaVersion 1).
   */
  exhausted: Schema.optionalKey(ExhaustedLimit),
  /**
   * The typed `AgentPolicyError.limit` of a `failed` hard-rail settlement
   * (RUN-011): which finite policy dimension failed the Run, preserved
   * alongside the bounded `{errorTag, message}` failure projection in
   * `result`. Absent for every non-policy failure and on histories persisted
   * before the limit became durable (additive, schemaVersion 1).
   */
  policyLimit: Schema.optionalKey(PolicyLimit),
  /** Canonical aggregate of all priced model calls made by this Run. */
  usageSummary: Schema.optionalKey(RunUsageSummary),
});

const isPolicyFailureProjection = Schema.is(
  Schema.Struct({ errorTag: Schema.Literal("AgentPolicyError") }),
);

const isSettlementFailureDiagnostic = Schema.is(SettlementFailureDiagnostic);

const hasValidSettlementFamily = (settled: typeof RawSubmissionSettled.Type): boolean =>
  (settled.finishReason === undefined || settled.outcome === "completed") &&
  (settled.finishReason === undefined) === (settled.exhausted === undefined) &&
  (settled.usageSummary === undefined || settled.runId !== undefined) &&
  (settled.runDisposition === undefined ||
    (settled.outcome === "completed" &&
      settled.finishReason === undefined &&
      settled.runId !== undefined &&
      settled.result !== undefined)) &&
  (settled.outcome !== "failed" || isSettlementFailureDiagnostic(settled.result)) &&
  (settled.outcome !== "aborted" || settled.result === undefined) &&
  (settled.policyLimit === undefined ||
    (settled.outcome === "failed" && isPolicyFailureProjection(settled.result)));

const SubmissionSettledFields = RawSubmissionSettled.check(
  Schema.makeFilter(hasValidSettlementFamily, {
    title:
      "failed settlements require a bounded diagnostic; aborted settlements carry no result; budget metadata must match its settlement family",
  }),
);

export class SubmissionSettled extends Schema.TaggedClass<SubmissionSettled>(
  "@effect-agent/thread/SubmissionSettled",
)("SubmissionSettled", SubmissionSettledFields) {}

/** Canonical completed settlement; joined completion may legitimately carry no independent result. */
export type CompletedSubmissionSettled = SubmissionSettled & {
  readonly outcome: "completed";
};

/** Canonical failed settlement; its bounded diagnostic is required and typed. */
export type FailedSubmissionSettled = SubmissionSettled & {
  readonly outcome: "failed";
  readonly result: SettlementFailureDiagnostic;
};

/** Canonical aborted settlement; abort records intent rather than fabricating a terminal result. */
export type AbortedSubmissionSettled = SubmissionSettled & {
  readonly outcome: "aborted";
  readonly result?: never;
};

export type SubmissionSettledRecord =
  | CompletedSubmissionSettled
  | FailedSubmissionSettled
  | AbortedSubmissionSettled;

/**
 * Canonical-boundary view of `SubmissionSettled`: `finishReason` is valid
 * only on a `completed` outcome, `exhausted` only alongside
 * `finishReason: "budget-exhausted"`, every `failed` outcome requires the exact bounded
 * `SettlementFailureDiagnostic`, aborted outcomes carry no result, and `policyLimit` only occurs
 * on a `failed` outcome whose diagnostic carries the `AgentPolicyError` tag. `runDisposition`
 * occurs only on an ordinary completed settlement with a Run —
 * so a malformed persisted combination such as
 * `{ outcome: "failed", finishReason: "budget-exhausted" }` or a
 * `policyLimit` contradicting `result.errorTag` fails closed at decode
 * instead of becoming trusted audit history (STORE-006, RUN-011).
 */
export const SubmissionSettledRecord = SubmissionSettled.pipe(
  Schema.refine(
    (settled): settled is SubmissionSettledRecord => hasValidSettlementFamily(settled),
    {
      expected:
        "failed settlements require a bounded diagnostic, aborted settlements carry no result, finishReason only occurs on completed settlements, exhausted only occurs with finishReason budget-exhausted, runDisposition only occurs on an ordinary completed settlement with a Run result, and policyLimit only occurs on a failed AgentPolicyError settlement",
    },
  ),
);

/**
 * PARENT-log record of one durable child establishment request:
 * the exact parent Tool Call, delegation and target identity, the digests that pin the child's
 * Binding/input/grant, the fenced budget reservation, and the INTENDED child identity derived
 * deterministically from the parent Run and Tool Call pair (D4). `childInput` carries the
 * prepared child input in encoded form (D3) so recovery can complete child admission from this
 * record alone — no live delegation handler is required. `childPrincipal`/`childIdempotencyKey`
 * carry the ledger admission scope with the ledger's exact bounds; the layering keeps their
 * branded Schemas in the ledger port.
 */
export class SubagentRequested extends Schema.TaggedClass<SubagentRequested>(
  "@effect-agent/thread/SubagentRequested",
)("SubagentRequested", {
  runId: RunId,
  turnId: TurnId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  delegationId: DelegationId,
  targetAgentId: AgentId,
  targetDigests: DefinitionDigests,
  childInput: PersistedJson,
  childInputDigest: Digest,
  grantDigest: Digest,
  reservationId: BoundedName,
  reservationDigest: Digest,
  childThreadId: ThreadId,
  childPrincipal: BoundedName,
  childIdempotencyKey: BoundedName,
  /** Effective invocation bound, already clamped to the delegation and child Definition. */
  toolCallAllowance: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  policy: Schema.optionalKey(Schema.toCodecJson(AgentPolicy)),
  budget: Schema.optionalKey(SubagentBudgetReservation),
}) {}

/**
 * PARENT-log record that the intended child exists as accepted work:
 * the full established child identity. It is appended only after the child Receipt
 * exists — SUB-017 holds by construction because `childReceiptId` is a required field.
 */
export class SubagentStarted extends Schema.TaggedClass<SubagentStarted>(
  "@effect-agent/thread/SubagentStarted",
)("SubagentStarted", {
  runId: RunId,
  toolCallId: ToolCallId,
  childThreadId: ThreadId,
  childSubmissionId: SubmissionId,
  childReceiptId: ReceiptId,
  childRunId: RunId,
}) {}

/**
 * PARENT-log record of one verified child settlement join:
 * the child's canonical Settlement identity and outcome, the digests pinning the verified child
 * result and the bounded projected parent result, the child usage summary, and the FINAL
 * consumed/released accounting decision for the reservation. It commits in ONE atomic batch with
 * the parent `ToolCallSettled` record (SUB-019); `beginChildBudgetRelease` replays
 * `finalAccounting` from this record, so canonical history authorizes the release (DUR-015).
 */
export class SubagentJoined extends Schema.TaggedClass<SubagentJoined>(
  "@effect-agent/thread/SubagentJoined",
)("SubagentJoined", {
  runId: RunId,
  toolCallId: ToolCallId,
  childSubmissionId: SubmissionId,
  childSettlementId: SettlementId,
  childOutcome: SettlementOutcome,
  childResultDigest: Digest,
  projectedResultDigest: Digest,
  usageSummary: PersistedJson,
  reservationId: BoundedName,
  finalAccounting: PersistedJson,
}) {}

/**
 * CHILD-log immutable lineage: the Parent Link plus the digests that pin
 * the child's definition, input, and authority grant. It is the first record after the child's
 * `ThreadCreated` (its own single-record batch, so the generic `thread-created:{cid}`
 * batch identity is never contradicted) and the join path verifies it fail-closed — a fabricated
 * child or parent identity fails Parent Link verification (SUB-004, D10).
 */
export class SubagentLineageRecorded extends Schema.TaggedClass<SubagentLineageRecorded>(
  "@effect-agent/thread/SubagentLineageRecorded",
)("SubagentLineageRecorded", {
  parentLink: SubagentParentLink,
  parentSubmissionId: SubmissionId,
  childDefinitionDigests: DefinitionDigests,
  childInputDigest: Digest,
  grantDigest: Digest,
  /** Copied from the canonical request before readiness; restored for every child Attempt. */
  toolCallAllowance: SubagentRequested.fields.toolCallAllowance,
  policy: SubagentRequested.fields.policy,
}) {}

/**
 * Current private-development canonical payload family. Phase 5 adds the seven durable-Tool tags
 * (prepared/unknown/resolved/step/approval-request/approval-decision/interrupted) additively; S2
 * adds the four durable-Subagent tags (requested/started/joined/lineage) additively, so the
 * envelope keeps `schemaVersion: 1` (P4 precedent for additive payload tags).
 */
export const CanonicalRecordPayload = Schema.Union([
  ThreadCreated,
  UserInputRecorded,
  RunStartedRecord,
  RunPolicyUsageReserved,
  ModelCompleted,
  ModelResponseRecorded,
  ToolCallPrepared,
  ToolCallSettled,
  ToolCallUnknown,
  ToolCallResolved,
  ToolStepSettled,
  ToolApprovalRequested,
  ToolApprovalDecided,
  ModelResponseInterrupted,
  CompactionCreated,
  RunFailed,
  RunCompleted,
  AbortRequested,
  SubmissionSettledRecord,
  SubagentRequested,
  SubagentStarted,
  SubagentJoined,
  SubagentLineageRecorded,
  RepairAnnotated,
]);

export type CanonicalRecordPayload = typeof CanonicalRecordPayload.Type;

/**
 * Versioned record envelope stored in an atomic batch. Thread ordering is assigned only
 * when the batch is committed.
 */
export class RecordEnvelope extends Schema.Class<RecordEnvelope>(
  "@effect-agent/thread/RecordEnvelope",
)({
  recordId: RecordId,
  family: Schema.Literal("thread"),
  schemaVersion: Schema.Literal(1),
  createdAt: Schema.DateTimeUtcFromString,
  deploymentId: DeploymentId,
  payload: CanonicalRecordPayload,
}) {}

/** Backward-compatible domain name for a canonical record. */
export const CanonicalRecord = RecordEnvelope;
export type CanonicalRecord = RecordEnvelope;

/** One non-empty, bounded, idempotent atomic append unit. */
export class CanonicalBatch extends Schema.Class<CanonicalBatch>(
  "@effect-agent/thread/CanonicalBatch",
)({
  batchId: BatchId,
  producerId: ProducerId,
  records: Schema.NonEmptyArray(RecordEnvelope).check(Schema.isMaxLength(256)),
}) {}

/**
 * A committed record with its Thread ordering and opaque resumable observation cursor.
 */
export class CanonicalRecordEnvelope extends Schema.Class<CanonicalRecordEnvelope>(
  "@effect-agent/thread/CanonicalRecordEnvelope",
)({
  threadId: ThreadId,
  batchId: BatchId,
  sequence: CanonicalSequence,
  offset: ObservationOffset,
  record: RecordEnvelope,
}) {}

export const CURRENT_CANONICAL_SCHEMA_VERSION = 1 as const;
