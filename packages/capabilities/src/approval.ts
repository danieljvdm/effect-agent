import { Clock, Context, DateTime, Duration, Effect, Encoding, Layer, Ref, Schema } from "effect";
import { ConversationId, RunId, ToolCallId } from "@effect-agent/core";

import { RedactedPreview, Redactor, type RedactionError } from "./redaction.ts";

const MAX_APPROVAL_TARGETS = 32;
const MAX_APPROVAL_TARGET_BYTES = 64 * 1024;
const MAX_APPROVAL_AUDIT_EVENTS = 2_048;
const ApprovalTargets = Schema.Array(Schema.String.check(Schema.isMaxLength(2 * 1024)))
  .check(Schema.isMaxLength(MAX_APPROVAL_TARGETS))
  .pipe(
    Schema.refine(
      (targets): targets is ReadonlyArray<string> =>
        targets.reduce((total, target) => total + Encoding.encodeHex(target).length / 2, 0) <=
        MAX_APPROVAL_TARGET_BYTES,
      { expected: `approval targets totaling at most ${MAX_APPROVAL_TARGET_BYTES} UTF-8 bytes` },
    ),
  );

const ApprovalRequestFields = {
  requestId: Schema.NonEmptyString,
  runId: RunId,
  conversationId: ConversationId,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
  actionSummary: Schema.String.check(Schema.isMaxLength(2 * 1024)),
  resourceTargets: ApprovalTargets,
  risk: Schema.Literals(["low", "medium", "high", "critical"]),
  expiresAt: Schema.DateTimeUtcFromString,
  denial: Schema.Literals(["terminal", "recoverable"]),
} as const;

/** Decoded approval metadata before the Redactor owns Tool input preview creation. */
export class ApprovalRequestDraft extends Schema.Class<ApprovalRequestDraft>(
  "@effect-agent/capabilities/ApprovalRequestDraft",
)(ApprovalRequestFields) {}

/** Fully decoded, structurally redacted approval input. */
export class ApprovalRequest extends Schema.Class<ApprovalRequest>(
  "@effect-agent/capabilities/ApprovalRequest",
)({
  ...ApprovalRequestFields,
  redactedInputPreview: RedactedPreview,
}) {}

export class ApprovalApproved extends Schema.TaggedClass<ApprovalApproved>()("ApprovalApproved", {
  requestId: Schema.NonEmptyString,
  decidedAt: Schema.DateTimeUtcFromString,
  resolver: Schema.NonEmptyString,
}) {}

export class ApprovalDenied extends Schema.TaggedClass<ApprovalDenied>()("ApprovalDenied", {
  requestId: Schema.NonEmptyString,
  decidedAt: Schema.DateTimeUtcFromString,
  resolver: Schema.NonEmptyString,
  reason: Schema.String.check(Schema.isMaxLength(2 * 1024)),
  timedOut: Schema.Boolean,
}) {}

export const ApprovalDecision = Schema.Union([ApprovalApproved, ApprovalDenied]);
export type ApprovalDecision = typeof ApprovalDecision.Type;

/** Resolver infrastructure failed before it could make a policy decision. */
export class ApprovalResolverError extends Schema.TaggedErrorClass<ApprovalResolverError>()(
  "ApprovalResolverError",
  { message: Schema.String },
) {}

/** The resolver explicitly observed its request deadline; callers turn this into a denial. */
export class ApprovalTimedOut extends Schema.TaggedErrorClass<ApprovalTimedOut>()(
  "ApprovalTimedOut",
  {
    requestId: Schema.NonEmptyString,
  },
) {}

/** First-class approval authority. Interactive, policy, and remote adapters implement this service. */
export class ApprovalResolver extends Context.Service<
  ApprovalResolver,
  {
    readonly request: (
      request: ApprovalRequest,
    ) => Effect.Effect<ApprovalDecision, ApprovalResolverError | ApprovalTimedOut>;
  }
>()("@effect-agent/capabilities/ApprovalResolver") {}

export class ApprovalRequestRecorded extends Schema.TaggedClass<ApprovalRequestRecorded>()(
  "ApprovalRequestRecorded",
  {
    sequence: Schema.Natural,
    recordedAt: Schema.DateTimeUtcFromString,
    request: ApprovalRequest,
  },
) {}

export class ApprovalDecisionRecorded extends Schema.TaggedClass<ApprovalDecisionRecorded>()(
  "ApprovalDecisionRecorded",
  {
    sequence: Schema.Natural,
    recordedAt: Schema.DateTimeUtcFromString,
    decision: ApprovalDecision,
  },
) {}

export const ApprovalAuditEvent = Schema.Union([ApprovalRequestRecorded, ApprovalDecisionRecorded]);
export type ApprovalAuditEvent = typeof ApprovalAuditEvent.Type;
export const ApprovalAuditEvents = Schema.Array(ApprovalAuditEvent).check(
  Schema.isMaxLength(MAX_APPROVAL_AUDIT_EVENTS),
);

export class ApprovalAuditLimitExceeded extends Schema.TaggedErrorClass<ApprovalAuditLimitExceeded>()(
  "ApprovalAuditLimitExceeded",
  {
    limitValue: Schema.Natural,
    observedValue: Schema.Natural,
  },
) {}

/** A resolver or audit sink tried to associate a decision with another request. */
export class ApprovalDecisionMismatch extends Schema.TaggedErrorClass<ApprovalDecisionMismatch>()(
  "ApprovalDecisionMismatch",
  {
    expectedRequestId: Schema.NonEmptyString,
    observedRequestId: Schema.NonEmptyString,
  },
) {}

/** P2 semantic audit sink; durable runtimes replace this with canonical records. */
export class ApprovalAudit extends Context.Service<
  ApprovalAudit,
  {
    readonly recordRequest: (
      request: ApprovalRequest,
    ) => Effect.Effect<void, ApprovalAuditLimitExceeded>;
    readonly recordDecision: (
      decision: ApprovalDecision,
    ) => Effect.Effect<void, ApprovalAuditLimitExceeded | ApprovalDecisionMismatch>;
    readonly events: Effect.Effect<ReadonlyArray<ApprovalAuditEvent>>;
  }
>()("@effect-agent/capabilities/ApprovalAudit") {}

/** Scoped in-memory audit projection with monotonic event order. */
export const ApprovalAuditMemoryLive = Layer.effect(
  ApprovalAudit,
  Effect.gen(function* () {
    interface AuditState {
      readonly events: ReadonlyArray<ApprovalAuditEvent>;
      readonly pending: ReadonlySet<string>;
    }
    const state = yield* Ref.make<AuditState>({ events: [], pending: new Set() });
    return ApprovalAudit.of({
      recordRequest: (request) =>
        Effect.gen(function* () {
          const recordedAt = DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));
          const error = yield* Ref.modify(state, (current) => {
            // Each accepted request atomically reserves room for its decision.
            const reservedSize = current.events.length + current.pending.size + 2;
            if (reservedSize > MAX_APPROVAL_AUDIT_EVENTS) {
              return [
                ApprovalAuditLimitExceeded.make({
                  limitValue: MAX_APPROVAL_AUDIT_EVENTS,
                  observedValue: reservedSize,
                }),
                current,
              ] as const;
            }
            const event = ApprovalRequestRecorded.make({
              sequence: current.events.length,
              recordedAt,
              request,
            });
            return [
              undefined,
              {
                events: [...current.events, event],
                pending: new Set(current.pending).add(request.requestId),
              },
            ] as const;
          });
          if (error !== undefined) return yield* error;
        }),
      recordDecision: (decision) =>
        Effect.gen(function* () {
          const recordedAt = DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));
          const error = yield* Ref.modify(state, (current) => {
            if (!current.pending.has(decision.requestId)) {
              return [
                ApprovalDecisionMismatch.make({
                  expectedRequestId: [...current.pending][0] ?? "no-pending-request",
                  observedRequestId: decision.requestId,
                }),
                current,
              ] as const;
            }
            const pending = new Set(current.pending);
            pending.delete(decision.requestId);
            const event = ApprovalDecisionRecorded.make({
              sequence: current.events.length,
              recordedAt,
              decision,
            });
            return [
              undefined,
              {
                events: [...current.events, event],
                pending,
              },
            ] as const;
          });
          if (error !== undefined) return yield* error;
        }),
      events: Ref.get(state).pipe(Effect.map((current) => current.events)),
    });
  }),
);

/** Construct an ApprovalRequest only after decoded input passes the configured Redactor. */
export const makeApprovalRequest = Effect.fn("makeApprovalRequest")(function* (
  draft: ApprovalRequestDraft,
  decodedToolInput: unknown,
): Effect.fn.Return<ApprovalRequest, RedactionError, Redactor> {
  const redactor = yield* Redactor;
  const redactedInputPreview = yield* redactor.redact(decodedToolInput);
  return ApprovalRequest.make({
    requestId: draft.requestId,
    runId: draft.runId,
    conversationId: draft.conversationId,
    toolCallId: draft.toolCallId,
    toolName: draft.toolName,
    actionSummary: draft.actionSummary,
    resourceTargets: draft.resourceTargets,
    risk: draft.risk,
    expiresAt: draft.expiresAt,
    denial: draft.denial,
    redactedInputPreview,
  });
});

const timeoutDenial = (request: ApprovalRequest, decidedAt: DateTime.Utc): ApprovalDenied =>
  ApprovalDenied.make({
    requestId: request.requestId,
    decidedAt,
    resolver: "effect-agent.timeout-deny",
    reason: "Approval request expired before a decision was available",
    timedOut: true,
  });

/**
 * Resolve with fail-closed deadline semantics and record request/decision audit
 * events around every successful, denied, timed-out, or resolver-failed
 * resolution. A resolver infrastructure failure records a synthetic denial so
 * the audit reservation is released before the typed error propagates.
 */
export const requestApproval = Effect.fn("requestApproval")(function* (request: ApprovalRequest) {
  const audit = yield* ApprovalAudit;
  yield* audit.recordRequest(request);
  const nowMillis = yield* Clock.currentTimeMillis;
  const now = DateTime.toUtc(DateTime.makeUnsafe(nowMillis));
  const remainingMillis = DateTime.toEpochMillis(request.expiresAt) - nowMillis;

  let decision: ApprovalDecision;
  if (remainingMillis <= 0) {
    decision = timeoutDenial(request, now);
  } else {
    const resolver = yield* ApprovalResolver;
    decision = yield* resolver.request(request).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(remainingMillis),
        orElse: () =>
          Clock.currentTimeMillis.pipe(
            Effect.map((millis) =>
              timeoutDenial(request, DateTime.toUtc(DateTime.makeUnsafe(millis))),
            ),
          ),
      }),
      Effect.catchTag("ApprovalTimedOut", () =>
        Clock.currentTimeMillis.pipe(
          Effect.map((millis) =>
            timeoutDenial(request, DateTime.toUtc(DateTime.makeUnsafe(millis))),
          ),
        ),
      ),
      Effect.tapError(() =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((millis) =>
            audit.recordDecision(
              ApprovalDenied.make({
                requestId: request.requestId,
                decidedAt: DateTime.toUtc(DateTime.makeUnsafe(millis)),
                resolver: "effect-agent.resolver-error",
                reason: "Approval resolver failed before making a policy decision",
                timedOut: false,
              }),
            ),
          ),
        ),
      ),
    );
  }
  if (decision.requestId !== request.requestId) {
    const mismatch = ApprovalDecisionMismatch.make({
      expectedRequestId: request.requestId,
      observedRequestId: decision.requestId,
    });
    const denied = ApprovalDenied.make({
      requestId: request.requestId,
      decidedAt: DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis)),
      resolver: "effect-agent.correlation-check",
      reason: "Approval resolver returned a decision for another request",
      timedOut: false,
    });
    yield* audit.recordDecision(denied);
    return yield* mismatch;
  }
  yield* audit.recordDecision(decision);
  return decision;
});

/** Deterministic policy layer for tests and deny-by-default deployments. */
export const ApprovalDenyAll = Layer.succeed(ApprovalResolver)({
  request: (request) =>
    Clock.currentTimeMillis.pipe(
      Effect.map((millis) =>
        ApprovalDenied.make({
          requestId: request.requestId,
          decidedAt: DateTime.toUtc(DateTime.makeUnsafe(millis)),
          resolver: "effect-agent.deny-all",
          reason: "No approving policy is configured",
          timedOut: false,
        }),
      ),
    ),
});
