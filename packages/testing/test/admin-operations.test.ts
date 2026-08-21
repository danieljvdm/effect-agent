import { Agent, AgentPolicy, ConversationId, ToolCallId } from "@effect-agent/core";
import {
  ApprovalDecisionCommand,
  CanonicalRecordEnvelope,
  ConversationExport,
  ConversationExportRequest,
  ConversationRead,
  ConversationStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointTestControl,
  IdempotencyKey,
  ObligationThresholds,
  OperationAuthorizer,
  OperationDenied,
  Principal,
  ProducerId,
  RECOVERY_DECISION_MEANINGS,
  RecordEnvelope,
  RecoverySnapshot,
  RecoverySnapshotRequest,
  ResolutionNeverHappened,
  RetryCommand,
  SubmissionLedger,
  SubmissionLookupByKey,
  SubmissionLookupById,
  ToolReconciler,
  UnknownResolutionCommand,
  UserInputRecorded,
  WakeScheduler,
  renderRecoveryExplanation,
  verifyConversationInvariants,
  type AuthorizedOperation,
  type BatchId,
  type DurableExplainFailure,
  type DurableObligationFailure,
  type DurableRetryFailure,
  type DurableRuntimeFailpointLocation,
  type DurableSubmitOptions,
  type DurableVerifyFailure,
  type IntegrityCheckName,
  type IntegrityReport,
  type ObligationReport,
  type OperationAuthorizationRequest,
  type OperationAuthorizerService,
  type RecoveryExplanation,
  type RecoveryReport,
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Context, Duration, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-admin-operations");
const PRODUCER_ID = Schema.decodeSync(ProducerId)("producer-admin-operations");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const submitOptions = (conversationId: string, idempotencyKey: string): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversationId),
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCall = (id: string, name: string, params: unknown): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage },
];

/** Scripted model whose state survives Layer rebuilds across Attempts. */
const makeScriptedModel = (script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const model = Model.make(
      "scripted",
      "admin-operations-test",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(calls, (call) => call + 1).pipe(
                Effect.map((call) => Stream.fromIterable(script(call))),
              ),
            ),
        }),
      ),
    );
    return { model };
  });

const policy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 4,
  maxDuration: "30 seconds",
  toolConcurrency: 2,
});

/** Unannotated → fail-closed `uncertain`: enters the prepared/settled protocol. */
const Book = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});
const bookTools = Toolkit.make(Book);
const bookDefinition = Agent.define("admin-ops-book", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book it.",
  toolkit: bookTools,
  policy,
});
const bookToolLayer = bookTools.toLayer({
  book: ({ ref }) => Effect.succeed({ confirmation: `confirmed-${ref}` }),
});

/** Approval-gated booking Tool: with no resolver, the lane suspends durably. */
const BookApproval = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});
const approvalTools = Toolkit.make(BookApproval);
const approvalDefinition = Agent.define("admin-ops-approval", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book after approval.",
  toolkit: approvalTools,
  policy,
});
const approvalToolLayer = approvalTools.toLayer({
  book: ({ ref }) => Effect.succeed({ confirmation: `confirmed-${ref}` }),
});

/** Tool-free agent for happy-path lanes. */
const plainDefinition = Agent.define("admin-ops-plain", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer.",
  toolkit: Toolkit.empty,
  policy,
});

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-admin-operations"),
  producerId: PRODUCER_ID,
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

/** Ref-driven non-default authorizer: per-test denial sets, plus the request trace. */
class AuthorizerTestControl extends Context.Service<
  AuthorizerTestControl,
  {
    readonly deny: (operations: ReadonlyArray<AuthorizedOperation>) => Effect.Effect<void>;
    readonly reset: Effect.Effect<void>;
    readonly requests: Effect.Effect<ReadonlyArray<OperationAuthorizationRequest>>;
  }
>()("@effect-agent/testing/AuthorizerTestControl") {}

const authorizerLayer = Layer.effectContext(
  Effect.gen(function* () {
    const denied = yield* Ref.make<ReadonlySet<AuthorizedOperation>>(new Set());
    const seen = yield* Ref.make<ReadonlyArray<OperationAuthorizationRequest>>([]);
    const service: OperationAuthorizerService = {
      authorize: (request) =>
        Effect.gen(function* () {
          yield* Ref.update(seen, (all) => [...all, request]);
          const deniedOperations = yield* Ref.get(denied);
          if (deniedOperations.has(request.operation)) {
            return yield* OperationDenied.make({
              operation: request.operation,
              reason: "denied by the test authorization policy",
              ...(request.conversationId === undefined
                ? {}
                : { conversationId: request.conversationId }),
              ...(request.submissionId === undefined ? {} : { submissionId: request.submissionId }),
            });
          }
        }),
    };
    return Context.make(OperationAuthorizer, service).pipe(
      Context.add(
        AuthorizerTestControl,
        AuthorizerTestControl.of({
          deny: (operations) => Ref.set(denied, new Set(operations)),
          reset: Ref.set(denied, new Set<AuthorizedOperation>()).pipe(
            Effect.andThen(Ref.set(seen, [])),
          ),
          requests: Ref.get(seen),
        }),
      ),
    );
  }),
);

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryConversationStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layerTest,
  ToolReconciler.uncertain,
  configLayer,
  authorizerLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layer.pipe(Layer.provideMerge(baseLayer));

const readLog = (conversationId: string) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(
        ConversationRead.make({
          conversationId: decodeConversationId(conversationId),
          limit: 1_024,
        }),
      ),
    );
  });

const armFailpoint = (location: DurableRuntimeFailpointLocation) =>
  Effect.gen(function* () {
    const control = yield* DurableRuntimeFailpointTestControl;
    yield* control.setHandler((hitLocation) =>
      hitLocation === location
        ? Effect.fail(DurableRuntimeFailpointError.make({ location: hitLocation }))
        : Effect.void,
    );
  });

const clearFailpoint = Effect.gen(function* () {
  const control = yield* DurableRuntimeFailpointTestControl;
  yield* control.clear;
});

const resetAuthorizer = Effect.gen(function* () {
  const control = yield* AuthorizerTestControl;
  yield* control.reset;
});

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  const error: unknown = failure.value;
  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

const failureValue = <A, E>(exit: Exit.Exit<A, E>): E => {
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  return failure.value;
};

const encodeSnapshots = Schema.encodeEffect(Schema.Array(RecoverySnapshot));
const encodeEnvelopes = Schema.encodeEffect(Schema.Array(CanonicalRecordEnvelope));

/** Byte-exact durable-state fingerprint: the full canonical log + every recovery snapshot. */
const durableStateFingerprint = (conversationId: string) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const records = yield* readLog(conversationId);
    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
    const snapshots: Array<RecoverySnapshot> = [];
    for (const submission of nonterminal) {
      if (submission.conversationId !== conversationId) continue;
      snapshots.push(
        yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
        ),
      );
    }
    const encodedRecords = yield* encodeEnvelopes([...records]).pipe(Effect.orDie);
    const encodedSnapshots = yield* encodeSnapshots(snapshots).pipe(Effect.orDie);
    return JSON.stringify({ records: encodedRecords, snapshots: encodedSnapshots });
  });

/** Drive one lane into the durable `unknown` state (prepared call, no outcome, recovery). */
const makeUnknownLane = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const scripted = yield* makeScriptedModel((call) =>
      call === 0
        ? toolTurn(toolCall("book-1", "book", { ref: "r-unknown" }))
        : finalParts('{"answer":"never"}'),
    );
    const agent = Agent.withModel(bookDefinition, scripted.model);
    const receipt = yield* runtime.submit(
      agent,
      { question: "book it" },
      submitOptions(conversation, key),
    );
    yield* armFailpoint("tools:after-prepared-append");
    const killed = yield* Effect.exit(
      runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(bookToolLayer)),
    );
    expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
    yield* clearFailpoint;
    const reports = yield* runtime.runRecovery;
    const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
    expect(report?.disposition).toBe("unknown");
    return receipt;
  });

/** Drive one lane into the durable `suspended(ApprovalPending)` state. */
const makeApprovalSuspendedLane = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const scripted = yield* makeScriptedModel((call) =>
      call === 0
        ? toolTurn(toolCall("book-1", "book", { ref: "r-approval" }))
        : finalParts('{"answer":"approved"}'),
    );
    const agent = Agent.withModel(approvalDefinition, scripted.model);
    const receipt = yield* runtime.submit(
      agent,
      { question: "book it" },
      submitOptions(conversation, key),
    );
    const settlements = yield* runtime
      .processConversation(agent, decodeConversationId(conversation))
      .pipe(Effect.provide(approvalToolLayer));
    expect(settlements).toEqual([]);
    return receipt;
  });

/** Run one plain lane to a completed settlement. */
const makeSettledLane = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"done"}'));
    const agent = Agent.withModel(plainDefinition, scripted.model);
    const receipt = yield* runtime.submit(
      agent,
      { question: "answer" },
      submitOptions(conversation, key),
    );
    const settlements = yield* runtime.processConversation(
      agent,
      decodeConversationId(conversation),
    );
    expect(settlements[0]?.outcome).toBe("completed");
    return receipt;
  });

const checkByName = (
  report: IntegrityReport,
  name: IntegrityCheckName,
): { readonly status: string; readonly detail?: string | undefined } => {
  const found = report.checks.find((check) => check.name === name);
  expect(found, `missing integrity check ${name}`).toBeDefined();
  if (found === undefined) throw new Error(`missing integrity check ${name}`);
  return found;
};

layer(testLayer)("DUR-017/SEC-011 P7 administrative operations", (it) => {
  it.effect(
    "explain performs zero writes: the canonical log and ledger rows are byte-identical before and after",
    () =>
      Effect.gen(function* () {
        yield* resetAuthorizer;
        const runtime = yield* DurableAgentRuntime;
        const conversation = "conversation-admin-explain";
        const receipt = yield* makeUnknownLane(conversation, "explain-1");

        const before = yield* durableStateFingerprint(conversation);
        const explanation = yield* runtime.explain(receipt.submissionId);
        const laneExplanations = yield* runtime.explainConversation(
          decodeConversationId(conversation),
        );
        const after = yield* durableStateFingerprint(conversation);
        expect(after).toBe(before);

        expect(explanation.submission.submissionId).toBe(receipt.submissionId);
        expect(explanation.submission.state).toBe("unknown");
        expect(explanation.decision._tag).toBe("AwaitUnknownResolution");
        expect(explanation.disposition).toBe("unknown");
        expect(explanation.decisionMeaning).toBe(RECOVERY_DECISION_MEANINGS.AwaitUnknownResolution);
        expect(explanation.evidence.unknownCalls).toHaveLength(1);
        expect(explanation.evidence.unknownCalls[0]?.toolCallId).toBe("book-1");
        expect(explanation.evidence.unknownCalls[0]?.resolved).toBe(false);
        expect(explanation.submission.ageSeconds).toBeGreaterThanOrEqual(0);

        expect(laneExplanations).toHaveLength(1);
        expect(laneExplanations[0]?.decision._tag).toBe("AwaitUnknownResolution");

        // The pure renderer names the decision, its meaning, and the block for the operator.
        const rendered = renderRecoveryExplanation(explanation);
        expect(rendered).toContain("AwaitUnknownResolution");
        expect(rendered).toContain("unknown outcome: book#book-1");
        expect(rendered).toContain("disposition unknown");
      }),
  );

  it.effect("verify reports typed per-check results and stays honest about the digest chain", () =>
    Effect.gen(function* () {
      yield* resetAuthorizer;
      const runtime = yield* DurableAgentRuntime;
      const conversation = "conversation-admin-verify";
      yield* makeSettledLane(conversation, "verify-1");

      const report = yield* runtime.verify(decodeConversationId(conversation));
      expect(report.ok).toBe(true);
      expect(report.submissionCount).toBe(1);
      expect(checkByName(report, "schema-round-trip").status).toBe("passed");
      expect(checkByName(report, "record-identity").status).toBe("passed");
      expect(checkByName(report, "sequence-contiguity").status).toBe("passed");
      expect(checkByName(report, "fifo-input-order").status).toBe("passed");
      expect(checkByName(report, "fifo-settlement-order").status).toBe("passed");
      expect(checkByName(report, "terminal-uniqueness").status).toBe("passed");
      expect(checkByName(report, "ledger-canonical-agreement").status).toBe("passed");
      // Honest scoping: the port does not export per-batch producer identity, so the runtime
      // operation reports the chain check skipped instead of silently claiming it.
      const digestCheck = checkByName(report, "digest-chain");
      expect(digestCheck.status).toBe("skipped");
      expect(digestCheck.detail).toContain("producer identity");
    }),
  );

  it.effect(
    "verifyConversationInvariants catches an injected digest break and a record-identity duplicate on a corrupted copy",
    () =>
      Effect.gen(function* () {
        yield* resetAuthorizer;
        const conversation = "conversation-admin-corrupt";
        const receipt = yield* makeSettledLane(conversation, "corrupt-1");
        const ledger = yield* SubmissionLedger;
        const store = yield* ConversationStore;

        const exported = yield* store.export(
          ConversationExportRequest.make({
            conversationId: decodeConversationId(conversation),
          }),
        );
        const submissionRow = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: receipt.submissionId }),
        );
        expect(Option.isSome(submissionRow)).toBe(true);
        if (Option.isNone(submissionRow)) throw new Error("Expected the Submission row");
        const submissions = [submissionRow.value];
        // Single-producer lane: the coordinator wrote every batch, so the test KNOWS the
        // per-batch producer directory the port cannot export.
        const batchProducers = new Map<BatchId, ProducerId>(
          exported.records.map((envelope) => [envelope.batchId, PRODUCER_ID]),
        );

        const clean = yield* verifyConversationInvariants({
          export: exported,
          submissions,
          batchProducers,
          requireAllSettled: true,
        });
        expect(clean.ok).toBe(true);
        expect(checkByName(clean, "digest-chain").status).toBe("passed");
        expect(checkByName(clean, "all-settled").status).toBe("passed");

        // Injected digest break: tamper one record's payload content on a copy.
        const tamperedRecords = exported.records.map((envelope) => {
          const payload = envelope.record.payload;
          if (payload._tag !== "UserInputRecorded") return envelope;
          return CanonicalRecordEnvelope.make({
            conversationId: envelope.conversationId,
            batchId: envelope.batchId,
            sequence: envelope.sequence,
            offset: envelope.offset,
            record: RecordEnvelope.make({
              recordId: envelope.record.recordId,
              family: envelope.record.family,
              schemaVersion: envelope.record.schemaVersion,
              createdAt: envelope.record.createdAt,
              deploymentId: envelope.record.deploymentId,
              payload: UserInputRecorded.make({
                submissionId: payload.submissionId,
                kind: payload.kind,
                input: "tampered-by-the-integrity-test",
                ...(payload.runId === undefined ? {} : { runId: payload.runId }),
              }),
            }),
          });
        });
        const digestBroken = yield* verifyConversationInvariants({
          export: ConversationExport.make({
            format: exported.format,
            conversationId: exported.conversationId,
            tailSequence: exported.tailSequence,
            tailDigest: exported.tailDigest,
            records: tamperedRecords,
          }),
          submissions,
          batchProducers,
        });
        expect(digestBroken.ok).toBe(false);
        expect(checkByName(digestBroken, "digest-chain").status).toBe("failed");
        expect(checkByName(digestBroken, "record-identity").status).toBe("passed");

        // Injected record-identity duplicate: the last record reuses the first record's id.
        const first = exported.records[0];
        const last = exported.records.at(-1);
        expect(first).toBeDefined();
        expect(last).toBeDefined();
        if (first === undefined || last === undefined) throw new Error("Expected records");
        const duplicated = [
          ...exported.records.slice(0, -1),
          CanonicalRecordEnvelope.make({
            conversationId: last.conversationId,
            batchId: last.batchId,
            sequence: last.sequence,
            offset: last.offset,
            record: RecordEnvelope.make({
              recordId: first.record.recordId,
              family: last.record.family,
              schemaVersion: last.record.schemaVersion,
              createdAt: last.record.createdAt,
              deploymentId: last.record.deploymentId,
              payload: last.record.payload,
            }),
          }),
        ];
        const identityBroken = yield* verifyConversationInvariants({
          export: ConversationExport.make({
            format: exported.format,
            conversationId: exported.conversationId,
            tailSequence: exported.tailSequence,
            tailDigest: exported.tailDigest,
            records: duplicated,
          }),
          submissions,
          batchProducers,
        });
        expect(identityBroken.ok).toBe(false);
        expect(checkByName(identityBroken, "record-identity").status).toBe("failed");
        expect(checkByName(identityBroken, "record-identity").detail).toContain(
          first.record.recordId,
        );
      }),
  );

  it.effect("retry re-drives exactly one deferred decision with the RepairAnnotated audit", () =>
    Effect.gen(function* () {
      yield* resetAuthorizer;
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const conversation = "conversation-admin-retry";
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"done"}'));
      const agent = Agent.withModel(plainDefinition, scripted.model);

      // Crash between admission and materialization: the lane is admitted, nothing more.
      yield* armFailpoint("submit:after-admit");
      const killed = yield* Effect.exit(
        runtime.submit(agent, { question: "answer" }, submitOptions(conversation, "retry-1")),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      const row = yield* ledger.lookup(
        SubmissionLookupByKey.make({
          conversationId: decodeConversationId(conversation),
          principal: PRINCIPAL,
          idempotencyKey: decodeIdempotencyKey("retry-1"),
        }),
      );
      expect(Option.isSome(row)).toBe(true);
      if (Option.isNone(row)) throw new Error("Expected the admitted Submission");
      expect(row.value.state).toBe("admitted");

      const report = yield* runtime.retry(
        RetryCommand.make({
          submissionId: row.value.submissionId,
          author: "operator",
          reason: "finish the interrupted admission",
        }),
      );
      expect(report.decision._tag).toBe("CompleteMaterialization");
      expect(report.disposition).toBe("repaired");

      // DUR-013: the executed repair carries its deterministic RepairAnnotated audit record.
      const records = yield* readLog(conversation);
      const audit = records.find(
        (envelope) =>
          envelope.record.recordId === `repair:${row.value.submissionId}:CompleteMaterialization`,
      )?.record.payload;
      expect(audit?._tag).toBe("RepairAnnotated");
      if (audit?._tag === "RepairAnnotated") {
        expect(audit.reason).toBe("recovery:CompleteMaterialization");
      }

      // The repaired lane finishes normally.
      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements[0]?.outcome).toBe("completed");
    }),
  );

  it.effect("retry refuses typed for settled, unknown-blocked, and approval-blocked lanes", () =>
    Effect.gen(function* () {
      yield* resetAuthorizer;
      const runtime = yield* DurableAgentRuntime;

      const settled = yield* makeSettledLane("conversation-admin-refuse-settled", "refuse-1");
      const settledExit = yield* Effect.exit(
        runtime.retry(
          RetryCommand.make({
            submissionId: settled.submissionId,
            author: "operator",
            reason: "re-drive settled work",
          }),
        ),
      );
      const settledRefusal = failureValue(settledExit);
      expect(settledRefusal).toMatchObject({ _tag: "RetryRefused", refusal: "settled" });

      const unknown = yield* makeUnknownLane("conversation-admin-refuse-unknown", "refuse-2");
      const unknownExit = yield* Effect.exit(
        runtime.retry(
          RetryCommand.make({
            submissionId: unknown.submissionId,
            author: "operator",
            reason: "re-drive an unknown-blocked lane",
          }),
        ),
      );
      expect(failureValue(unknownExit)).toMatchObject({
        _tag: "RetryRefused",
        refusal: "await-unknown-resolution",
        decisionTag: "AwaitUnknownResolution",
      });

      const approval = yield* makeApprovalSuspendedLane(
        "conversation-admin-refuse-approval",
        "refuse-3",
      );
      const approvalExit = yield* Effect.exit(
        runtime.retry(
          RetryCommand.make({
            submissionId: approval.submissionId,
            author: "operator",
            reason: "re-drive an approval-suspended lane",
          }),
        ),
      );
      expect(failureValue(approvalExit)).toMatchObject({
        _tag: "RetryRefused",
        refusal: "await-approval-decision",
        decisionTag: "AwaitApprovalDecision",
      });
    }),
  );

  it.effect("scanObligations ages and severities deterministically under TestClock", () =>
    Effect.gen(function* () {
      yield* resetAuthorizer;
      const runtime = yield* DurableAgentRuntime;

      // Three obligations at TestClock time zero: an unknown block, an approval suspension,
      // and a ready lane nobody claims.
      const unknown = yield* makeUnknownLane("conversation-admin-age-unknown", "age-1");
      const approval = yield* makeApprovalSuspendedLane("conversation-admin-age-approval", "age-2");
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"queued"}'));
      const agent = Agent.withModel(plainDefinition, scripted.model);
      const ready = yield* runtime.submit(
        agent,
        { question: "wait" },
        submitOptions("conversation-admin-age-ready", "age-3"),
      );

      yield* TestClock.adjust(Duration.seconds(120));
      const thresholds = ObligationThresholds.make({ agingSeconds: 60, overdueSeconds: 600 });
      const report = yield* runtime.scanObligations(thresholds);
      const byId = new Map(report.entries.map((entry) => [entry.submissionId, entry]));

      const unknownEntry = byId.get(unknown.submissionId);
      expect(unknownEntry?.blockedOn).toBe("unknown");
      expect(unknownEntry?.ageSeconds).toBe(120);
      expect(unknownEntry?.severity).toBe("aging");

      const approvalEntry = byId.get(approval.submissionId);
      expect(approvalEntry?.blockedOn).toBe("approval");
      expect(approvalEntry?.ageSeconds).toBe(120);
      expect(approvalEntry?.severity).toBe("aging");

      const readyEntry = byId.get(ready.submissionId);
      expect(readyEntry?.blockedOn).toBe("ready-aged");
      expect(readyEntry?.ageSeconds).toBe(120);
      expect(readyEntry?.severity).toBe("aging");

      yield* TestClock.adjust(Duration.seconds(600));
      const later = yield* runtime.scanObligations(thresholds);
      const laterById = new Map(later.entries.map((entry) => [entry.submissionId, entry]));
      expect(laterById.get(unknown.submissionId)?.ageSeconds).toBe(720);
      expect(laterById.get(unknown.submissionId)?.severity).toBe("overdue");
      expect(laterById.get(ready.submissionId)?.severity).toBe("overdue");
    }),
  );

  it.effect("a non-default authorizer denies every consulted surface fail-closed", () =>
    Effect.gen(function* () {
      yield* resetAuthorizer;
      const runtime = yield* DurableAgentRuntime;
      const control = yield* AuthorizerTestControl;
      const conversation = "conversation-admin-deny";
      const receipt = yield* makeSettledLane(conversation, "deny-1");
      const before = yield* durableStateFingerprint(conversation);

      yield* control.deny([
        "observe",
        "explain",
        "verify",
        "retry",
        "wake",
        "scanObligations",
        "resolveUnknown",
        "resolveApproval",
      ]);

      const explainExit = yield* Effect.exit(runtime.explain(receipt.submissionId));
      expect(failureTag(explainExit)).toBe("OperationDenied");
      const verifyExit = yield* Effect.exit(runtime.verify(decodeConversationId(conversation)));
      expect(failureTag(verifyExit)).toBe("OperationDenied");
      const retryExit = yield* Effect.exit(
        runtime.retry(
          RetryCommand.make({
            submissionId: receipt.submissionId,
            author: "operator",
            reason: "denied",
          }),
        ),
      );
      expect(failureTag(retryExit)).toBe("OperationDenied");
      const wakeExit = yield* Effect.exit(runtime.wake(decodeConversationId(conversation)));
      expect(failureTag(wakeExit)).toBe("OperationDenied");
      const scanExit = yield* Effect.exit(
        runtime.scanObligations(
          ObligationThresholds.make({ agingSeconds: 60, overdueSeconds: 600 }),
        ),
      );
      expect(failureTag(scanExit)).toBe("OperationDenied");
      const observeExit = yield* Effect.exit(Stream.runCollect(runtime.observe(receipt)));
      expect(failureTag(observeExit)).toBe("OperationDenied");
      const resolveExit = yield* Effect.exit(
        runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("book-1"),
            author: "operator",
            reason: "denied",
            resolution: ResolutionNeverHappened.make(),
          }),
        ),
      );
      expect(failureTag(resolveExit)).toBe("OperationDenied");
      const approvalExit = yield* Effect.exit(
        runtime.resolveApproval(
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("book-1"),
            decision: "approved",
            resolver: "operator",
            reason: "denied",
          }),
        ),
      );
      expect(failureTag(approvalExit)).toBe("OperationDenied");

      // Fail-closed means fail-before-effect: nothing was read into a repair, nothing written.
      const after = yield* durableStateFingerprint(conversation);
      expect(after).toBe(before);
      const requests = yield* control.requests;
      expect(requests.map((request) => request.operation)).toEqual([
        "explain",
        "verify",
        "retry",
        "wake",
        "scanObligations",
        "observe",
        "resolveUnknown",
        "resolveApproval",
      ]);

      // The denial policy lifts and the default possession behavior is restored.
      yield* control.reset;
      const explanation = yield* runtime.explain(receipt.submissionId);
      expect(explanation.decision._tag).toBe("NoAction");
      expect(explanation.disposition).toBe("none");
    }),
  );

  it.effect("keeps the admin failure channels typed (E proofs)", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const conversationId = decodeConversationId("conversation-admin-types");
      const receipt = yield* makeSettledLane("conversation-admin-types", "types-1");

      const explainEffect: Effect.Effect<RecoveryExplanation, DurableExplainFailure> =
        runtime.explain(receipt.submissionId);
      const explainLane: Effect.Effect<
        ReadonlyArray<RecoveryExplanation>,
        DurableExplainFailure
      > = runtime.explainConversation(conversationId);
      const verifyEffect: Effect.Effect<IntegrityReport, DurableVerifyFailure> =
        runtime.verify(conversationId);
      const retryEffect: Effect.Effect<RecoveryReport, DurableRetryFailure> = runtime.retry(
        RetryCommand.make({ submissionId: receipt.submissionId, author: "a", reason: "b" }),
      );
      const wakeEffect: Effect.Effect<void, OperationDenied> = runtime.wake(conversationId);
      const scanEffect: Effect.Effect<ObligationReport, DurableObligationFailure> =
        runtime.scanObligations(ObligationThresholds.make({ agingSeconds: 1, overdueSeconds: 2 }));

      // Execute the read-only members to keep the proof honest at runtime too.
      yield* explainEffect;
      yield* explainLane;
      yield* verifyEffect;
      yield* wakeEffect;
      yield* scanEffect;
      const retryExit = yield* Effect.exit(retryEffect);
      expect(failureTag(retryExit)).toBe("RetryRefused");
    }),
  );
});
