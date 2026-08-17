import {
  Redactor,
  StructuralRedactorLive,
  SubagentExecutionFailure,
} from "@effect-agent/capabilities";
import { ConversationId, ToolCallId } from "@effect-agent/core";
import { NodeDurableRuntime, type NodeDurableRuntimeOptions } from "@effect-agent/platform-node";
import {
  ConversationRead,
  ConversationStore,
  DurableAgentRuntime,
  IdempotencyKey,
  childConversationIdFor,
  runIdForSubmission,
  type CanonicalRecordEnvelope,
} from "@effect-agent/session";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, type PlatformError, Schema, Stream } from "effect";

import { TrustedLocalDurableAuthorizationLayer } from "../../src/durable-test-authorization.ts";
import {
  docsCoordinatorConfidentialMarker,
  docsDocumentBodySecret,
  docsMissionConfidentialMarker,
  docsResearcherDeploymentId,
  docsResearcherProducerId,
  docsResearcherSubmitAgent,
  docsResearcherSubmitOptions,
  documentBodyPhrase,
  documentSummaryFor,
  makeDocsResearcherHarness,
  researchCorpusDocumentIds,
  researchMissionRequest,
  summarizeCallId,
} from "../../src/index.ts";

// ---------------------------------------------------------------------------
// Red-team suite: CHILD EXFILTRATION THROUGH THE DURABLE JOIN (P7 WP5, plan §4;
// spec/subagents.md §11/§15, S2 D5; testing.md §10 "child failure, progress,
// and provenance payloads containing secret-bearing values"; SEC-008/SUB-015).
//
// The threat: a Subagent child holds secrets in its OWN Conversation (fetched
// document bodies, internal working notes) and, if compromised or
// prompt-injected, tries to smuggle them across the delegation boundary into
// the parent — through the successful join, a failure payload, or a raw Cause.
// The framework's declassification boundary is `projectResult` on success and a
// bounded `{errorTag, message}` / `SubagentExecutionFailure` projection on
// failure; NO raw Cause, stack, or child payload crosses.
//
// This suite drives a real DN durable delegation and asserts the exfiltration
// invariants end-to-end, then proves at the Schema level that the durable
// failure projection cannot carry an unbounded raw Cause, and that the
// structural Redactor strips secret scalars from a child failure/progress
// payload preview.
// ---------------------------------------------------------------------------

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableRuntimeOptions>,
): NodeDurableRuntimeOptions => ({
  filename,
  deploymentId: docsResearcherDeploymentId,
  producerId: docsResearcherProducerId,
  observationPollInterval: 1,
  ...overrides,
});

const withTemporaryDirectory = <A, E>(
  use: (directory: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-redteam-exfiltration-",
      });
      return yield* use(directory);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const submitMission = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    return yield* runtime.submit(
      docsResearcherSubmitAgent,
      researchMissionRequest,
      docsResearcherSubmitOptions(decodeConversationId(conversation), decodeIdempotencyKey(key)),
    );
  });

const drive = (conversationId: ConversationId) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    yield* runtime.runRecovery;
    return yield* runtime.processConversationResolved(conversationId);
  });

const readLog = (conversationId: ConversationId) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(ConversationRead.make({ conversationId, limit: 1_024 })),
    );
  });

const payloadsOf = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tag: string,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  records.filter((envelope) => envelope.record.payload._tag === tag);

describe("SUB-015 durable child exfiltration resistance (DN)", () => {
  it.effect(
    "the child's fetched-body secret and raw working notes never cross the delegation boundary into the parent — only the bounded declared summary does",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDocsResearcherHarness().pipe(Effect.provide(NodeCrypto.layer));
          yield* Effect.gen(function* () {
            const receipt = yield* submitMission("redteam-exfiltration", "redteam-exfil-1");
            const documents = researchCorpusDocumentIds;
            const parentRunId = runIdForSubmission(receipt.submissionId);
            const childConversations = documents.map((documentId) =>
              childConversationIdFor(
                receipt.submissionId,
                decodeToolCallId(summarizeCallId(documentId)),
              ),
            );

            // Establish, run each child, and join — the full durable delegation.
            yield* drive(receipt.conversationId);
            for (const childConversationId of childConversations) {
              const settlements = yield* drive(childConversationId);
              expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            }
            const settlements = yield* drive(receipt.conversationId);
            expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);

            // The child DID read the secret: each child log holds the fetched body verbatim.
            for (const [index, childConversationId] of childConversations.entries()) {
              const documentId = documents[index] ?? "";
              const childLogJson = JSON.stringify(
                (yield* readLog(childConversationId)).map((envelope) => envelope.record.payload),
              );
              expect(childLogJson).toContain(docsDocumentBodySecret);
              expect(childLogJson).toContain(documentBodyPhrase(documentId));
            }

            // The parent NEVER saw it: no body secret, no raw body phrase, not in the log, not
            // in any coordinator prompt, and not in the final settlement result.
            const parentLog = yield* readLog(receipt.conversationId);
            const parentLogJson = JSON.stringify(
              parentLog.map((envelope) => envelope.record.payload),
            );
            expect(parentLogJson).not.toContain(docsDocumentBodySecret);
            for (const prompt of yield* harness.parentPrompts) {
              expect(prompt).not.toContain(docsDocumentBodySecret);
            }
            for (const documentId of documents) {
              expect(parentLogJson).not.toContain(documentBodyPhrase(documentId));
            }

            // Only the bounded DECLARED summary crossed — each join carries exactly the
            // projected finding, nothing more (SUB-015 declassification).
            for (const documentId of documents) {
              const joinSettle = parentLog.find(
                (envelope) =>
                  envelope.record.recordId ===
                  `tool-settled:${parentRunId}:1:${summarizeCallId(documentId)}`,
              )?.record.payload;
              expect(
                joinSettle?._tag === "ToolCallSettled" ? joinSettle.result : undefined,
              ).toEqual({
                documentId,
                summary: documentSummaryFor(documentId).summary,
              });
            }
            expect(payloadsOf(parentLog, "SubagentJoined")).toHaveLength(documents.length);

            // And the mission/coordinator secrets never leaked DOWN into a child either.
            for (const childConversationId of childConversations) {
              const childLogJson = JSON.stringify(
                (yield* readLog(childConversationId)).map((envelope) => envelope.record.payload),
              );
              expect(childLogJson).not.toContain(docsMissionConfidentialMarker);
              expect(childLogJson).not.toContain(docsCoordinatorConfidentialMarker);
            }
          }).pipe(
            Effect.provide(
              NodeDurableRuntime.layer(
                runtimeOptions(`${directory}/redteam-exfiltration.sqlite`, {
                  bindings: harness.bindings,
                }),
              ).pipe(Layer.provide(TrustedLocalDurableAuthorizationLayer)),
            ),
          );
        }),
      ),
  );
});

const SECRET = "child-cause-secret-ZZZ-do-not-leak";
const decodeExecutionFailure = Schema.decodeUnknownEffect(SubagentExecutionFailure);

/** A well-formed durable child-failure projection, with the fields a real join would carry. */
const validFailurePayload = {
  _tag: "SubagentExecutionFailure",
  delegationId: "delegate_document_summary",
  targetAgentId: "doc-summarizer",
  classification: "child-failed",
  errorTag: "DocumentSummaryFailed",
  message: "the child Run failed",
} as const;

describe("S2-D5 the durable failure projection cannot smuggle a raw Cause or unbounded payload", () => {
  it.effect(
    "SubagentExecutionFailure has no Cause/stack field: an injected raw-Cause key is dropped on decode",
    () =>
      Effect.gen(function* () {
        // An attacker-shaped join payload tries to ride a raw Cause, a stack trace, and a child
        // payload alongside the declared fields.
        const attackerShaped = {
          ...validFailurePayload,
          cause: { secret: SECRET, stack: `Error: ${SECRET}\n  at child` },
          childPayload: { apiKey: SECRET },
          stack: `Error: ${SECRET}`,
        };
        const decoded = yield* decodeExecutionFailure(attackerShaped);
        // Only the declared, bounded fields survive; no channel exists for the smuggled data.
        expect(decoded.errorTag).toBe("DocumentSummaryFailed");
        expect(Object.keys(decoded)).not.toContain("cause");
        expect(Object.keys(decoded)).not.toContain("childPayload");
        expect(Object.keys(decoded)).not.toContain("stack");
        const reEncoded = JSON.stringify(
          yield* Schema.encodeEffect(SubagentExecutionFailure)(decoded),
        );
        expect(reEncoded).not.toContain(SECRET);
      }),
  );

  it.effect("the bounded errorTag and message reject an over-length secret-bearing payload", () =>
    Effect.gen(function* () {
      // errorTag is bounded at 256 bytes; a child that stuffs a long secret into it is rejected
      // fail-closed rather than persisting an unbounded value on the canonical join.
      const oversizedTag = yield* Effect.flip(
        decodeExecutionFailure({
          ...validFailurePayload,
          errorTag: `${SECRET}-${"x".repeat(512)}`,
        }),
      );
      expect(oversizedTag._tag).toBe("SchemaError");

      // message is bounded at 4096 bytes; the same fail-closed rejection applies.
      const oversizedMessage = yield* Effect.flip(
        decodeExecutionFailure({
          ...validFailurePayload,
          message: `${SECRET} ${"y".repeat(8192)}`,
        }),
      );
      expect(oversizedMessage._tag).toBe("SchemaError");
    }),
  );

  it.effect(
    "the structural Redactor strips every secret scalar from a child failure/progress preview (SEC-008)",
    () =>
      Effect.gen(function* () {
        const redactor = yield* Redactor;
        // A secret-bearing child failure + progress payload as it would appear in an event/span.
        const childEventPayload = {
          _tag: "ToolCallFailed",
          errorTag: "DocumentSummaryFailed",
          message: `internal note: ${SECRET}`,
          progress: { note: SECRET, fetchedBody: `amber-ledger-passage ${SECRET}` },
        };
        const preview = yield* redactor.redact(childEventPayload);
        expect(preview).not.toContain(SECRET);
        expect(preview).not.toContain("amber-ledger-passage");
        // Shape survives for the reviewer; scalars become type markers.
        expect(preview).toContain("[REDACTED:string]");
      }).pipe(Effect.provide(StructuralRedactorLive)),
  );
});
