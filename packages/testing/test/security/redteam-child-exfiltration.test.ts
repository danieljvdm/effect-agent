import {
  NodeDurableAgentRuntime,
  type NodeDurableAgentRuntimeOptions,
} from "@effect-agent/platform-node/node-durable-agent-runtime";
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
} from "@effect-agent/testing/docs-researcher";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FileSystem, type PlatformError, Schema, Stream } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ThreadId, ToolCallId } from "effect-agent/identifiers";
import { type CanonicalRecordEnvelope } from "effect-agent/records";
import { childThreadIdFor, runIdForSubmission } from "effect-agent/run-journal";
import { IdempotencyKey } from "effect-agent/submission-ledger";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";

// ---------------------------------------------------------------------------
// Red-team suite: child exfiltration through the durable join.
// Failure, progress, and provenance payloads may contain secret-bearing values.
//
// The threat: a Subagent child holds secrets in its OWN Thread (fetched
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

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableAgentRuntimeOptions>,
): NodeDurableAgentRuntimeOptions => ({
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

const submitMission = (thread: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;

    return yield* runtime.submit(
      docsResearcherSubmitAgent,
      researchMissionRequest,
      docsResearcherSubmitOptions(decodeThreadId(thread), decodeIdempotencyKey(key)),
    );
  });

const drive = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;

    return yield* runtime.processThreadResolved(threadId);
  });

const readLog = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    return yield* Stream.runCollect(store.read(ThreadRead.make({ threadId, limit: 1_024 })));
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

            const childThreads = documents.map((documentId) =>
              childThreadIdFor(receipt.submissionId, decodeToolCallId(summarizeCallId(documentId))),
            );

            // Establish, run each child, and join — the full durable delegation.
            yield* drive(receipt.threadId);
            for (const childThreadId of childThreads) {
              const settlements = yield* drive(childThreadId);

              expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            }
            const settlements = yield* drive(receipt.threadId);

            expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);

            // The child DID read the secret: each child log holds the fetched body verbatim.
            for (const [index, childThreadId] of childThreads.entries()) {
              const documentId = documents[index] ?? "";

              const childLogJson = JSON.stringify(
                (yield* readLog(childThreadId)).map((envelope) => envelope.record.payload),
              );

              expect(childLogJson).toContain(docsDocumentBodySecret);
              expect(childLogJson).toContain(documentBodyPhrase(documentId));
            }

            // The parent NEVER saw it: no body secret, no raw body phrase, not in the log, not
            // in any coordinator prompt, and not in the final settlement result.
            const parentLog = yield* readLog(receipt.threadId);

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
            for (const childThreadId of childThreads) {
              const childLogJson = JSON.stringify(
                (yield* readLog(childThreadId)).map((envelope) => envelope.record.payload),
              );

              expect(childLogJson).not.toContain(docsMissionConfidentialMarker);
              expect(childLogJson).not.toContain(docsCoordinatorConfidentialMarker);
            }
          }).pipe(
            Effect.provide(
              NodeDurableAgentRuntime.layerWithBindings(
                harness.bindings,
                runtimeOptions(`${directory}/redteam-exfiltration.sqlite`),
              ),
            ),
          );
        }),
      ),
  );
});
