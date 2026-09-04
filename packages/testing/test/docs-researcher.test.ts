import { connectMcp } from "@effect-agent/capabilities/Mcp";
import { StructuralRedactorLive } from "@effect-agent/capabilities/Redaction";
import { ThreadId, ToolCallId, type SubmissionId } from "@effect-agent/core/Identifiers";
import {
  NodeDurableAgentRuntime,
  type NodeDurableAgentRuntimeOptions,
} from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import {
  assertDiscoveryMatchesAuthoredToolkit,
  docsCoordinatorConfidentialMarker,
  docsDocumentBodySecret,
  docsMcpConnectorLayer,
  docsMcpMismatchedConnectorLayer,
  docsMcpOversizedConnectorLayer,
  docsMcpRequest,
  docsMissionConfidentialMarker,
  docsResearcherDeploymentId,
  docsResearcherProducerId,
  docsResearcherSubmitAgent,
  docsResearcherSubmitOptions,
  documentBodyPhrase,
  documentSummaryFor,
  expectedResearchDigest,
  fetchCallId,
  makeDocsResearcherHarness,
  redactedDocumentPreview,
  ResearchDigest,
  researchCorpusDocumentIds,
  researchMissionRequest,
  summarizeCallId,
} from "@effect-agent/testing/DocsResearcher";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { ProducerId, type CanonicalRecordEnvelope } from "@effect-agent/thread/Records";
import { childThreadIdFor, runIdForSubmission } from "@effect-agent/thread/RunJournal";
import {
  ClaimRequest,
  IdempotencyKey,
  SubmissionLedger,
  SubmissionLookupById,
} from "@effect-agent/thread/SubmissionLedger";
import { ThreadRead, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Effect, FileSystem, Option, Schema, Stream } from "effect";

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodeProducerId = Schema.decodeSync(ProducerId);
const decodeDigest = Schema.decodeUnknownEffect(ResearchDigest);

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
        prefix: "effect-agent-docs-researcher-",
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

/** Drive one Thread lane through the S2 multi-binding worker entry point. */
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

const payloadsOf = <Tag extends string>(
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tag: Tag,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  records.filter((envelope) => envelope.record.payload._tag === tag);

const submissionState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");

    return snapshot.value;
  });

describe("CAP-009 docs-researcher MCP content discovery", () => {
  it.effect(
    "binds the summarizer's authored toolkit to bounded discovery and fails closed on over-limit or drifted contracts",
    () =>
      Effect.gen(function* () {
        // Happy path: bounded discovery, digest bound, authored-toolkit match.
        const connection = yield* Effect.scoped(
          Effect.gen(function* () {
            const opened = yield* connectMcp(docsMcpRequest);

            yield* assertDiscoveryMatchesAuthoredToolkit(opened);

            return opened;
          }),
        ).pipe(Effect.provide(docsMcpConnectorLayer));

        expect(connection.discovery.identity.serverId).toBe("docs-content-mcp");
        expect(connection.discovery.tools).toHaveLength(1);
        expect(connection.discovery.tools[0]?.name).toBe("fetch_document");
        expect(connection.discovery.toolkitSchemaDigest).toMatch(/^sha256:[a-f0-9]{64}$/);
        expect(connection.discovery.encodedBytes).toBeLessThanOrEqual(
          docsMcpRequest.maxDiscoveryBytes,
        );

        // A tool description over the requested byte bound fails closed (SEC-013).
        const oversized = yield* Effect.scoped(connectMcp(docsMcpRequest)).pipe(
          Effect.provide(docsMcpOversizedConnectorLayer),
          Effect.flip,
        );

        expect(oversized).toMatchObject({
          _tag: "McpDiscoveryLimitExceeded",
          limit: "tool-description-bytes",
          limitValue: docsMcpRequest.maxToolDescriptionBytes,
        });

        // A served schema that drifted from the authored toolkit never reaches
        // Binding registration: the digest comparison fails closed.
        const drifted = yield* Effect.scoped(connectMcp(docsMcpRequest)).pipe(
          Effect.provide(docsMcpMismatchedConnectorLayer),
          Effect.flip,
        );

        expect(drifted._tag).toBe("McpToolkitMismatch");
      }).pipe(Effect.provide(NodeCrypto.layer)),
  );
});

describe("SUB-030 docs-researcher durable delegation (DN)", () => {
  it.effect(
    "delegates per-document summarization to doc-summarizer children over SQLite: MCP-gated bindings, waitingForChild without a permit, bounded summaries joined, secrets isolated and previews redacted",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDocsResearcherHarness().pipe(Effect.provide(NodeCrypto.layer));

          expect(harness.discovery.tools.map((tool) => tool.name)).toEqual(["fetch_document"]);

          yield* Effect.gen(function* () {
            const receipt = yield* submitMission("docs-researcher-happy", "docs-happy-1");
            const parentRunId = runIdForSubmission(receipt.submissionId);
            const documents = researchCorpusDocumentIds;

            const childThreads = documents.map((documentId) =>
              childThreadIdFor(receipt.submissionId, decodeToolCallId(summarizeCallId(documentId))),
            );

            // Phase 1: one coordinator Turn declares BOTH delegation calls; the
            // parent suspends waitingForChild, holds no worker permit, and no
            // child model ran (spec §12 step 10, SUB-030).
            const first = yield* drive(receipt.threadId);

            expect(first).toHaveLength(0);
            expect((yield* submissionState(receipt.submissionId)).state).toBe("suspended");
            const ledger = yield* SubmissionLedger;

            const claimed = yield* ledger.claim(
              ClaimRequest.make({
                threadId: receipt.threadId,
                producerId: decodeProducerId("docs-researcher-probe"),
              }),
            );

            expect(Option.isNone(claimed)).toBe(true);
            expect(yield* harness.childModelCalls).toBe(0);
            const afterEstablish = yield* readLog(receipt.threadId);

            expect(payloadsOf(afterEstablish, "SubagentRequested")).toHaveLength(documents.length);
            expect(payloadsOf(afterEstablish, "SubagentStarted")).toHaveLength(documents.length);

            // Phase 2: each child lane runs to Settlement under its own Attempt.
            // Every summarizer consulted the MCP content tool exactly once.
            for (const childThreadId of childThreads) {
              const settlements = yield* drive(childThreadId);

              expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            }
            for (const documentId of documents) {
              expect(yield* harness.fetchInvocations(documentId)).toBe(1);
            }
            expect(yield* harness.childModelCalls).toBe(documents.length * 2);

            // Context isolation (SUB-006/SUB-015): each child saw only its
            // projected brief — never the mission or coordinator markers.
            for (const prompt of yield* harness.childPrompts) {
              expect(prompt).toContain("summarize:durability-claims");
              expect(prompt).not.toContain(docsMissionConfidentialMarker);
              expect(prompt).not.toContain(docsCoordinatorConfidentialMarker);
            }

            // Phase 3: the woken parent joins BOTH verified settlements and
            // settles completed with the digest of bounded findings.
            const settlements = yield* drive(receipt.threadId);

            expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            const log = yield* readLog(receipt.threadId);

            expect(payloadsOf(log, "SubagentJoined")).toHaveLength(documents.length);
            const settled = payloadsOf(log, "SubmissionSettled")[0]?.record.payload;

            if (settled?._tag !== "SubmissionSettled")
              throw new Error("Expected SubmissionSettled");
            expect(yield* decodeDigest(settled.result)).toEqual(expectedResearchDigest(documents));
            for (const documentId of documents) {
              const joinSettle = log.find(
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

            // Redaction and declassification (SEC-008, SUB-015): the parent
            // Thread and coordinator prompts carry the bounded summary
            // but never the body secret or the raw body phrases; each child's
            // own log holds the fetched body — with the secret — exactly once;
            // and the audit-surface preview of a raw document is structurally
            // redacted end to end.
            const parentLogJson = JSON.stringify(log.map((envelope) => envelope.record.payload));

            expect(parentLogJson).not.toContain(docsDocumentBodySecret);
            const finalPrompt = (yield* harness.parentPrompts).at(-1);

            expect(finalPrompt).toBeDefined();
            for (const documentId of documents) {
              expect(parentLogJson).not.toContain(documentBodyPhrase(documentId));
              expect(finalPrompt).toContain(documentSummaryFor(documentId).summary);
              expect(finalPrompt).not.toContain(documentBodyPhrase(documentId));
            }
            expect(finalPrompt).not.toContain(docsDocumentBodySecret);
            for (const [index, childThreadId] of childThreads.entries()) {
              const documentId = documents[index] ?? "";
              const childLog = yield* readLog(childThreadId);

              const childLogJson = JSON.stringify(
                childLog.map((envelope) => envelope.record.payload),
              );

              expect(childLogJson).toContain(docsDocumentBodySecret);
              expect(childLogJson).toContain(fetchCallId(documentId));

              const preview = yield* redactedDocumentPreview(documentId).pipe(
                Effect.provide(StructuralRedactorLive),
              );

              expect(preview).toContain("[REDACTED:string]");
              expect(preview).not.toContain(docsDocumentBodySecret);
              expect(preview).not.toContain(documentBodyPhrase(documentId));
            }
            expect(yield* harness.parentModelCalls).toBe(2);
          }).pipe(
            Effect.provide(
              NodeDurableAgentRuntime.layerWithBindings(
                harness.bindings,
                runtimeOptions(`${directory}/docs-researcher.sqlite`),
              ),
            ),
          );
        }),
      ),
  );
});
