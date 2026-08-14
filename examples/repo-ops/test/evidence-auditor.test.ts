import { OpenAiClient } from "@effect/ai-openai";
import { NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Config,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  PlatformError,
  Schema,
  Stream,
} from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import * as path from "node:path";

import { Agent, ConversationId, ToolCallId, type SubmissionId } from "@effect-agent/core";
import { NodeDurableRuntime, type NodeDurableRuntimeOptions } from "@effect-agent/platform-node";
import { layer as LocalSandboxLayer } from "@effect-agent/sandbox-local";
import {
  ApprovalDecisionCommand,
  ConversationRead,
  ConversationStore,
  DurableAgentRuntime,
  IdempotencyKey,
  SubmissionLedger,
  SubmissionLookupById,
  runIdForSubmission,
  toolStepSettledRecordId,
  type CanonicalRecordEnvelope,
  type Receipt,
} from "@effect-agent/session";
import { phase7LiveProfileEnabled } from "@effect-agent/testing";

import {
  AUDIT_REPORT_PATH,
  AuditMission,
  AuditReportSink,
  EvidenceAuditor,
  EvidenceAuditReport,
  EvidenceAuditorProfile,
  EvidenceAuditorToolkitLayer,
  evidenceAuditorProfile,
  expectedOfflineReport,
  extractCitedTestTitles,
  makeOfflineAuditorModel,
  normalizeRepoRelativePath,
  OFFLINE_AUDIT_CALL_ID,
  OFFLINE_WRITE_CALL_ID,
  offlineReportSummary,
  OpenAiEvidenceAuditor,
  RepoOpsWorkspace,
  repoOpsDeploymentId,
  repoOpsProducerId,
  repoOpsSubmitOptions,
} from "../src/index.ts";

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodeReport = Schema.decodeUnknownEffect(EvidenceAuditReport);

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableRuntimeOptions>,
): NodeDurableRuntimeOptions => ({
  filename,
  deploymentId: repoOpsDeploymentId,
  producerId: repoOpsProducerId,
  observationPollInterval: 1,
  ...overrides,
});

const withTemporaryDirectory = <A, E>(
  use: (directory: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "effect-agent-repo-ops-" });
      return yield* use(directory);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

// ---------------------------------------------------------------------------
// Deterministic fixture repository tree: two evidence documents citing three
// test titles, of which exactly one is intentionally absent from the tree.
// ---------------------------------------------------------------------------

const PRESENT_TITLE_A = "keeps the ledger honest";
const PRESENT_TITLE_B = "settles aborted work without waiting";
const MISSING_TITLE = "this test was renamed long ago";

const FIXTURE_DOCUMENTS = ["docs/EVIDENCE-A.md", "docs/EVIDENCE-B.md"] as const;

const writeFixtureTree = (root: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.makeDirectory(`${root}/docs`, { recursive: true });
    yield* fs.makeDirectory(`${root}/packages/notes/test`, { recursive: true });
    yield* fs.writeFileString(
      `${root}/docs/EVIDENCE-A.md`,
      [
        "# Evidence A",
        "",
        `Proven by (\`packages/notes/test/notes.test.ts\` — “${PRESENT_TITLE_A}”).`,
        `Also cites (\`packages/notes/test/notes.test.ts\` — “${MISSING_TITLE}”).`,
      ].join("\n"),
    );
    yield* fs.writeFileString(
      `${root}/docs/EVIDENCE-B.md`,
      [
        "# Evidence B",
        "",
        `Pinned by (\`packages/notes/test/notes.test.ts\` — “${PRESENT_TITLE_B}”).`,
      ].join("\n"),
    );
    yield* fs.writeFileString(
      `${root}/packages/notes/test/notes.test.ts`,
      [
        'describe("notes", () => {',
        `  it("${PRESENT_TITLE_A}", () => {});`,
        `  it("${PRESENT_TITLE_B}", () => {});`,
        "});",
        "",
      ].join("\n"),
    );
  }).pipe(Effect.provide(NodeFileSystem.layer));

const workerLayerFor = (workspace: {
  readonly root: string;
  readonly searchRoots: ReadonlyArray<string>;
  readonly reportRoot: string;
}) => {
  const workspaceLayer = RepoOpsWorkspace.layer(workspace);
  const sinkLayer = AuditReportSink.layerFileSystem.pipe(
    Layer.provide(Layer.merge(NodeFileSystem.layer, workspaceLayer)),
  );
  return EvidenceAuditorToolkitLayer.pipe(
    Layer.provideMerge(Layer.mergeAll(LocalSandboxLayer, sinkLayer, workspaceLayer)),
  );
};

const readLog = (conversationId: ConversationId) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(ConversationRead.make({ conversationId, limit: 1_024 })),
    );
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

const settledReport = (records: ReadonlyArray<CanonicalRecordEnvelope>) =>
  Effect.gen(function* () {
    const settled = payloadsOf(records, "SubmissionSettled")[0]?.record.payload;
    if (settled?._tag !== "SubmissionSettled") throw new Error("Expected SubmissionSettled");
    expect(settled.outcome).toBe("completed");
    return yield* decodeReport(settled.result);
  });

describe("CAP-010 evidence auditor offline profile (DN)", () => {
  it("pins the profile claim and its schema round-trip", () => {
    const decoded = Schema.decodeUnknownSync(EvidenceAuditorProfile)(
      Schema.encodeSync(EvidenceAuditorProfile)(evidenceAuditorProfile),
    );
    expect(decoded).toEqual(evidenceAuditorProfile);
    expect(evidenceAuditorProfile).toEqual({
      deploymentClass: "DN",
      durableStepPerDocument: true,
      approvalGatedReportWrite: true,
      sandboxIsolation: "unisolated",
      liveProfileOptIn: true,
      exactlyOnceExternalEffects: false,
    });
  });

  it.effect("normalizes model-supplied paths fail-closed (SEC-007)", () =>
    Effect.gen(function* () {
      expect(yield* normalizeRepoRelativePath("docs/EVIDENCE-A.md")).toBe("docs/EVIDENCE-A.md");
      for (const hostile of [
        "/etc/passwd",
        "../outside.md",
        "docs/../../outside.md",
        "docs//gap.md",
        "docs\\windows.md",
        "C:evil.md",
        "docs/./here.md",
      ]) {
        const exit = yield* Effect.exit(normalizeRepoRelativePath(hostile));
        expect(Exit.isFailure(exit)).toBe(true);
      }
    }),
  );

  it("extracts curly-quoted cited test titles deterministically", () => {
    const text =
      "cites (`a/b.test.ts` — “first title”) and (`c/d.test.ts` — “second title”) and “first title” again.";
    expect(extractCitedTestTitles(text)).toEqual(["first title", "second title"]);
    expect(extractCitedTestTitles("no citations here")).toEqual([]);
  });

  it.effect(
    "audits a fixture tree as accepted work: read-only sandbox commands, one durable step per document, approval-gated report write, completed settlement",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const root = `${directory}/workspace`;
          yield* writeFixtureTree(root);
          const documents = [...FIXTURE_DOCUMENTS];
          const missing = [MISSING_TITLE];
          const scripted = yield* makeOfflineAuditorModel(documents, missing);
          const agent = Agent.withModel(EvidenceAuditor, scripted.model);
          const workerLayer = workerLayerFor({
            root,
            searchRoots: ["packages"],
            reportRoot: root,
          });
          const reportFile = `${root}/${AUDIT_REPORT_PATH}`;

          yield* Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem;
            const runtime = yield* DurableAgentRuntime;
            const conversation = decodeConversationId("repo-ops-audit-happy");
            const receipt: Receipt = yield* runtime.submit(
              agent,
              AuditMission.make({ evidenceDocuments: documents }),
              repoOpsSubmitOptions(conversation, decodeIdempotencyKey("repo-ops-happy-1")),
            );
            const runId = runIdForSubmission(receipt.submissionId);

            // Drive 1: the audit runs (read-only ls/cat/grep through the
            // sandbox, one named durable step per document) and the report
            // write suspends durably on its canonical approval request.
            const first = yield* runtime
              .processConversation(agent, conversation)
              .pipe(Effect.provide(workerLayer));
            expect(first).toHaveLength(0);
            expect((yield* submissionState(receipt.submissionId)).state).toBe("suspended");
            expect(yield* fs.exists(reportFile)).toBe(false);

            const suspendedLog = yield* readLog(conversation);
            const stepIds = documents.map((document) =>
              toolStepSettledRecordId(
                runId,
                decodeToolCallId(OFFLINE_AUDIT_CALL_ID),
                `audit:${document}`,
              ),
            );
            const recordIds = suspendedLog.map((envelope) => envelope.record.recordId as string);
            for (const stepId of stepIds) {
              expect(recordIds).toContain(stepId);
            }
            expect(payloadsOf(suspendedLog, "ToolStepSettled")).toHaveLength(documents.length);
            expect(payloadsOf(suspendedLog, "ToolApprovalRequested")).toHaveLength(1);

            // The audit tool's canonical settlement carries the verified rows:
            // the missing citation is data, not an error.
            const auditSettled = suspendedLog.find(
              (envelope) =>
                envelope.record.recordId === `tool-settled:${runId}:1:${OFFLINE_AUDIT_CALL_ID}`,
            )?.record.payload;
            if (auditSettled?._tag !== "ToolCallSettled") {
              throw new Error("Expected the audit ToolCallSettled record");
            }
            expect(auditSettled.result).toEqual({
              rows: [
                {
                  document: "docs/EVIDENCE-A.md",
                  citedTests: [PRESENT_TITLE_A, MISSING_TITLE],
                  missingTests: [MISSING_TITLE],
                },
                {
                  document: "docs/EVIDENCE-B.md",
                  citedTests: [PRESENT_TITLE_B],
                  missingTests: [],
                },
              ],
            });

            // Approve: the resumed batch replays the declared write without a
            // model call, the sink writes exactly once, the Run settles.
            yield* runtime.resolveApproval(
              ApprovalDecisionCommand.make({
                submissionId: receipt.submissionId,
                toolCallId: decodeToolCallId(OFFLINE_WRITE_CALL_ID),
                decision: "approved",
                resolver: "repo-ops-operator",
                reason: "The audit rows were reviewed.",
              }),
            );
            const settlements = yield* runtime
              .processConversation(agent, conversation)
              .pipe(Effect.provide(workerLayer));
            expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);

            const log = yield* readLog(conversation);
            expect(yield* settledReport(log)).toEqual(expectedOfflineReport(documents, missing));
            const decision = payloadsOf(log, "ToolApprovalDecided")[0]?.record.payload;
            if (decision?._tag !== "ToolApprovalDecided") {
              throw new Error("Expected ToolApprovalDecided");
            }
            expect(decision.decision).toBe("approved");

            // The approved report exists with the exact deterministic content.
            expect(yield* fs.exists(reportFile)).toBe(true);
            const written: unknown = JSON.parse(yield* fs.readFileString(reportFile));
            expect(written).toEqual({
              reportPath: AUDIT_REPORT_PATH,
              summary: offlineReportSummary(documents, missing),
              missingTests: missing,
            });

            // Exactly three model requests ever: audit turn, write turn, and
            // the final report turn after the approved resume.
            expect(yield* scripted.calls).toBe(3);
          }).pipe(
            Effect.provide(
              Layer.merge(
                NodeDurableRuntime.layer(runtimeOptions(`${directory}/audit.sqlite`)),
                NodeFileSystem.layer,
              ),
            ),
          );
        }),
      ),
  );
});

// ---------------------------------------------------------------------------
// Opt-in live profile: the SAME agent over the ACTUAL repository with a real
// model, behind the shared P7 environment gate. One mission, bounded drives.
// ---------------------------------------------------------------------------

const liveEnabled = phase7LiveProfileEnabled(process.env);

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

describe.skipIf(!liveEnabled)("CAP-010 evidence auditor live profile (opt-in)", () => {
  it.live(
    "audits one real evidence document over the actual repository with a live model",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const repoRoot = path.resolve(process.cwd(), "..", "..");
          const workerLayer = workerLayerFor({
            root: repoRoot,
            searchRoots: ["packages"],
            reportRoot: directory,
          }).pipe(Layer.provideMerge(OpenAiClientLayer));

          yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;
            const conversation = decodeConversationId("repo-ops-audit-live");
            const receipt = yield* runtime.submit(
              OpenAiEvidenceAuditor,
              AuditMission.make({ evidenceDocuments: ["docs/S2-EVIDENCE.md"] }),
              repoOpsSubmitOptions(conversation, decodeIdempotencyKey("repo-ops-live-1")),
            );

            // Bounded drive loop: resolve any pending approval between drives.
            let outcome: string | undefined;
            for (let drive = 0; drive < 4 && outcome === undefined; drive += 1) {
              const settlements = yield* runtime
                .processConversation(OpenAiEvidenceAuditor, conversation)
                .pipe(Effect.provide(workerLayer));
              outcome = settlements[0]?.outcome;
              if (outcome !== undefined) break;
              const state = yield* submissionState(receipt.submissionId);
              if (state.state === "suspended") {
                const log = yield* readLog(conversation);
                const request = payloadsOf(log, "ToolApprovalRequested").at(-1)?.record.payload;
                if (request?._tag === "ToolApprovalRequested") {
                  yield* runtime.resolveApproval(
                    ApprovalDecisionCommand.make({
                      submissionId: receipt.submissionId,
                      toolCallId: request.toolCallId,
                      decision: "approved",
                      resolver: "repo-ops-operator",
                      reason: "Live smoke: report write approved.",
                    }),
                  );
                }
              }
            }
            expect(outcome).toBe("completed");
            const report = yield* settledReport(yield* readLog(conversation));
            expect(report.documentsAudited).toBe(1);
          }).pipe(
            Effect.provide(NodeDurableRuntime.layer(runtimeOptions(`${directory}/live.sqlite`))),
          );
        }),
      ),
    300_000,
  );
});
