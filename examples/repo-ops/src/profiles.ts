import { Agent, type ThreadId } from "@effect-agent/core";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  Principal,
  ProducerId,
  type DurableSubmitOptions,
  type IdempotencyKey,
} from "@effect-agent/thread";
import { OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";

import { AUDIT_REPORT_PATH, EvidenceAuditor, EvidenceAuditReport } from "./evidence-auditor.ts";

// ---------------------------------------------------------------------------
// The evidence auditor's two profiles, mirroring the Travel Planner P7 shape:
// a deterministic offline profile (scripted model + fixture tree in a temp
// directory) that runs on every ordinary gate, and an opt-in live profile
// (real model over the actual repository) behind the SAME
// `phase7LiveProfileEnabled` environment gate from `@effect-agent/testing`.
// ---------------------------------------------------------------------------

/** The auditor's committed capability claim, schema-first like every profile. */
export class EvidenceAuditorProfile extends Schema.Class<EvidenceAuditorProfile>(
  "@effect-agent/example-repo-ops/EvidenceAuditorProfile",
)({
  deploymentClass: Schema.Literal("DN"),
  /** Every audited document is one named Durable Step (`audit:{document}`). */
  durableStepPerDocument: Schema.Literal(true),
  /** The only mutating Tool suspends on a canonical approval request (CAP-006). */
  approvalGatedReportWrite: Schema.Literal(true),
  /** Local process execution is honestly labeled; never a security boundary (CAP-010). */
  sandboxIsolation: Schema.Literal("unisolated"),
  /** The live profile is env-gated out of every ordinary test gate. */
  liveProfileOptIn: Schema.Literal(true),
  /** Never claimed at any phase (DUR-003). */
  exactlyOnceExternalEffects: Schema.Literal(false),
}) {}

export const evidenceAuditorProfile = EvidenceAuditorProfile.make({
  deploymentClass: "DN",
  durableStepPerDocument: true,
  approvalGatedReportWrite: true,
  sandboxIsolation: "unisolated",
  liveProfileOptIn: true,
  exactlyOnceExternalEffects: false,
});

export const repoOpsDeploymentId = Schema.decodeSync(DeploymentId)("repo-ops-p7-deployment");
export const repoOpsProducerId = Schema.decodeSync(ProducerId)("repo-ops-p7-producer");
export const repoOpsPrincipal = Schema.decodeSync(Principal)("repo-ops-p7-principal");

const digestOf = (pair: string) => Schema.decodeSync(Digest)(pair.repeat(32));

/** Redacted, deterministic definition digests for this fixture version. */
export const repoOpsAuditorDigests = DefinitionDigests.make({
  agent: digestOf("60"),
  model: digestOf("61"),
  tools: digestOf("62"),
});

/** Durable admission options for one audit Submission on one mission lane. */
export const repoOpsSubmitOptions = (
  threadId: ThreadId,
  idempotencyKey: IdempotencyKey,
): DurableSubmitOptions => ({
  threadId,
  principal: repoOpsPrincipal,
  idempotencyKey,
  definitions: repoOpsAuditorDigests,
});

// ---------------------------------------------------------------------------
// Deterministic offline profile: a prompt-aware scripted model whose call
// counter and captured prompts live OUTSIDE the Model Layer, so they survive
// Layer rebuilds across Attempts (the durable-approval resume replays the
// declared batch WITHOUT a model call — the script keys on committed history
// in the prompt, never on call order).
// ---------------------------------------------------------------------------

export const OFFLINE_AUDIT_CALL_ID = "audit-1";
export const OFFLINE_WRITE_CALL_ID = "write-1";

const scriptedUsage = { inputTokens: { total: 64 }, outputTokens: { total: 48 } };

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "audit-report" },
  { type: "text-delta", id: "audit-report", delta: text },
  { type: "text-end", id: "audit-report" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

/** The report the offline script expects the Run to settle with. */
export const expectedOfflineReport = (
  documents: ReadonlyArray<string>,
  missingTests: ReadonlyArray<string>,
): EvidenceAuditReport =>
  EvidenceAuditReport.make({
    documentsAudited: documents.length,
    missingCitations: missingTests.length,
    reportPath: AUDIT_REPORT_PATH,
  });

/** The summary line the offline script submits with the report write. */
export const offlineReportSummary = (
  documents: ReadonlyArray<string>,
  missingTests: ReadonlyArray<string>,
): string =>
  `Audited ${documents.length} evidence documents; ${missingTests.length} cited tests are missing from the tree.`;

/**
 * Build the offline scripted auditor model. Turn 1 declares the audit call
 * (durable steps per document), Turn 2 declares the approval-gated report
 * write, Turn 3 writes the final JSON report — all decided from the prompt's
 * committed history.
 */
export const makeOfflineAuditorModel = (
  documents: ReadonlyArray<string>,
  missingTests: ReadonlyArray<string>,
) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
    const decide = (promptJson: string): ReadonlyArray<Response.StreamPartEncoded> => {
      if (promptJson.includes(OFFLINE_WRITE_CALL_ID)) {
        return finalParts(
          JSON.stringify(
            Schema.encodeSync(EvidenceAuditReport)(expectedOfflineReport(documents, missingTests)),
          ),
        );
      }
      if (promptJson.includes(OFFLINE_AUDIT_CALL_ID)) {
        return toolTurn({
          type: "tool-call",
          id: OFFLINE_WRITE_CALL_ID,
          name: "write_audit_report",
          params: {
            reportPath: AUDIT_REPORT_PATH,
            summary: offlineReportSummary(documents, missingTests),
            missingTests,
          },
          providerExecuted: false,
        });
      }
      return toolTurn({
        type: "tool-call",
        id: OFFLINE_AUDIT_CALL_ID,
        name: "audit_evidence",
        params: { documents },
        providerExecuted: false,
      });
    };
    const model = Model.make(
      "scripted",
      "evidence-auditor-offline",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Ref.update(calls, (value) => value + 1);
                const promptJson = JSON.stringify(request.prompt);
                yield* Ref.update(prompts, (previous) => [...previous, promptJson]);
                return Stream.fromIterable(decide(promptJson));
              }),
            ),
        }),
      ),
    );
    return { model, calls: Ref.get(calls), prompts: Ref.get(prompts) };
  });

// ---------------------------------------------------------------------------
// Live profile: the SAME Agent Definition bound to a real model. The consumer
// supplies the upstream OpenAI client Layer (with its own redacted
// OPENAI_API_KEY) and drives it over the actual repository — opt-in through
// `phase7LiveProfileEnabled` exactly like the Travel Planner live smoke.
// ---------------------------------------------------------------------------

/** Explicit live binding for an application-supplied OpenAI client Layer. */
export const OpenAiEvidenceAuditor = Agent.withModel(
  EvidenceAuditor,
  OpenAiLanguageModel.model("gpt-4.1-mini"),
);
