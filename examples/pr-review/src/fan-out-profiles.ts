import { OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { Agent } from "effect-agent";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";

import { FanOutReviewer, FileReviewer, FileReviewReport } from "./fan-out-review-agent.ts";
import { DEFAULT_REVIEW_MODEL } from "./profiles.ts";
import { CodeReview } from "./review-agent.ts";

// ---------------------------------------------------------------------------
// The fan-out reviewer's two profiles, mirroring profiles.ts: a deterministic
// offline profile with prompt-keyed scripted models for BOTH the coordinator
// and the file-reviewer children, and an opt-in live profile binding the same
// two definitions to a real OpenAI model behind the same environment gate.
// ---------------------------------------------------------------------------

/** The fan-out reviewer's committed capability claim, schema-first. */
export class FanOutReviewerProfile extends Schema.Class<FanOutReviewerProfile>(
  "@effect-agent/example-pr-review/FanOutReviewerProfile",
)({
  /** Ephemeral runtime: one bounded AgentRuntime.run per invocation. */
  deploymentClass: Schema.Literal("E"),
  /** Every model-callable tool — parent and child — is a read; none mutate. */
  readOnlyToolSurface: Schema.Literal(true),
  /** The review is posted by the host AFTER the run settles, never by a tool. */
  publicationOutsideAgentLoop: Schema.Literal(true),
  /** Child findings are untrusted; anchors are validated against the parsed diff. */
  anchorsValidatedBeforePublication: Schema.Literal(true),
  /** S1 attached ephemeral delegation at depth 1; nested delegation is rejected. */
  attachedEphemeralDelegation: Schema.Literal(true),
  /** A failed unit surfaces to the coordinator as a typed failed result, never retried. */
  failedUnitsReportedNotRetried: Schema.Literal(true),
  /** The live profile is env-gated out of every ordinary test gate. */
  liveProfileOptIn: Schema.Literal(true),
  /** Never claimed at any phase (DUR-003). */
  exactlyOnceExternalEffects: Schema.Literal(false),
}) {}

export const fanOutReviewerProfile = FanOutReviewerProfile.make({
  deploymentClass: "E",
  readOnlyToolSurface: true,
  publicationOutsideAgentLoop: true,
  anchorsValidatedBeforePublication: true,
  attachedEphemeralDelegation: true,
  failedUnitsReportedNotRetried: true,
  liveProfileOptIn: true,
  exactlyOnceExternalEffects: false,
});

// ---------------------------------------------------------------------------
// Live profile: one caller-selected OpenAI model bound to both definitions.
// The OpenAI client Layer stays application-supplied (D-027).
// ---------------------------------------------------------------------------

export const makeOpenAiFanOutReviewer = (model?: string) => {
  const languageModel = OpenAiLanguageModel.model(model ?? DEFAULT_REVIEW_MODEL, {
    max_output_tokens: 8_000,
    store: false,
    strictJsonSchema: true,
  });
  return {
    parent: Agent.withModel(FanOutReviewer, languageModel),
    child: Agent.withModel(FileReviewer, languageModel),
  };
};

// ---------------------------------------------------------------------------
// Deterministic offline profile. Both scripted models key every decision on
// committed history in the prompt (tool-call ids and briefed unit ids), never
// on call order, so concurrent children and replays stay honest.
// ---------------------------------------------------------------------------

export const OFFLINE_UNITS_CALL_ID = "units-1";

/** The delegation Tool Call id the scripted coordinator uses for one unit. */
export const offlineUnitCallId = (unitId: string): string => `delegate-${unitId}`;

/** The diff Tool Call id the scripted child uses for one unit. */
export const offlineChildDiffCallId = (unitId: string): string => `fanout-diff-${unitId}`;

const scriptedUsage = { inputTokens: { total: 64 }, outputTokens: { total: 48 } };

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "fan-out-final" },
  { type: "text-delta", id: "fan-out-final", delta: text },
  { type: "text-end", id: "fan-out-final" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

/** One scripted delegation the offline coordinator declares. */
export interface OfflineUnitCall {
  readonly unitId: string;
  readonly paths: ReadonlyArray<string>;
}

/**
 * Build the offline scripted coordinator model. Turn 1 lists the review
 * units, Turn 2 declares one delegation Tool Call per scripted unit in one
 * batch, Turn 3 returns the scripted merged review JSON. Decisions key on
 * tool-call ids already committed to the prompt.
 */
export const makeOfflineFanOutCoordinatorModel = (script: {
  readonly unitCalls: ReadonlyArray<OfflineUnitCall>;
  readonly review: CodeReview;
}) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
    const firstUnitCallId = offlineUnitCallId(script.unitCalls[0]?.unitId ?? "unit-none");
    const decide = (promptJson: string): ReadonlyArray<Response.StreamPartEncoded> => {
      if (promptJson.includes(firstUnitCallId)) {
        return finalParts(JSON.stringify(Schema.encodeSync(CodeReview)(script.review)));
      }
      if (promptJson.includes(OFFLINE_UNITS_CALL_ID)) {
        return toolTurn(
          ...script.unitCalls.map(
            (unit): Response.StreamPartEncoded => ({
              type: "tool-call",
              id: offlineUnitCallId(unit.unitId),
              name: "delegate_file_review",
              params: { unitId: unit.unitId, paths: unit.paths },
              providerExecuted: false,
            }),
          ),
        );
      }
      return toolTurn({
        type: "tool-call",
        id: OFFLINE_UNITS_CALL_ID,
        name: "list_review_units",
        params: { scope: "all" },
        providerExecuted: false,
      });
    };
    const model = Model.make(
      "scripted",
      "pr-fanout-coordinator-offline",
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

/** How one scripted child behaves for its briefed unit. */
export type OfflineUnitOutcome =
  /** Read one diff, then return the scripted report. */
  | { readonly _tag: "findings"; readonly report: FileReviewReport }
  /** Read one diff, then return non-JSON — the child fails typed (AgentOutputError). */
  | { readonly _tag: "malformed-output" }
  /**
   * Declare more Tool Calls than the child's AgentPolicy allows in one turn —
   * the child fails typed (AgentPolicyError "tool-calls") before any executes.
   */
  | { readonly _tag: "budget-runaway"; readonly declaredCalls: number };

export interface OfflineUnitScript {
  readonly unitId: string;
  /** The one file the scripted child reads the diff of. */
  readonly diffPath: string;
  readonly outcome: OfflineUnitOutcome;
}

/**
 * Build the offline scripted file-reviewer model shared by every delegated
 * child. Each child Run builds the Model Layer inside its own scope; the
 * script entry is selected by the briefed unitId present in the child's OWN
 * prompt, and the turn is selected by whether that unit's diff Tool Call id
 * is already committed there — content-keyed on both axes, so concurrent
 * children never interfere. First-turn child prompts are recorded for
 * context-isolation assertions.
 */
export const makeOfflineFileReviewerModel = (scripts: ReadonlyArray<OfflineUnitScript>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
    const decide = (promptJson: string): ReadonlyArray<Response.StreamPartEncoded> | undefined => {
      const script = scripts.find((candidate) => promptJson.includes(candidate.unitId));
      if (script === undefined) return undefined;
      switch (script.outcome._tag) {
        case "budget-runaway": {
          return toolTurn(
            ...Array.from(
              { length: script.outcome.declaredCalls },
              (_, index): Response.StreamPartEncoded => ({
                type: "tool-call",
                id: `runaway-${script.unitId}-${index + 1}`,
                name: "read_file_diff",
                params: { path: script.diffPath },
                providerExecuted: false,
              }),
            ),
          );
        }
        case "malformed-output":
        case "findings": {
          if (promptJson.includes(offlineChildDiffCallId(script.unitId))) {
            return finalParts(
              script.outcome._tag === "findings"
                ? JSON.stringify(Schema.encodeSync(FileReviewReport)(script.outcome.report))
                : "this is not the JSON you are looking for",
            );
          }
          return toolTurn({
            type: "tool-call",
            id: offlineChildDiffCallId(script.unitId),
            name: "read_file_diff",
            params: { path: script.diffPath },
            providerExecuted: false,
          });
        }
      }
    };
    const model = Model.make(
      "scripted",
      "pr-fanout-file-reviewer-offline",
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
                const parts = decide(promptJson);
                if (parts === undefined) {
                  return yield* Effect.die(
                    new Error("The child prompt names no scripted review unit"),
                  );
                }
                return Stream.fromIterable(parts);
              }),
            ),
        }),
      ),
    );
    return { model, calls: Ref.get(calls), prompts: Ref.get(prompts) };
  });
