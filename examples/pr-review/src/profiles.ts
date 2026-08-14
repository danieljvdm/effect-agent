import { OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { Agent } from "effect-agent";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";

import { CodeReview, PullRequestReviewer } from "./review-agent.ts";

// ---------------------------------------------------------------------------
// The reviewer's two profiles, mirroring the repo-ops shape: a deterministic
// offline profile (prompt-aware scripted model over a fixture pull request)
// that runs on every ordinary gate, and an opt-in live profile (real
// OpenAI model) behind an explicit environment gate.
// ---------------------------------------------------------------------------

/** The reviewer's committed capability claim, schema-first like every profile. */
export class PullRequestReviewerProfile extends Schema.Class<PullRequestReviewerProfile>(
  "@effect-agent/example-pr-review/PullRequestReviewerProfile",
)({
  /** Ephemeral runtime: one bounded AgentRuntime.run per CI invocation. */
  deploymentClass: Schema.Literal("E"),
  /** Every model-callable tool is a read of the pull request; none mutate. */
  readOnlyToolSurface: Schema.Literal(true),
  /** The review is posted by the host AFTER the run settles, never by a tool. */
  publicationOutsideAgentLoop: Schema.Literal(true),
  /** Finding anchors are validated against the parsed diff before posting. */
  anchorsValidatedBeforePublication: Schema.Literal(true),
  /** The live profile is env-gated out of every ordinary test gate. */
  liveProfileOptIn: Schema.Literal(true),
  /** Never claimed at any phase (DUR-003). */
  exactlyOnceExternalEffects: Schema.Literal(false),
}) {}

export const pullRequestReviewerProfile = PullRequestReviewerProfile.make({
  deploymentClass: "E",
  readOnlyToolSurface: true,
  publicationOutsideAgentLoop: true,
  anchorsValidatedBeforePublication: true,
  liveProfileOptIn: true,
  exactlyOnceExternalEffects: false,
});

// ---------------------------------------------------------------------------
// Live profile.
// ---------------------------------------------------------------------------

export const DEFAULT_REVIEW_MODEL = "gpt-5.6-sol";

export const LIVE_GATE_ENV = "EFFECT_AGENT_LIVE";
export const LIVE_CREDENTIAL_ENV = "OPENAI_API_KEY";

/** `EFFECT_AGENT_LIVE=1` plus a credential is the only enabling combination. */
export const liveReviewProfileEnabled = (env: Record<string, string | undefined>): boolean =>
  env[LIVE_GATE_ENV] === "1" && (env[LIVE_CREDENTIAL_ENV] ?? "") !== "";

/**
 * Live binding factory: one definition, caller-selected model. The OpenAI
 * client Layer (with its redacted OPENAI_API_KEY) is supplied by the
 * application, never by the definition (D-027).
 */
export const makeOpenAiReviewer = (model?: string) =>
  Agent.withModel(
    PullRequestReviewer,
    OpenAiLanguageModel.model(model ?? DEFAULT_REVIEW_MODEL, {
      max_output_tokens: 8_000,
      store: false,
      strictJsonSchema: true,
    }),
  );

// ---------------------------------------------------------------------------
// Deterministic offline profile: a prompt-aware scripted model that walks the
// real tool surface — list, diff, read — then returns the fixture review as
// its terminal JSON. Decisions key on committed history in the prompt, never
// on call order, so replays stay honest.
// ---------------------------------------------------------------------------

export const OFFLINE_LIST_CALL_ID = "list-1";
export const OFFLINE_DIFF_CALL_ID = "diff-1";
export const OFFLINE_READ_CALL_ID = "read-1";

const scriptedUsage = { inputTokens: { total: 64 }, outputTokens: { total: 48 } };

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "code-review" },
  { type: "text-delta", id: "code-review", delta: text },
  { type: "text-end", id: "code-review" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

/**
 * Build the offline scripted reviewer model. Turn 1 lists the changeset,
 * Turn 2 reads one file diff, Turn 3 reads head context, Turn 4 returns the
 * scripted review JSON.
 */
export const makeOfflineReviewerModel = (script: {
  readonly diffPath: string;
  readonly readPath: string;
  readonly review: CodeReview;
}) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
    const decide = (promptJson: string): ReadonlyArray<Response.StreamPartEncoded> => {
      if (promptJson.includes(OFFLINE_READ_CALL_ID)) {
        return finalParts(JSON.stringify(Schema.encodeSync(CodeReview)(script.review)));
      }
      if (promptJson.includes(OFFLINE_DIFF_CALL_ID)) {
        return toolTurn({
          type: "tool-call",
          id: OFFLINE_READ_CALL_ID,
          name: "read_file",
          params: { path: script.readPath },
          providerExecuted: false,
        });
      }
      if (promptJson.includes(OFFLINE_LIST_CALL_ID)) {
        return toolTurn({
          type: "tool-call",
          id: OFFLINE_DIFF_CALL_ID,
          name: "read_file_diff",
          params: { path: script.diffPath },
          providerExecuted: false,
        });
      }
      return toolTurn({
        type: "tool-call",
        id: OFFLINE_LIST_CALL_ID,
        name: "list_changed_files",
        params: { scope: "all" },
        providerExecuted: false,
      });
    };
    const model = Model.make(
      "scripted",
      "pr-review-offline",
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
