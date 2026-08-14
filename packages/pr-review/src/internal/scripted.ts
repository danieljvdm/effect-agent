import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";

import { CodeReview } from "./review-agent.ts";

// ---------------------------------------------------------------------------
// Deterministic offline model for the flat reviewer: a prompt-aware scripted
// model that walks the real tool surface — list, diff, read — then returns
// the scripted review as its terminal JSON. Decisions key on committed
// history in the prompt, never on call order, so replays stay honest.
// ---------------------------------------------------------------------------

export const OFFLINE_LIST_CALL_ID = "list-1";
export const OFFLINE_DIFF_CALL_ID = "diff-1";
export const OFFLINE_READ_CALL_ID = "read-1";

const scriptedUsage = { inputTokens: { total: 64 }, outputTokens: { total: 48 } };

export const scriptedToolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

export const scriptedFinalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "code-review" },
  { type: "text-delta", id: "code-review", delta: text },
  { type: "text-end", id: "code-review" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

/** A prompt-keyed scripted LanguageModel with call and prompt observability. */
export const makePromptKeyedModel = (
  name: string,
  decide: (promptJson: string) => ReadonlyArray<Response.StreamPartEncoded>,
) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
    const model = Model.make(
      "scripted",
      name,
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
  makePromptKeyedModel("pr-review-offline", (promptJson) => {
    if (promptJson.includes(OFFLINE_READ_CALL_ID)) {
      return scriptedFinalParts(JSON.stringify(Schema.encodeSync(CodeReview)(script.review)));
    }
    if (promptJson.includes(OFFLINE_DIFF_CALL_ID)) {
      return scriptedToolTurn({
        type: "tool-call",
        id: OFFLINE_READ_CALL_ID,
        name: "read_file",
        params: { path: script.readPath },
        providerExecuted: false,
      });
    }
    if (promptJson.includes(OFFLINE_LIST_CALL_ID)) {
      return scriptedToolTurn({
        type: "tool-call",
        id: OFFLINE_DIFF_CALL_ID,
        name: "read_file_diff",
        params: { path: script.diffPath },
        providerExecuted: false,
      });
    }
    return scriptedToolTurn({
      type: "tool-call",
      id: OFFLINE_LIST_CALL_ID,
      name: "list_changed_files",
      params: { scope: "all" },
      providerExecuted: false,
    });
  });
