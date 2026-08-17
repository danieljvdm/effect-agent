import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";

import { FileReviewReport } from "./fan-out.ts";
import { CodeReview } from "./review-agent.ts";
import { makePromptKeyedModel, scriptedFinalParts, scriptedToolTurn } from "./scripted.ts";

// ---------------------------------------------------------------------------
// Deterministic offline models for the fan-out reviewer: prompt-keyed
// scripted models for BOTH the coordinator and the file-reviewer children.
// Both key every decision on committed history in the prompt (tool-call ids
// and briefed unit ids), never on call order, so concurrent children and
// replays stay honest.
// ---------------------------------------------------------------------------

export const OFFLINE_UNITS_CALL_ID = "units-1";

/** The delegation Tool Call id the scripted coordinator uses for one unit. */
export const offlineUnitCallId = (unitId: string): string => `delegate-${unitId}`;

/** The retry Tool Call id the scripted coordinator uses for one failed unit. */
export const offlineUnitRetryCallId = (unitId: string): string => `delegate-retry-${unitId}`;

/** The diff Tool Call id the scripted child uses for one unit. */
export const offlineChildDiffCallId = (unitId: string): string => `fanout-diff-${unitId}`;

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
  readonly retryUnitCalls?: ReadonlyArray<OfflineUnitCall> | undefined;
  readonly review: CodeReview;
}) => {
  const firstUnitCallId = offlineUnitCallId(script.unitCalls[0]?.unitId ?? "unit-none");
  const firstRetryCallId = offlineUnitRetryCallId(
    script.retryUnitCalls?.[0]?.unitId ?? "unit-none",
  );
  return makePromptKeyedModel("pr-fanout-coordinator-offline", (promptJson) => {
    if ((script.retryUnitCalls?.length ?? 0) > 0 && promptJson.includes(firstRetryCallId)) {
      return scriptedFinalParts(JSON.stringify(Schema.encodeSync(CodeReview)(script.review)));
    }
    if (promptJson.includes(firstUnitCallId)) {
      if ((script.retryUnitCalls?.length ?? 0) > 0) {
        return scriptedToolTurn(
          ...(script.retryUnitCalls ?? []).map(
            (unit): Response.StreamPartEncoded => ({
              type: "tool-call",
              id: offlineUnitRetryCallId(unit.unitId),
              name: "delegate_file_review",
              params: { unitId: unit.unitId, paths: unit.paths },
              providerExecuted: false,
            }),
          ),
        );
      }
      return scriptedFinalParts(JSON.stringify(Schema.encodeSync(CodeReview)(script.review)));
    }
    if (promptJson.includes(OFFLINE_UNITS_CALL_ID)) {
      return scriptedToolTurn(
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
    return scriptedToolTurn({
      type: "tool-call",
      id: OFFLINE_UNITS_CALL_ID,
      name: "list_review_units",
      params: { scope: "all" },
      providerExecuted: false,
    });
  });
};

/** How one scripted child behaves for its briefed unit. */
export type OfflineUnitOutcome =
  /** Read one diff, then return the scripted report. */
  | { readonly _tag: "findings"; readonly report: FileReviewReport }
  /** Read one diff, then return non-JSON — the child fails typed (AgentOutputError). */
  | { readonly _tag: "malformed-output" }
  /** First child fails output decoding; one explicit retry returns the report. */
  | { readonly _tag: "malformed-once-then-findings"; readonly report: FileReviewReport }
  /**
   * Declare more Tool Calls than the child's AgentPolicy allows in one turn —
   * none executes and the child fails typed (AgentPolicyError "tool-calls",
   * the reviewer's deliberate `onExhaustion: "fail"` pin).
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
    const completedAttempts = yield* Ref.make<ReadonlyMap<string, number>>(new Map());
    const decide = (
      promptJson: string,
      completedAttempt: number | undefined,
    ): ReadonlyArray<Response.StreamPartEncoded> | undefined => {
      const script = scripts.find((candidate) => promptJson.includes(candidate.unitId));
      if (script === undefined) return undefined;
      switch (script.outcome._tag) {
        case "budget-runaway": {
          return scriptedToolTurn(
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
            return scriptedFinalParts(
              script.outcome._tag === "findings"
                ? JSON.stringify(Schema.encodeSync(FileReviewReport)(script.outcome.report))
                : "this is not the JSON you are looking for",
            );
          }
          return scriptedToolTurn({
            type: "tool-call",
            id: offlineChildDiffCallId(script.unitId),
            name: "read_file_diff",
            params: { path: script.diffPath },
            providerExecuted: false,
          });
        }
        case "malformed-once-then-findings": {
          if (promptJson.includes(offlineChildDiffCallId(script.unitId))) {
            return scriptedFinalParts(
              completedAttempt === 0
                ? "this is not the JSON you are looking for"
                : JSON.stringify(Schema.encodeSync(FileReviewReport)(script.outcome.report)),
            );
          }
          return scriptedToolTurn({
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
                const script = scripts.find((candidate) => promptJson.includes(candidate.unitId));
                const completedAttempt =
                  script?.outcome._tag === "malformed-once-then-findings" &&
                  promptJson.includes(offlineChildDiffCallId(script.unitId))
                    ? yield* Ref.modify(completedAttempts, (attempts) => {
                        const current = attempts.get(script.unitId) ?? 0;
                        const next = new Map(attempts);
                        next.set(script.unitId, current + 1);
                        return [current, next] as const;
                      })
                    : undefined;
                const parts = decide(promptJson, completedAttempt);
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
