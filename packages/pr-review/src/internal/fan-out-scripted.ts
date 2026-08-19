import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model } from "effect/unstable/ai";

import { FileReviewReport } from "./fan-out.ts";
import { scriptedFinalParts } from "./scripted.ts";

// Deterministic offline child models for the host-scheduled fan-out pipeline.
// Decisions key on the work ID committed into each child's instructions,
// never call order, so bounded pass concurrency cannot make fixtures flaky.
// Each work ID carries a SEQUENCE of outcomes consumed per call, so tests can
// script "fail once, then settle" and pin the pipeline's bounded retry.

export type OfflineUnitOutcome =
  | { readonly _tag: "report"; readonly report: FileReviewReport }
  | { readonly _tag: "malformed-output" };

export interface OfflineUnitScript {
  readonly workId: string;
  /** Consumed one per child call for this work ID; the last outcome repeats. */
  readonly outcomes: ReadonlyArray<OfflineUnitOutcome>;
}

export const makeOfflineFileReviewerModel = (scripts: ReadonlyArray<OfflineUnitScript>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
    const callsByWorkId = yield* Ref.make<ReadonlyMap<string, number>>(new Map());
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
                const script = scripts.filter((candidate) =>
                  promptJson.includes(`for ${candidate.workId} in host-planned unit`),
                );
                if (script.length !== 1) {
                  return yield* Effect.die(
                    new Error("The child prompt must name exactly one scripted work ID"),
                  );
                }
                const [selected] = script;
                if (selected === undefined) return yield* Effect.die("unreachable scripted match");
                const occurrence = yield* Ref.modify(callsByWorkId, (byWorkId) => {
                  const count = byWorkId.get(selected.workId) ?? 0;
                  const next = new Map(byWorkId);
                  next.set(selected.workId, count + 1);
                  return [count, next] as const;
                });
                const outcome =
                  selected.outcomes[Math.min(occurrence, selected.outcomes.length - 1)];
                if (outcome === undefined) {
                  return yield* Effect.die(
                    new Error(`Work ID ${selected.workId} scripts no outcomes`),
                  );
                }
                return Stream.fromIterable(
                  scriptedFinalParts(
                    outcome._tag === "report"
                      ? JSON.stringify(Schema.encodeSync(FileReviewReport)(outcome.report))
                      : "this is not valid review JSON",
                  ),
                );
              }),
            ),
        }),
      ),
    );
    return { model, calls: Ref.get(calls), prompts: Ref.get(prompts) };
  });
