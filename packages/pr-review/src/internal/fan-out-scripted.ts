import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";

import { FileReviewReport, FileReviewRequest } from "./fan-out.ts";
import { CodeReview } from "./review-agent.ts";
import { makePromptKeyedModel, scriptedFinalParts, scriptedToolTurn } from "./scripted.ts";

// Deterministic offline models for the complete fan-out protocol. Decisions
// key on committed Tool Call IDs or work IDs in each prompt, never call order,
// so bounded child concurrency cannot make the fixtures flaky.

export const OFFLINE_UNITS_CALL_ID = "units-1";

export const offlineUnitCallId = (workId: string): string => `delegate-${workId}`;

export type OfflineUnitCall = FileReviewRequest;

export const makeOfflineFanOutCoordinatorModel = (script: {
  readonly discoveryCalls: ReadonlyArray<OfflineUnitCall>;
  readonly verificationCalls: ReadonlyArray<OfflineUnitCall>;
  readonly review: CodeReview;
}) => {
  const firstDiscovery = offlineUnitCallId(script.discoveryCalls[0]?.workId ?? "none");
  const firstVerification = offlineUnitCallId(script.verificationCalls[0]?.workId ?? "none");
  return makePromptKeyedModel("pr-fanout-coordinator-offline", (promptJson) => {
    if (
      script.verificationCalls.length === 0
        ? promptJson.includes(firstDiscovery)
        : promptJson.includes(firstVerification)
    ) {
      return scriptedFinalParts(JSON.stringify(Schema.encodeSync(CodeReview)(script.review)));
    }
    if (promptJson.includes(firstDiscovery)) {
      return scriptedToolTurn(
        ...script.verificationCalls.map(
          (call): Response.StreamPartEncoded => ({
            type: "tool-call",
            id: offlineUnitCallId(call.workId),
            name: "delegate_file_review",
            params: Schema.encodeSync(FileReviewRequest)(call),
            providerExecuted: false,
          }),
        ),
      );
    }
    if (promptJson.includes(OFFLINE_UNITS_CALL_ID)) {
      return scriptedToolTurn(
        ...script.discoveryCalls.map(
          (call): Response.StreamPartEncoded => ({
            type: "tool-call",
            id: offlineUnitCallId(call.workId),
            name: "delegate_file_review",
            params: Schema.encodeSync(FileReviewRequest)(call),
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

export type OfflineUnitOutcome =
  | { readonly _tag: "report"; readonly report: FileReviewReport }
  | { readonly _tag: "malformed-output" };

export interface OfflineUnitScript {
  readonly workId: string;
  readonly outcome: OfflineUnitOutcome;
}

export const makeOfflineFileReviewerModel = (scripts: ReadonlyArray<OfflineUnitScript>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
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
                return Stream.fromIterable(
                  scriptedFinalParts(
                    selected.outcome._tag === "report"
                      ? JSON.stringify(Schema.encodeSync(FileReviewReport)(selected.outcome.report))
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
