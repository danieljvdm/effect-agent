import { gunzipSync } from "node:zlib";

import type { DecisionSchema } from "@effect-agent/ai-decision";
import { DecisionModel, DecisionQuery } from "@effect-agent/ai-decision";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Clock, Console, Effect, FileSystem, Layer, Option, Result, Schema, Stream } from "effect";
import { CompactionPolicy } from "effect-agent/agent-policy";
import { CompactionEvaluator, ContextCompactor } from "effect-agent/context-compactor";
import { RunId, ThreadId } from "effect-agent/identifiers";
import * as SelectiveCompactor from "effect-agent/selective-compactor";
import { Prompt } from "effect/unstable/ai";
import { Command, Flag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";

const Capture = Schema.Struct({
  phase: Schema.String,
  number: Schema.Natural,
  prompt: Schema.Struct({
    threadId: Schema.String,
    runId: Schema.String,
    turn: Schema.Natural,
    source: Prompt.Prompt,
  }),
});

class ReplayInputError extends Schema.TaggedError<ReplayInputError>()("ReplayInputError", {
  message: Schema.String,
}) {}

const live = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layer),
  Layer.provide(TypeSafeClient.Config.layer),
  Layer.provide(FetchHttpClient.layer),
);

// A frozen native prompt is the input. There is one live decision request per checkpoint;
// alternative cutoffs reuse its answers so model variance cannot favor a cutoff.
export const command = Command.make(
  "travel-score",
  {
    input: Flag.String("input"),
    output: Flag.String("output"),
    question: Flag.String("question").pipe(Flag.optional),
    dropBelow: Flag.Finite("drop-below").pipe(Flag.withDefault(0.1)),
  },
  Effect.fn("TravelScore.run")(function* ({ input, output, question, dropBelow }) {
    const fs = yield* FileSystem.FileSystem;

    if (dropBelow < 0 || dropBelow > 1)
      return yield* ReplayInputError.make({ message: "--drop-below must be between 0 and 1" });
    if (yield* fs.exists(output))
      return yield* Effect.fail("Keep score attempts in separate files");

    const bytes = yield* fs.readFile(input);

    const json = yield* Effect.try({
      try: () =>
        new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(
          input.endsWith(".gz") ? gunzipSync(bytes, { maxOutputLength: 16 * 1024 * 1024 }) : bytes,
        ),
      catch: () =>
        ReplayInputError.make({
          message: "Could not decode the recorded JSON prompt (16 MiB gzip limit)",
        }),
    });

    const capture = yield* Schema.decodeEffect(Schema.fromJsonString(Capture))(json);

    const native = yield* DecisionModel.DecisionModel;
    let selection: typeof SelectiveCompactor.SelectionState.Type | undefined;
    let response: DecisionSchema.EvaluateResponse | undefined;
    let observedRequest: DecisionSchema.EvaluateRequest | undefined;

    const observer = Layer.effect(
      DecisionModel.DecisionModel,
      DecisionModel.make({
        evaluate: (request) =>
          Effect.gen(function* () {
            observedRequest = request;
            selection = yield* Schema.decodeUnknownEffect(SelectiveCompactor.SelectionState)(
              request.state,
            ).pipe(Effect.orDie);
            response = yield* native.evaluate(request);

            return response;
          }),
      }),
    );

    const source = capture.prompt.source;
    const start = yield* Clock.currentTimeMillis;

    const outcome = yield* Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      return yield* compactor
        .compact({
          source,
          threadId: ThreadId.make(capture.prompt.threadId),
          runId: RunId.make(capture.prompt.runId),
          turn: capture.prompt.turn,
          trigger: "pressure",
          modelCallAllowed: true,
          targetTokens: undefined,
          policy: CompactionPolicy.make({ mode: "prune", keepRecentTokens: 1 }),
          state: {
            protectedStart: source.content.findLastIndex((message) => message.role === "user"),
            protectedEnd: source.content.findLastIndex((message) => message.role === "user") + 1,
            clearedThrough: 0,
            replacement: undefined,
            lastCompactionTurn: 0,
            overflowRetryTurn: 0,
            lastViewLength: -1,
          },
          summarize: () => Effect.die("This replay only measures selective pruning"),
        })
        .pipe(Stream.runCollect);
    }).pipe(
      Effect.provideService(CompactionEvaluator(), {
        available: true,
        evaluate: (operation) => operation.pipe(Effect.map((result) => result.value)),
      }),
      Effect.provide(
        SelectiveCompactor.layer({
          dropBelow,
          ...(Option.isSome(question)
            ? {
                question: ({ result }: SelectiveCompactor.QuestionInput) =>
                  Effect.succeed(
                    DecisionQuery.probability({
                      instructions: `${question.value} Candidate: ${result.id} (${result.tool}).`,
                    }),
                  ),
              }
            : {}),
        }).pipe(Layer.provide(ContextCompactor.layer), Layer.provide(observer)),
      ),
      Effect.result,
    );

    const record = {
      input,
      question: Option.getOrNull(question),
      dropBelow,
      phase: capture.phase,
      threadId: capture.prompt.threadId,
      elapsedMs: (yield* Clock.currentTimeMillis) - start,
      error: Result.isFailure(outcome) ? outcome.failure.message : null,
      decisions: Result.isSuccess(outcome) ? outcome.success : [],
      selection,
      response,
      request: observedRequest,
    };

    yield* fs.writeFileString(output, JSON.stringify(record, null, 2));
    yield* Console.log(
      JSON.stringify({
        phase: record.phase,
        elapsedMs: record.elapsedMs,
        candidates: selection?.results.length ?? 0,
        decisions: record.decisions,
        error: record.error,
      }),
    );
  }, Effect.provide(live)),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: "1.0.0" }).pipe(Effect.provide(NodeServices.layer)),
  );
