import { Effect, Layer, Stream } from "effect";
import { LanguageModel } from "effect/unstable/ai";

import { PlannerAttempt } from "./progress.ts";
import { responseTextPreview } from "./response-stream.ts";

/**
 * Native Effect AI is the ordered observation boundary. This decorator preserves
 * the model's E/R and backpressure; it neither forks a reader nor retains parts.
 * Only text and the designated completion message enter the disposable preview.
 * Generation is provisional; canonical settlement remains the result authority.
 * The attempt service owns the writer and fences writes by the admitted attempt.
 */
export const PublicOutputLive = Layer.effect(
  LanguageModel.LanguageModel,
  Effect.gen(function* () {
    const model = yield* LanguageModel.LanguageModel;
    const { progress: writer } = yield* PlannerAttempt;

    return {
      ...model,
      // Preserve native overload inference across a transparent variadic decorator.
      streamText: ((...args: Parameters<LanguageModel.Service["streamText"]>) =>
        Stream.unwrap(
          Effect.sync(() => {
            let responseCall: string | undefined;
            const preview = responseTextPreview();

            return Stream.fromEffect(writer.newResponse).pipe(
              Stream.drain,
              Stream.concat(
                model.streamText(...args).pipe(
                  Stream.tap((part) => {
                    if (part.type === "text-delta") return writer.text(part.delta);
                    if (
                      part.type === "tool-params-start" &&
                      part.name === "deliver_response" &&
                      !part.providerExecuted &&
                      responseCall === undefined
                    ) {
                      responseCall = part.id;

                      return writer.newResponse;
                    }
                    if (part.type === "tool-params-delta" && part.id === responseCall) {
                      const delta = preview(part.delta);

                      return delta.length > 0 ? writer.text(delta) : Effect.void;
                    }

                    return Effect.void;
                  }),
                ),
              ),
            );
          }),
        )) as LanguageModel.Service["streamText"],
    } satisfies LanguageModel.Service;
  }),
);
