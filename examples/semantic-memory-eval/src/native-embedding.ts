import { pipeline, type FeatureExtractionPipeline } from "@huggingface/transformers";
import { Clock, Effect, Layer, Ref, Semaphore } from "effect";
import { AiError, EmbeddingModel } from "effect/unstable/ai";

import { MODEL_ID, MODEL_REVISION } from "./contracts.ts";

export interface NativeEmbeddingOptions {
  readonly cachePath: string;
  readonly offline: boolean;
  readonly delayMillis?: number;
  readonly fail?: boolean;
}

export interface EmbeddingTelemetrySnapshot {
  readonly modelStartupMillis: number;
  readonly calls: number;
  readonly inputs: number;
  readonly textBytes: number;
}

const aiError = (method: string, cause: unknown) =>
  AiError.make({
    module: "SemanticMemoryEvaluation",
    method,
    reason: AiError.UnknownError.make({
      description: cause instanceof Error ? cause.message : String(cause),
    }),
  });

const byteLength = (text: string): number => new TextEncoder().encode(text).length;
const elapsedMillis = (started: bigint, finished: bigint): number =>
  Number(finished - started) / 1_000_000;

const load = (options: NativeEmbeddingOptions) =>
  Effect.tryPromise({
    try: () =>
      pipeline("feature-extraction", MODEL_ID, {
        revision: MODEL_REVISION,
        dtype: "fp32",
        cache_dir: options.cachePath,
        local_files_only: options.offline,
        session_options: {
          executionProviders: ["cpu"],
          intraOpNumThreads: 1,
          interOpNumThreads: 1,
          executionMode: "sequential",
        },
      }),
    catch: (cause) => aiError("load", cause),
  });

const dispose = (extractor: FeatureExtractionPipeline) =>
  Effect.tryPromise({
    try: () => extractor.dispose(),
    catch: (cause) => aiError("dispose", cause),
  }).pipe(Effect.orDie);

/**
 * A local Transformers.js adapter at the Effect AI boundary. Native inference has no abort
 * signal, so each started call is uninterruptible and scope finalization waits for it before
 * disposing the pipeline. Delays injected before inference remain cooperatively interruptible.
 */
export const makeNativeEmbeddingLayer = Effect.fn("makeNativeEmbeddingLayer")(function* (
  options: NativeEmbeddingOptions,
) {
  const telemetry = yield* Ref.make<EmbeddingTelemetrySnapshot>({
    modelStartupMillis: 0,
    calls: 0,
    inputs: 0,
    textBytes: 0,
  });
  const semaphore = yield* Semaphore.make(1);
  const layer = Layer.effect(
    EmbeddingModel.EmbeddingModel,
    Effect.acquireRelease(
      Effect.gen(function* () {
        const started = yield* Clock.monotonicTimeNanos;
        const extractor = yield* load(options);
        const finished = yield* Clock.monotonicTimeNanos;
        yield* Ref.update(telemetry, (current) => ({
          ...current,
          modelStartupMillis: elapsedMillis(started, finished),
        }));
        return extractor;
      }),
      (extractor) => semaphore.withPermits(1)(dispose(extractor)),
    ).pipe(
      Effect.flatMap((extractor) =>
        EmbeddingModel.make({
          embedMany: ({ inputs }) =>
            Effect.gen(function* () {
              if ((options.delayMillis ?? 0) > 0) yield* Effect.sleep(options.delayMillis!);
              if (options.fail === true) return yield* aiError("embedMany", "injected failure");
              yield* Ref.update(telemetry, (current) => ({
                ...current,
                calls: current.calls + 1,
                inputs: current.inputs + inputs.length,
                textBytes:
                  current.textBytes + inputs.reduce((total, input) => total + byteLength(input), 0),
              }));
              const results = yield* semaphore.withPermits(1)(
                Effect.acquireUseRelease(
                  Effect.uninterruptible(
                    Effect.tryPromise({
                      try: () => extractor([...inputs], { pooling: "mean", normalize: true }),
                      catch: (cause) => aiError("embedMany", cause),
                    }),
                  ),
                  (tensor) =>
                    Effect.gen(function* () {
                      if (
                        tensor.type !== "float32" ||
                        tensor.dims.length !== 2 ||
                        tensor.dims[0] !== inputs.length ||
                        tensor.dims[1] !== 384 ||
                        !(tensor.data instanceof Float32Array)
                      ) {
                        return yield* aiError(
                          "embedMany",
                          `unexpected tensor ${tensor.type} [${tensor.dims.join(",")}]`,
                        );
                      }
                      const data = tensor.data;
                      return Array.from({ length: inputs.length }, (_, row) =>
                        Array.from(data.subarray(row * 384, (row + 1) * 384)),
                      );
                    }),
                  (tensor) => Effect.sync(() => tensor.dispose()).pipe(Effect.orDie),
                ),
              );
              return { results, usage: { inputTokens: undefined } };
            }),
        }),
      ),
    ),
  );
  return { layer, snapshot: Ref.get(telemetry) } as const;
});
