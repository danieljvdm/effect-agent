import { Effect, FileSystem, Schema } from "effect";

import { EvalDataError, type EvalObservation } from "./contracts.ts";
import { decodeObservationLines } from "./corpus.ts";
import { EvalJudgmentSet } from "./judgments.ts";
import { EvalQualityReport } from "./report.ts";

const MAX_OBSERVATION_BYTES = 256n * 1_024n * 1_024n;
const MAX_JUDGMENT_BYTES = 64n * 1_024n * 1_024n;

const dataError = (
  operation: string,
  message: string,
  path: string,
  cause?: unknown,
): EvalDataError =>
  EvalDataError.make({
    operation,
    message,
    path,
    ...(cause === undefined ? {} : { cause }),
  });

const readBoundedFile = Effect.fn("PrReviewEval.readBoundedFile")(function* (
  path: string,
  maximumBytes: bigint,
  operation: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const info = yield* fs
    .stat(path)
    .pipe(
      Effect.mapError((cause) => dataError(operation, `Could not inspect ${path}`, path, cause)),
    );
  if (info.type !== "File") {
    return yield* dataError(operation, "Input path is not a regular file", path);
  }
  if (info.size > maximumBytes) {
    return yield* dataError(
      operation,
      `Input exceeds the ${maximumBytes.toString()} byte limit`,
      path,
    );
  }
  return yield* fs
    .readFileString(path)
    .pipe(Effect.mapError((cause) => dataError(operation, `Could not read ${path}`, path, cause)));
});

export const loadObservationFile = Effect.fn("PrReviewEval.loadObservationFile")(function* (
  path: string,
): Effect.fn.Return<ReadonlyArray<EvalObservation>, EvalDataError, FileSystem.FileSystem> {
  const contents = yield* readBoundedFile(path, MAX_OBSERVATION_BYTES, "read observations");
  return yield* decodeObservationLines(contents);
});

export const loadObservationFiles = Effect.fn("PrReviewEval.loadObservationFiles")(function* (
  paths: ReadonlyArray<string>,
) {
  const batches = yield* Effect.forEach(paths, loadObservationFile);
  return batches.flat();
});

export const loadJudgmentSet = Effect.fn("PrReviewEval.loadJudgmentSet")(function* (path: string) {
  const contents = yield* readBoundedFile(path, MAX_JUDGMENT_BYTES, "read judgments");
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(EvalJudgmentSet))(contents).pipe(
    Effect.mapError((cause) =>
      dataError("decode judgments", `Judgment set at ${path} is invalid`, path, cause),
    ),
  );
});

const encodeQualityReport = Schema.encodeEffect(Schema.fromJsonString(EvalQualityReport));

export const writeQualityReport = Effect.fn("PrReviewEval.writeQualityReport")(function* (
  path: string,
  report: EvalQualityReport,
) {
  const contents = yield* encodeQualityReport(report).pipe(
    Effect.mapError((cause) =>
      dataError("encode quality report", "Quality report failed to encode", path, cause),
    ),
  );
  const fs = yield* FileSystem.FileSystem;
  yield* fs
    .writeFileString(path, `${contents}\n`, { flag: "wx" })
    .pipe(
      Effect.mapError((cause) =>
        dataError("write quality report", `Could not write quality report to ${path}`, path, cause),
      ),
    );
});
