import { ReviewRequest } from "@effect-agent/pr-review";
import { Crypto, Effect, Encoding, FileSystem, Path, Schema } from "effect";

import { EvalDataError, EvalInputDigest, EvalObservation, EvalSuite } from "./contracts.ts";

const MAX_SUITE_BYTES = 64n * 1_024n * 1_024n;
const decodeSuiteJson = Schema.decodeUnknownEffect(Schema.fromJsonString(EvalSuite));
const encodeRequestJson = Schema.encodeEffect(Schema.fromJsonString(ReviewRequest));
const encodeObservationJson = Schema.encodeEffect(Schema.fromJsonString(EvalObservation));

const dataError = (
  operation: string,
  message: string,
  options?: { readonly path?: string; readonly cause?: unknown },
): EvalDataError =>
  EvalDataError.make({
    operation,
    message,
    ...(options?.path === undefined ? {} : { path: options.path }),
    ...(options?.cause === undefined ? {} : { cause: options.cause }),
  });

export const digestReviewRequest = Effect.fn("PrReviewEval.digestReviewRequest")(function* (
  request: ReviewRequest,
) {
  const encoded = yield* encodeRequestJson(request).pipe(
    Effect.mapError((cause) =>
      dataError("encode request", "ReviewRequest failed to encode as canonical JSON", { cause }),
    ),
  );
  return yield* digestText(encoded);
});

export const digestText = Effect.fn("PrReviewEval.digestText")(function* (text: string) {
  const bytes = yield* Effect.fromResult(Encoding.decodeHex(Encoding.encodeHex(text))).pipe(
    Effect.mapError((cause) => dataError("encode text", "UTF-8 encoding failed", { cause })),
  );
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto
    .digest("SHA-256", bytes)
    .pipe(Effect.mapError((cause) => dataError("digest text", "SHA-256 failed", { cause })));
  return yield* Schema.decodeUnknownEffect(EvalInputDigest)(Encoding.encodeHex(digest)).pipe(
    Effect.mapError((cause) =>
      dataError("digest text", "SHA-256 returned an invalid digest", { cause }),
    ),
  );
});

export const validateEvalSuite = Effect.fn("PrReviewEval.validateEvalSuite")(function* (
  suite: EvalSuite,
) {
  yield* Effect.forEach(
    suite.cases,
    Effect.fn("PrReviewEval.validateCaseDigest")(function* (evalCase) {
      const actual = yield* digestReviewRequest(evalCase.request);
      if (actual !== evalCase.inputDigest) {
        return yield* dataError(
          "validate case digest",
          `Case ${evalCase.id} has input digest ${evalCase.inputDigest}, expected ${actual}`,
        );
      }
    }),
    { discard: true },
  );
  return suite;
});

export const loadEvalSuite = Effect.fn("PrReviewEval.loadEvalSuite")(function* (path: string) {
  const fs = yield* FileSystem.FileSystem;
  const info = yield* fs
    .stat(path)
    .pipe(
      Effect.mapError((cause) =>
        dataError("read suite", `Could not inspect eval suite at ${path}`, { path, cause }),
      ),
    );
  if (info.type !== "File") {
    return yield* dataError("read suite", "Eval suite path is not a regular file", { path });
  }
  if (info.size > MAX_SUITE_BYTES) {
    return yield* dataError(
      "read suite",
      `Eval suite exceeds the ${MAX_SUITE_BYTES.toString()} byte limit`,
      { path },
    );
  }
  const contents = yield* fs
    .readFileString(path)
    .pipe(
      Effect.mapError((cause) =>
        dataError("read suite", `Could not read eval suite at ${path}`, { path, cause }),
      ),
    );
  const suite = yield* decodeSuiteJson(contents).pipe(
    Effect.mapError((cause) =>
      dataError("decode suite", `Eval suite at ${path} is invalid`, { path, cause }),
    ),
  );
  return yield* validateEvalSuite(suite);
});

export const preflightObservationOutput = Effect.fn("PrReviewEval.preflightObservationOutput")(
  function* (output: string) {
    const fs = yield* FileSystem.FileSystem;
    const paths = yield* Path.Path;
    if (yield* fs.exists(output)) {
      return yield* dataError("write observations", "Output path already exists", {
        path: output,
      });
    }
    const parent = paths.dirname(output);
    const parentInfo = yield* fs.stat(parent).pipe(
      Effect.mapError((cause) =>
        dataError("write observations", "Output directory does not exist", {
          path: parent,
          cause,
        }),
      ),
    );
    if (parentInfo.type !== "Directory") {
      return yield* dataError("write observations", "Output parent is not a directory", {
        path: parent,
      });
    }
    yield* fs.access(parent, { writable: true }).pipe(
      Effect.mapError((cause) =>
        dataError("write observations", "Output directory is not writable", {
          path: parent,
          cause,
        }),
      ),
    );
  },
);

export const writeObservations = Effect.fn("PrReviewEval.writeObservations")(function* (
  output: string,
  observations: ReadonlyArray<EvalObservation>,
) {
  const lines = yield* Effect.forEach(observations, (observation) =>
    encodeObservationJson(observation).pipe(
      Effect.mapError((cause) =>
        dataError("encode observations", "Eval observation failed to encode", { cause }),
      ),
    ),
  );
  const contents = `${lines.join("\n")}\n`;
  const fs = yield* FileSystem.FileSystem;
  yield* fs.writeFileString(output, contents, { flag: "wx" }).pipe(
    Effect.mapError((cause) =>
      dataError("write observations", `Could not write eval observations to ${output}`, {
        path: output,
        cause,
      }),
    ),
  );
});

export const decodeObservationLines = Effect.fn("PrReviewEval.decodeObservationLines")(function* (
  contents: string,
) {
  const lines = contents.split("\n").filter((line) => line.trim().length > 0);
  return yield* Effect.forEach(lines, (line, index) =>
    Schema.decodeUnknownEffect(Schema.fromJsonString(EvalObservation))(line).pipe(
      Effect.mapError((cause) =>
        dataError("decode observations", `Observation line ${index + 1} is invalid`, { cause }),
      ),
    ),
  );
});
