import { ReviewRequest } from "@effect-agent/pr-review/Review";
import { Crypto, Effect, Encoding, FileSystem, Schema, Stream } from "effect";

import {
  EvalDataError,
  type EvalCase,
  EvalCaseKind,
  EvalExpectedDefect,
  EvalInputDigest,
  EvalObservation,
  type EvalRepositorySnapshot,
  EvalRepositoryFile,
  EvalSuite,
} from "./contracts.ts";

const MAX_SUITE_BYTES = 64n * 1_024n * 1_024n;
const decodeSuiteJson = Schema.decodeUnknownEffect(Schema.fromJsonString(EvalSuite));
const encodeRequestJson = Schema.encodeEffect(Schema.fromJsonString(ReviewRequest));

const encodeRepositoryJson = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({ version: Schema.Literal(1), files: Schema.Array(EvalRepositoryFile) }),
  ),
);

const encodeObservationJson = Schema.encodeEffect(Schema.fromJsonString(EvalObservation));

const Oracle = Schema.Struct({
  version: Schema.Int.check(Schema.isGreaterThan(0)),
  kind: EvalCaseKind,
  expectedDefects: Schema.Array(EvalExpectedDefect),
});

/** Oracle identity is separate from request identity, including independently judged severity. */
export const digestEvalOracle = Effect.fn("PrReviewEval.digestEvalOracle")(function* (
  evalCase: EvalCase,
) {
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Oracle))({
    version: evalCase.oracleVersion ?? 1,
    kind: evalCase.kind,
    expectedDefects: evalCase.expectedDefects,
  }).pipe(
    Effect.mapError((cause) =>
      dataError("encode oracle", "Eval oracle failed canonical encoding", { cause }),
    ),
  );

  return yield* digestText(encoded);
});

export const digestObservation = Effect.fn("PrReviewEval.digestObservation")(function* (
  observation: EvalObservation,
) {
  const encoded = yield* encodeObservationJson(observation).pipe(
    Effect.mapError((cause) =>
      dataError("encode observation", "Eval observation failed canonical encoding", { cause }),
    ),
  );

  return yield* digestText(encoded);
});

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

export const digestRepositorySnapshot = Effect.fn("PrReviewEval.digestRepositorySnapshot")(
  function* (snapshot: EvalRepositorySnapshot) {
    const encoded = yield* encodeRepositoryJson({
      version: snapshot.version,
      files: snapshot.files,
    }).pipe(
      Effect.mapError((cause) =>
        dataError("encode repository", "Frozen repository snapshot failed canonical encoding", {
          cause,
        }),
      ),
    );

    return yield* digestText(encoded);
  },
);

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
      if (
        evalCase.oracleDigest !== undefined &&
        evalCase.oracleDigest !== (yield* digestEvalOracle(evalCase))
      ) {
        return yield* dataError(
          "validate oracle digest",
          `Case ${evalCase.id} has a mismatched oracle identity; correct and version the oracle explicitly`,
        );
      }
      if (evalCase.repository !== undefined) {
        const repositoryDigest = yield* digestRepositorySnapshot(evalCase.repository);

        if (repositoryDigest !== evalCase.repository.digest) {
          return yield* dataError(
            "validate repository digest",
            `Case ${evalCase.id} has repository digest ${evalCase.repository.digest}, expected ${repositoryDigest}`,
          );
        }
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

export const writeObservations = Effect.fn("PrReviewEval.writeObservations")(function* <E, R>(
  output: string,
  observations: Stream.Stream<EvalObservation, E, R>,
) {
  const fs = yield* FileSystem.FileSystem;

  const file = yield* fs.open(output, { flag: "wx", mode: 0o600 }).pipe(
    Effect.mapError((cause) =>
      dataError("open observations", `Could not create new eval observations at ${output}`, {
        path: output,
        cause,
      }),
    ),
  );

  const encoder = new TextEncoder();

  return yield* observations.pipe(
    Stream.runFoldEffect(
      () => 0,
      Effect.fn("PrReviewEval.writeObservation")(function* (
        count: number,
        observation: EvalObservation,
      ) {
        const line = yield* encodeObservationJson(observation).pipe(
          Effect.mapError((cause) =>
            dataError("encode observations", "Eval observation failed to encode", { cause }),
          ),
        );

        yield* file.writeAll(encoder.encode(`${line}\n`)).pipe(
          Effect.andThen(file.sync),
          Effect.uninterruptible,
          Effect.mapError((cause) =>
            dataError("write observations", `Could not persist eval observation to ${output}`, {
              path: output,
              cause,
            }),
          ),
        );

        return count + 1;
      }),
    ),
  );
}, Effect.scoped);

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
