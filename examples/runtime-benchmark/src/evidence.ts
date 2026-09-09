import { Effect, FileSystem } from "effect";

/** A killed writer leaves the previous complete JSON document available to artifact readers. */
export const writeEvidence = Effect.fn("benchmark.writeEvidence")(function* (
  filename: string,
  contents: string,
) {
  const fs = yield* FileSystem.FileSystem;

  yield* fs.writeFileString(`${filename}.tmp`, contents);
  yield* fs.rename(`${filename}.tmp`, filename);
});
