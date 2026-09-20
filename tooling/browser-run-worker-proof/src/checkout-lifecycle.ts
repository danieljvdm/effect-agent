import { Effect, FileSystem, Schema } from "effect";

import { Report } from "./checkout-contract.ts";

/** An absent owner proves nothing about its browsers without retained closure acknowledgements. */
export const retirementPlan = (
  ownerStatus: number,
  cleanup: typeof Report.Type.cleanup,
  recordedClosures: ReadonlyArray<boolean>,
) =>
  ownerStatus === 200
    ? "close"
    : ownerStatus === 404 &&
        (cleanup === "browsers-closed" ||
          cleanup === "confirmed" ||
          recordedClosures.every((closed) => closed))
      ? "destroy"
      : "blocked";

/** Publish a whole recovery snapshot; interruption keeps either the preceding or next report. */
export const writeReportSnapshot = Effect.fnUntraced(function* (
  path: string,
  report: typeof Report.Type,
) {
  const fs = yield* FileSystem.FileSystem;
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Report))(report);

  yield* fs.writeFileString(`${path}.tmp`, encoded);
  yield* fs.rename(`${path}.tmp`, path);
});

/** Register retirement before deployment, including partially acknowledged provisioning. */
export const withRetirement = <A, E, R, E2, R2>(
  run: Effect.Effect<A, E, R>,
  retire: Effect.Effect<void, E2, R2>,
) => run.pipe(Effect.ensuring(retire.pipe(Effect.orDie)));
