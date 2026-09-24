import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import type { CanonicalRecordEnvelope } from "effect-agent/records";
import { CanonicalSequence } from "effect-agent/records";
import {
  ThreadProjectionError,
  ThreadProjectionMaintenance,
} from "effect-agent/thread-projection-maintenance";
import { ThreadRead, ThreadStore, ThreadTailRequest } from "effect-agent/thread-store";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { ThreadHostMaintenance, type ThreadHostMaintenanceLane } from "../src/Alarm.ts";
import { ThreadObjectIdentity } from "../src/CloudflareBindings.ts";

interface ProjectionControl {
  readonly operation?: "live" | "drain";
  readonly skipLive?: boolean;
  readonly retryAt?: number;
  readonly entered?: () => void;
  readonly release?: Promise<void>;
}

export const projectionControls = new Map<string, ProjectionControl>();

export const hostMaintenanceControls = new Map<string, ReadonlyArray<ThreadHostMaintenanceLane>>();

export const hostMaintenanceLayer = Layer.effectContext(
  Effect.gen(function* () {
    const { threadId } = yield* ThreadObjectIdentity;

    return Context.make(ThreadHostMaintenance, {
      get lanes() {
        return hostMaintenanceControls.get(threadId) ?? [];
      },
    });
  }),
);

export class ProjectionIndex extends Context.Service<
  ProjectionIndex,
  {
    readonly lookup: Effect.Effect<number, ThreadProjectionError>;
    readonly watermark: Effect.Effect<number, ThreadProjectionError>;
    readonly ownerSql: SqlClient;
  }
>()("test/ProjectionIndex") {}

const failure = (cause?: unknown) =>
  ThreadProjectionError.make({
    operation: "test projection",
    message: "derived index unavailable",
    cause,
  });

/** Real SQLite index and atomic cursor; only fault timing and the model are controlled. */
export const projectionLayer = Layer.effectContext(
  Effect.gen(function* () {
    const { threadId } = yield* ThreadObjectIdentity;
    const store = yield* ThreadStore;
    const sql = yield* SqlClient;

    yield* sql`CREATE TABLE IF NOT EXISTS test_projection_rows (sequence INTEGER PRIMARY KEY, record_id TEXT NOT NULL)`;
    yield* sql`CREATE TABLE IF NOT EXISTS test_projection_cursor (singleton INTEGER PRIMARY KEY, watermark INTEGER NOT NULL)`;
    yield* sql`INSERT OR IGNORE INTO test_projection_cursor VALUES (1, 0)`;

    const watermark = sql`SELECT watermark FROM test_projection_cursor WHERE singleton = 1`.pipe(
      Effect.flatMap(
        Schema.decodeUnknownEffect(
          Schema.NonEmptyArray(Schema.Struct({ watermark: Schema.Natural })),
        ),
      ),
      Effect.map((rows) => rows[0].watermark),
      Effect.mapError(failure),
    );

    const tail = store.inspectTail(ThreadTailRequest.make({ threadId })).pipe(
      Effect.map((value) => value.tailSequence),
      Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed(0)),
      Effect.mapError(failure),
    );

    const fault = Effect.fn("projectionFixture.fault")(function* (
      operation: "live" | "drain",
      stage: "before" | "after",
    ) {
      const control = projectionControls.get(threadId);

      if (control?.operation !== operation || stage !== "before") return;
      control.entered?.();
      const release = control.release;

      if (release !== undefined) yield* Effect.promise(() => release);
    });

    const batch = Effect.fn("projectionFixture.batch")(function* (
      through: number,
      limit: number,
      operation: "live" | "drain",
    ) {
      const before = yield* watermark;

      if (before >= through) return;
      const count = Math.min(limit, through - before);

      const records: ReadonlyArray<CanonicalRecordEnvelope> = yield* store
        .read(
          ThreadRead.make({
            threadId,
            afterSequence: CanonicalSequence.make(before),
            limit: count,
          }),
        )
        .pipe(Stream.runCollect, Effect.mapError(failure));

      if (
        records.length !== count ||
        records.some((record, index) => record.sequence !== before + index + 1)
      )
        return yield* failure();
      yield* fault(operation, "before");
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            if ((yield* watermark) !== before) return;
            for (const record of records)
              yield* sql`INSERT INTO test_projection_rows VALUES (${record.sequence}, ${record.record.recordId})`;
            yield* sql`UPDATE test_projection_cursor SET watermark = ${before + records.length} WHERE singleton = 1`;
          }),
        )
        .pipe(Effect.mapError(failure));
      yield* fault(operation, "after");
    }, Effect.scoped);

    const lookup = Effect.gen(function* () {
      const captured = yield* tail;
      const indexed = yield* watermark;

      if (indexed < captured) return yield* failure();

      return indexed;
    });

    return Context.make(ProjectionIndex, { lookup, watermark, ownerSql: sql }).pipe(
      Context.add(ThreadProjectionMaintenance, {
        applyCommitted: (request, result) =>
          Effect.gen(function* () {
            if (
              projectionControls.get(threadId)?.skipLive ||
              (yield* watermark) < result.firstSequence - 1
            )
              return;
            yield* batch(result.lastSequence, request.batch.records.length, "live");
          }),
        drain: Effect.gen(function* () {
          yield* batch(yield* tail, 4, "drain");
        }),
        pendingDeadline: Effect.gen(function* () {
          return (yield* watermark) >= (yield* tail)
            ? Option.none()
            : Option.some(projectionControls.get(threadId)?.retryAt ?? 0);
        }),
      }),
    );
  }),
);
