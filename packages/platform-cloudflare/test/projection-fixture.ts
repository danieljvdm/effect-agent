import * as Agent from "@effect-agent/core/Agent";
import { DurableWorkerBinding } from "@effect-agent/thread/AgentRegistration";
import type { CanonicalRecordEnvelope } from "@effect-agent/thread/Records";
import { CanonicalSequence } from "@effect-agent/thread/Records";
import {
  ThreadProjectionError,
  ThreadProjectionMaintenance,
} from "@effect-agent/thread/ThreadProjectionMaintenance";
import { ThreadRead, ThreadStore, ThreadTailRequest } from "@effect-agent/thread/ThreadStore";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { DurableObjectContext, ThreadObjectIdentity } from "../src/CloudflareBindings.ts";
import { TEST_DIGESTS, finalParts, plannerDefinition } from "./fixtures.ts";

interface ProjectionControl {
  readonly operation?: "live" | "drain";
  readonly failure?: "failure" | "defect" | "interruption" | "timeout" | "eviction";
  readonly stage?: "before" | "after";
  readonly skipLive?: boolean;
  readonly retryAt?: number;
  readonly entered?: () => void;
  readonly release?: Promise<void>;
}

export const projectionControls = new Map<string, ProjectionControl>();
export const projectionResources = new Map<string, { acquired: number; released: number }>();
export const projectionConstructions = new Map<string, number>();
export const projectionLookups = new Map<string, Array<number>>();
export const projectionLiveBatches = new Map<string, Array<number>>();

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
    const { ctx } = yield* DurableObjectContext;
    const { threadId } = yield* ThreadObjectIdentity;
    const store = yield* ThreadStore;
    const sql = yield* SqlClient;

    projectionConstructions.set(threadId, (projectionConstructions.get(threadId) ?? 0) + 1);
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

      if (control?.operation !== operation || (control.stage ?? "before") !== stage) return;
      control.entered?.();
      const release = control.release;

      if (release !== undefined) yield* Effect.promise(() => release);
      switch (control.failure) {
        case undefined:
          return;
        case "failure":
          return yield* failure();
        case "defect":
          return yield* Effect.die("projection defect");
        case "interruption":
          return yield* Effect.interrupt;
        case "timeout":
          return yield* Effect.never.pipe(Effect.timeoutOrElse({ duration: 0, orElse: failure }));
        case "eviction":
          projectionControls.delete(threadId);

          return yield* Effect.sync(() => ctx.abort("projection commit failpoint"));
      }
    });

    const batch = Effect.fn("projectionFixture.batch")(function* (
      through: number,
      limit: number,
      operation: "live" | "drain",
    ) {
      const resources = projectionResources.get(threadId) ?? { acquired: 0, released: 0 };

      projectionResources.set(threadId, resources);
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          resources.acquired++;
        }),
        () =>
          Effect.sync(() => {
            resources.released++;
          }),
      );
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
      const lookups = projectionLookups.get(threadId) ?? [];

      lookups.push(indexed);
      projectionLookups.set(threadId, lookups);

      return indexed;
    });

    return Context.make(ProjectionIndex, { lookup, watermark, ownerSql: sql }).pipe(
      Context.add(ThreadProjectionMaintenance, {
        applyCommitted: (request, result) =>
          Effect.gen(function* () {
            const batches = projectionLiveBatches.get(threadId) ?? [];

            batches.push(request.batch.records.length);
            projectionLiveBatches.set(threadId, batches);
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

const lookupTools = Toolkit.make(
  Tool.make("lookup_projection", {
    parameters: Schema.Struct({}),
    success: Schema.Natural,
    failure: ThreadProjectionError,
    dependencies: [ProjectionIndex],
  }),
);

export const projectionDefinition = Agent.make("cf-projection", {
  input: plannerDefinition.input,
  output: plannerDefinition.output,
  instructions: "Look up earlier evidence three times, then finish.",
  toolkit: lookupTools,
  policy: { maxTurns: 5, maxToolCalls: 4, maxDuration: "30 seconds" },
});

const lookupModel = Model.make(
  "scripted",
  "projection",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: (options) => {
        const prompt = JSON.stringify(options.prompt);
        const next = [1, 2, 3].find((turn) => !prompt.includes(`projection-lookup-${turn}`));

        return Stream.fromIterable(
          next === undefined
            ? finalParts('{"answer":"done"}')
            : [
                {
                  type: "tool-call",
                  id: `projection-lookup-${next}`,
                  name: "lookup_projection",
                  params: {},
                  providerExecuted: false,
                },
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: { inputTokens: {}, outputTokens: {} },
                },
              ],
        );
      },
    }),
  ),
);

export const makeProjectionBinding = DurableWorkerBinding.make(
  Agent.withModel(projectionDefinition, lookupModel),
  TEST_DIGESTS,
).pipe(
  Effect.provide(
    lookupTools.toLayer({
      lookup_projection: () => Effect.flatMap(ProjectionIndex, (index) => index.lookup),
    }),
  ),
);
