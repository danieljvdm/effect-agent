import { RecoverySnapshotRequest, SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import { ThreadStore, ThreadTailRequest } from "@effect-agent/thread/ThreadStore";
import { Clock, Effect, Layer, Option, Schema, Stream } from "effect";

import { ThreadPublication, DurableAlarmError } from "../src/Alarm.ts";
import { DurableObjectContext, ThreadObjectIdentity } from "../src/CloudflareBindings.ts";

export const PublicationCursor = Schema.Struct({
  version: Schema.Literal(1),
  generation: Schema.String,
  source: Schema.Natural,
  tail: Schema.Natural,
  dirty: Schema.Boolean,
  decisions: Schema.Array(Schema.String),
});

export const PUBLICATION_KEY = "test:publication:cursor";
export const SOURCE_KEY = "test:publication:source";

export interface PublicationControl {
  readonly entered?: () => void;
  readonly release?: Promise<void>;
  readonly failure?: "failure" | "defect" | "interruption" | "timeout";
  readonly retryAt?: number;
}

export const publicationControls = new Map<string, PublicationControl>();
export const publicationResources = new Map<string, { acquired: number; released: number }>();
export const publicationPreparations = new Map<string, Array<string>>();

/** Persistent host cursor over the real local source ports; only the destination is controlled. */
export const publicationLayer = Layer.effect(ThreadPublication)(
  Effect.gen(function* () {
    const { ctx } = yield* DurableObjectContext;
    const { threadId } = yield* ThreadObjectIdentity;
    const store = yield* ThreadStore;
    const ledger = yield* SubmissionLedger;

    const read = Effect.promise(() => ctx.storage.get(PUBLICATION_KEY)).pipe(
      Effect.flatMap((value) =>
        value === undefined
          ? Effect.succeed({
              version: 1 as const,
              generation: "",
              source: 0,
              tail: 0,
              dirty: true,
              decisions: [] as Array<string>,
            })
          : Schema.decodeUnknownEffect(PublicationCursor)(value),
      ),
    );

    const save = (cursor: typeof PublicationCursor.Type) =>
      Schema.encodeEffect(PublicationCursor)(cursor).pipe(
        Effect.flatMap((value) => Effect.promise(() => ctx.storage.put(PUBLICATION_KEY, value))),
      );

    const source = Effect.promise(() => ctx.storage.get(SOURCE_KEY)).pipe(
      Effect.flatMap((value) => Schema.decodeUnknownEffect(Schema.Natural)(value ?? 0)),
    );

    const tail = store.inspectTail(ThreadTailRequest.make({ threadId })).pipe(
      Effect.map((tail) => tail.tailSequence),
      Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed(0)),
    );

    const failure = (cause: unknown) =>
      DurableAlarmError.make({
        operation: "host publication",
        message: "host publication failed",
        cause,
      });

    return ThreadPublication.of({
      invalidate: Effect.gen(function* () {
        yield* save({ ...(yield* read), dirty: true });
      }).pipe(Effect.mapError(failure)),
      prepareGeneration: (generation) =>
        Effect.gen(function* () {
          const cursor = yield* read;

          if (cursor.generation === String(generation)) return;
          const calls = publicationPreparations.get(threadId) ?? [];

          calls.push(String(generation));
          publicationPreparations.set(threadId, calls);
          yield* save({ ...cursor, generation: String(generation), dirty: true });
        }).pipe(Effect.mapError(failure)),
      drain: Effect.gen(function* () {
        const resources = publicationResources.get(threadId) ?? { acquired: 0, released: 0 };

        publicationResources.set(threadId, resources);
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            resources.acquired++;
          }),
          () =>
            Effect.sync(() => {
              resources.released++;
            }),
        );
        const cursor = yield* read;
        const currentSource = yield* source;
        const currentTail = yield* tail;
        const control = publicationControls.get(threadId);

        if (control?.retryAt !== undefined) return;
        if (control?.failure === "failure") return yield* failure("retryable destination failure");
        if (control?.failure === "defect") return yield* Effect.die("destination defect");
        if (control?.failure === "interruption") return yield* Effect.interrupt;
        if (control?.failure === "timeout")
          return yield* Effect.never.pipe(Effect.timeout("0 millis"));
        control?.entered?.();
        const release = control?.release;

        if (release !== undefined) yield* Effect.promise(() => release);
        const rows = yield* Stream.runCollect(ledger.scanNonterminal);
        const decisions: Array<string> = [];

        for (const row of rows) {
          const snapshot = yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: row.submissionId }),
          );

          decisions.push(...snapshot.approvalDecisions.map((intent) => intent.decision));
        }
        yield* save({
          ...cursor,
          source: currentSource,
          tail: currentTail,
          dirty: false,
          decisions: [...new Set([...cursor.decisions, ...decisions])],
        });
      }).pipe(Effect.scoped, Effect.mapError(failure)),
      pendingDeadline: Effect.gen(function* () {
        const cursor = yield* read;

        if (!cursor.dirty && cursor.source >= (yield* source) && cursor.tail >= (yield* tail))
          return Option.none();

        return Option.some(
          publicationControls.get(threadId)?.retryAt ?? (yield* Clock.currentTimeMillis),
        );
      }).pipe(Effect.mapError(failure)),
    });
  }),
);
