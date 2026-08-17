import {
  GitCommitSha,
  type PullRequestWorkOrder,
  PublishedWorkOrder,
  SettledWorkOrder,
} from "@effect-agent/example-pr-work-orders";
import { Context, Effect, FileSystem, Layer, Path, Schema } from "effect";

import { AttemptIncomplete, IngressStoreFailure, StoredDeliveryFailure } from "./contracts.ts";

const AttemptFailure = Schema.Struct({
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  detail: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
});

export class AttemptSnapshot extends Schema.Class<AttemptSnapshot>(
  "@effect-agent/example-pr-work-order-ingress/AttemptSnapshot",
)({
  eventId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  headSha: GitCommitSha,
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  status: Schema.Literals(["claimed", "completed"]),
  result: Schema.optionalKey(Schema.Union([PublishedWorkOrder, SettledWorkOrder])),
  failure: Schema.optionalKey(AttemptFailure),
}) {}

export type ClaimResult =
  | { readonly _tag: "claimed" }
  | { readonly _tag: "duplicate"; readonly snapshot: AttemptSnapshot }
  | { readonly _tag: "incomplete"; readonly snapshot: AttemptSnapshot };

export const IngressStoreFailpointLocation = Schema.Literals([
  "before-claim-event",
  "after-claim-event",
  "before-claim-key",
  "after-claim-key",
  "before-complete-event",
  "after-complete-event",
  "before-complete-key",
  "after-complete-key",
]);
export type IngressStoreFailpointLocation = typeof IngressStoreFailpointLocation.Type;

export class IngressStoreFailpoint extends Context.Service<
  IngressStoreFailpoint,
  {
    readonly hit: (location: IngressStoreFailpointLocation) => Effect.Effect<void>;
  }
>()("@effect-agent/example-pr-work-order-ingress/IngressStoreFailpoint") {
  static readonly layer = Layer.succeed(
    IngressStoreFailpoint,
    IngressStoreFailpoint.of({ hit: () => Effect.void }),
  );
}

export class FileBackedAttemptStore extends Context.Service<
  FileBackedAttemptStore,
  {
    readonly claim: (
      order: PullRequestWorkOrder,
    ) => Effect.Effect<ClaimResult, IngressStoreFailure>;
    readonly complete: (
      order: PullRequestWorkOrder,
      outcome:
        | { readonly _tag: "result"; readonly result: PublishedWorkOrder | SettledWorkOrder }
        | { readonly _tag: "failure"; readonly errorTag: string; readonly detail: string },
    ) => Effect.Effect<void, IngressStoreFailure>;
  }
>()("@effect-agent/example-pr-work-order-ingress/FileBackedAttemptStore") {
  static readonly layer = (directory: string) =>
    Layer.effect(
      FileBackedAttemptStore,
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const failpoint = yield* IngressStoreFailpoint;
        const hit = failpoint.hit;
        const eventsDir = path.join(directory, "events");
        const attemptsDir = path.join(directory, "attempts");
        const ensureDirectories = Effect.gen(function* () {
          yield* fs.makeDirectory(eventsDir, { recursive: true });
          yield* fs.makeDirectory(attemptsDir, { recursive: true });
        }).pipe(
          Effect.mapError((cause) =>
            IngressStoreFailure.make({
              operation: "create attempt store",
              reason: String(cause).slice(0, 4_096),
            }),
          ),
        );
        const eventPath = (eventId: string) =>
          path.join(eventsDir, `${encodeURIComponent(eventId)}.json`);
        const attemptPath = (order: PullRequestWorkOrder) =>
          path.join(
            attemptsDir,
            `${encodeURIComponent(`${order.repository}#${String(order.pullRequestNumber)}@${order.headSha}#${order.workOrderId}`)}.json`,
          );
        const readSnapshot = (file: string) =>
          fs.readFileString(file).pipe(
            Effect.flatMap((text) =>
              Schema.decodeUnknownEffect(Schema.fromJsonString(AttemptSnapshot))(text),
            ),
            Effect.mapError((cause) =>
              IngressStoreFailure.make({
                operation: "read attempt snapshot",
                reason: String(cause).slice(0, 4_096),
              }),
            ),
          );
        const writeSnapshot = (file: string, snapshot: AttemptSnapshot, exclusive: boolean) =>
          Schema.encodeEffect(Schema.fromJsonString(AttemptSnapshot))(snapshot).pipe(
            Effect.flatMap((text) => {
              if (exclusive) {
                return fs.writeFileString(file, text, { flag: "wx" });
              }
              const next = `${file}.next`;
              return fs.writeFileString(next, text).pipe(Effect.andThen(fs.rename(next, file)));
            }),
            Effect.mapError((cause) =>
              IngressStoreFailure.make({
                operation: "write attempt snapshot",
                reason: String(cause).slice(0, 4_096),
              }),
            ),
          );
        const exists = (file: string) =>
          fs.exists(file).pipe(
            Effect.mapError((cause) =>
              IngressStoreFailure.make({
                operation: "stat attempt snapshot",
                reason: String(cause).slice(0, 4_096),
              }),
            ),
          );
        const existingOf = (
          snapshot: AttemptSnapshot,
        ): Exclude<ClaimResult, { readonly _tag: "claimed" }> =>
          snapshot.status === "claimed"
            ? { _tag: "incomplete", snapshot }
            : { _tag: "duplicate", snapshot };
        const claim = Effect.fn("FileBackedAttemptStore.claim")(function* (
          order: PullRequestWorkOrder,
        ) {
          yield* ensureDirectories;
          const claimed = AttemptSnapshot.make({
            eventId: order.dispatch.eventId,
            repository: order.repository,
            pullRequestNumber: order.pullRequestNumber,
            headSha: order.headSha,
            workOrderId: order.workOrderId,
            status: "claimed",
          });
          const eventFile = eventPath(order.dispatch.eventId);
          const keyFile = attemptPath(order);
          const eventExists = yield* exists(eventFile);
          if (eventExists) return existingOf(yield* readSnapshot(eventFile));
          const keyExists = yield* exists(keyFile);
          if (keyExists) return existingOf(yield* readSnapshot(keyFile));
          yield* hit("before-claim-event");
          const written = yield* writeSnapshot(eventFile, claimed, true).pipe(
            Effect.as(true),
            Effect.catch((error: IngressStoreFailure) =>
              exists(eventFile).pipe(
                Effect.flatMap((present) => (present ? Effect.succeed(false) : Effect.fail(error))),
              ),
            ),
          );
          if (!written) return existingOf(yield* readSnapshot(eventFile));
          yield* hit("after-claim-event");
          yield* hit("before-claim-key");
          yield* writeSnapshot(keyFile, claimed, false);
          yield* hit("after-claim-key");
          return { _tag: "claimed" as const };
        });
        const complete = Effect.fn("FileBackedAttemptStore.complete")(function* (
          order: PullRequestWorkOrder,
          outcome:
            | { readonly _tag: "result"; readonly result: PublishedWorkOrder | SettledWorkOrder }
            | { readonly _tag: "failure"; readonly errorTag: string; readonly detail: string },
        ) {
          yield* ensureDirectories;
          const snapshot = AttemptSnapshot.make({
            eventId: order.dispatch.eventId,
            repository: order.repository,
            pullRequestNumber: order.pullRequestNumber,
            headSha: order.headSha,
            workOrderId: order.workOrderId,
            status: "completed",
            ...(outcome._tag === "result"
              ? { result: outcome.result }
              : { failure: { errorTag: outcome.errorTag, detail: outcome.detail } }),
          });
          yield* hit("before-complete-event");
          yield* writeSnapshot(eventPath(order.dispatch.eventId), snapshot, false);
          yield* hit("after-complete-event");
          yield* hit("before-complete-key");
          yield* writeSnapshot(attemptPath(order), snapshot, false);
          yield* hit("after-complete-key");
        });
        return FileBackedAttemptStore.of({ claim, complete });
      }),
    );
}

export const replaySnapshot = (
  snapshot: AttemptSnapshot,
): Effect.Effect<
  PublishedWorkOrder | SettledWorkOrder,
  AttemptIncomplete | StoredDeliveryFailure
> => {
  if (snapshot.status === "claimed") {
    return AttemptIncomplete.make({
      eventId: snapshot.eventId,
      workOrderId: snapshot.workOrderId,
    });
  }
  if (snapshot.result !== undefined) {
    return Effect.succeed(snapshot.result);
  }
  return StoredDeliveryFailure.make({
    errorTag: snapshot.failure?.errorTag ?? "unknown",
    detail: snapshot.failure?.detail ?? "stored attempt failed",
  });
};
