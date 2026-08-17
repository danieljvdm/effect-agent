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

export class DurableAttemptStore extends Context.Service<
  DurableAttemptStore,
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
>()("@effect-agent/example-pr-work-order-ingress/DurableAttemptStore") {
  static readonly layer = (directory: string) =>
    Layer.effect(
      DurableAttemptStore,
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
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
            Effect.flatMap((text) =>
              fs.writeFileString(file, text, exclusive ? { flag: "wx" } : undefined),
            ),
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
        const claim = Effect.fn("DurableAttemptStore.claim")(function* (
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
          const written = yield* writeSnapshot(eventFile, claimed, true).pipe(
            Effect.as(true),
            Effect.catch((error: IngressStoreFailure) =>
              exists(eventFile).pipe(
                Effect.flatMap((present) => (present ? Effect.succeed(false) : Effect.fail(error))),
              ),
            ),
          );
          if (!written) return existingOf(yield* readSnapshot(eventFile));
          yield* writeSnapshot(keyFile, claimed, false);
          return { _tag: "claimed" as const };
        });
        const complete = Effect.fn("DurableAttemptStore.complete")(function* (
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
          yield* writeSnapshot(eventPath(order.dispatch.eventId), snapshot, false);
          yield* writeSnapshot(attemptPath(order), snapshot, false);
        });
        return DurableAttemptStore.of({ claim, complete });
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
