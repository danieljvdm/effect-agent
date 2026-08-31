import { ThreadId, RunId } from "@effect-agent/core";
import { Effect, Option, Queue, Schema } from "effect";

import { ThreadText } from "./thread.ts";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

/** Commands are delivered only at the engine's documented safe Turn seams. */
export class SteeringCommand extends Schema.TaggedClass<SteeringCommand>()("SteeringCommand", {
  id: Schema.NonEmptyString,
  runId: RunId,
  threadId: ThreadId,
  author: Schema.NonEmptyString,
  content: ThreadText,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

/** A follow-up remains queued until the Run would otherwise stop. */
export class FollowUpCommand extends Schema.TaggedClass<FollowUpCommand>()("FollowUpCommand", {
  id: Schema.NonEmptyString,
  runId: RunId,
  threadId: ThreadId,
  author: Schema.NonEmptyString,
  content: ThreadText,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

export const RunCommand = Schema.Union([SteeringCommand, FollowUpCommand]);
export type RunCommand = typeof RunCommand.Type;

/** The default drains one command; `all` is explicit so batching is never accidental. */
export const CommandDrainPolicy = Schema.Literals(["one", "all"]);
export type CommandDrainPolicy = typeof CommandDrainPolicy.Type;

/** Queue capacity is finite and uses Queue's suspending backpressure strategy. */
export class RunCommandQueueConfig extends Schema.Class<RunCommandQueueConfig>(
  "@effect-agent/capabilities/RunCommandQueueConfig",
)({ capacity: PositiveInt }) {}

/** A command cannot be admitted after the Run Scope has closed. */
export class RunCommandQueueClosed extends Schema.TaggedError<RunCommandQueueClosed>()(
  "RunCommandQueueClosed",
  { runId: RunId },
) {}

/** Scoped command queue for one active Run. */
export interface RunCommandQueue {
  readonly offer: (command: RunCommand) => Effect.Effect<void, RunCommandQueueClosed>;
  readonly drain: (policy?: CommandDrainPolicy) => Effect.Effect<ReadonlyArray<RunCommand>>;
  readonly shutdown: Effect.Effect<void>;
}

/**
 * Construct a bounded queue in the current Scope. `shutdown` is registered as
 * a finalizer so awaiting producers are interrupted with their parent Run.
 */
export const makeRunCommandQueue = Effect.fn("makeRunCommandQueue")(function* (
  runId: RunId,
  config: RunCommandQueueConfig,
) {
  const queue = yield* Queue.bounded<RunCommand>(config.capacity);
  const shutdown = Queue.shutdown(queue).pipe(Effect.asVoid);
  yield* Effect.addFinalizer(() => shutdown);

  const offer = (command: RunCommand): Effect.Effect<void, RunCommandQueueClosed> =>
    Queue.offer(queue, command).pipe(
      Effect.flatMap((accepted) =>
        accepted ? Effect.void : Effect.fail(RunCommandQueueClosed.make({ runId })),
      ),
    );

  const drainOne = Queue.poll(queue).pipe(Effect.map(Option.toArray));

  return {
    offer,
    drain: (policy = "one") =>
      policy === "one"
        ? drainOne
        : Queue.size(queue).pipe(
            Effect.flatMap((count) =>
              Effect.forEach(Array.from({ length: count }), () => drainOne).pipe(
                Effect.map((batches) => batches.flat()),
              ),
            ),
          ),
    shutdown,
  } satisfies RunCommandQueue;
});
