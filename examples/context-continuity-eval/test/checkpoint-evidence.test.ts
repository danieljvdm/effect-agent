import { ThreadId } from "@effect-agent/core/Identifiers";
import {
  CheckpointRejected,
  ThreadStore,
  ThreadStoreError,
} from "@effect-agent/thread/ThreadStore";
import { Cause, Effect, Exit, Option, Schema, Stream } from "effect";
import { expect, it } from "vite-plus/test";

import { readRecoveryCheckpoint } from "../src/host-evidence.ts";

it.each(["unsupported", "missing", "rejected", "storage-failure"] as const)(
  "observes %s cache evidence without replacing canonical authority",
  async (mode) => {
    const threadId = Schema.decodeSync(ThreadId)("checkpoint-evidence");

    const unavailable = ThreadStoreError.make({
      operation: "load",
      message: "storage unavailable",
    });

    const store = ThreadStore.of({
      materialize: () => Effect.die("Evidence must not mutate"),
      append: () => Effect.die("Evidence must not mutate"),
      read: () => Stream.die("Cache observation must not replace canonical evidence capture"),
      observe: () => Stream.die("Evidence must not subscribe"),
      export: () => Effect.die("Evidence must not export"),
      inspectTail: () => Effect.die("Evidence must not inspect a different tail"),
      ...(mode === "unsupported"
        ? {}
        : {
            recoveryCheckpoints: {
              save: () => Effect.die("Evidence must not mutate"),
              load: () =>
                mode === "missing"
                  ? Effect.succeed(Option.none())
                  : mode === "rejected"
                    ? Effect.fail(CheckpointRejected.make({ threadId, reason: "corrupt" }))
                    : Effect.fail(unavailable),
            },
          }),
    });

    const result = await Effect.runPromise(
      readRecoveryCheckpoint(threadId).pipe(Effect.provideService(ThreadStore, store), Effect.exit),
    );

    const observed = Exit.isFailure(result)
      ? { failure: Cause.findErrorOption(result.cause) }
      : { value: result.value };

    expect(observed).toEqual(
      mode === "storage-failure"
        ? { failure: Option.some(unavailable) }
        : {
            value:
              mode === "rejected" ? { status: "rejected", reason: "corrupt" } : { status: mode },
          },
    );
  },
);
