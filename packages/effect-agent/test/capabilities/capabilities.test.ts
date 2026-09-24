import { describe, expect, it } from "@effect/vitest";
import { Clock, Context, Deferred, Effect, Fiber, Schema } from "effect";
import { ThreadId, RunId } from "effect-agent/identifiers";
import {
  type ContextTransformError,
  type ContextTransform,
  prepareModelContext,
} from "effect-agent/model-context";
import {
  ThreadAppend,
  Thread as ThreadSnapshot,
  Store as ConversationStore,
  layerMemory,
  toPrompt,
} from "effect-agent/thread";
import { Prompt } from "effect/unstable/ai";
import type { expectTypeOf as ExpectTypeOf } from "vite-plus/test";

const threadId = Schema.decodeSync(ThreadId)("trip-1");
const runId = Schema.decodeSync(RunId)("run-1");

const textMessage = (role: "system" | "user" | "assistant", content: string): Prompt.Message => {
  const [message] = Prompt.make([{ role, content }]).content;

  if (message === undefined) {
    throw new Error("Expected Prompt.make to preserve its single input message");
  }

  return message;
};

describe("capability contracts", () => {
  it.effect("releases an interrupted history transaction without publishing its suffix", () =>
    Effect.gen(function* () {
      const threads = yield* ConversationStore;

      yield* threads.create(threadId);

      const base = yield* threads.append(
        threadId,
        ThreadAppend.make({ message: textMessage("user", "base") }),
      );

      const history = Prompt.fromMessages([
        ...toPrompt(base).content,
        textMessage("assistant", "suffix"),
      ]);

      const clock = yield* Clock.Clock;

      {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let finalized = false;

        const timestamp = Effect.acquireUseRelease(
          Effect.void,
          () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          () =>
            Effect.sync(() => {
              finalized = true;
            }),
        );

        const record = threads
          .recordHistory(threadId, runId, history)
          .pipe(Effect.provideService(Clock.Clock, { ...clock, currentTimeMillis: timestamp }));

        const fiber = yield* record.pipe(Effect.forkChild);

        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        expect(finalized).toBe(true);
        expect(yield* threads.snapshot(threadId)).toEqual(base);
      }
      const committed = yield* threads.recordHistory(threadId, runId, history);

      expect(committed.nextSequence).toBe(2);
      expect(committed.messages).toHaveLength(2);
    }).pipe(Effect.provide(layerMemory)),
  );
});

export const verifyRetainsContextTransformRequirementsInTuplesAndArrays = () => {
  class Prefix extends Context.Service<Prefix, string>()("test/ContextPrefix") {}
  class Suffix extends Context.Service<Suffix, string>()("test/ContextSuffix") {}

  const snapshot = ThreadSnapshot.make({
    version: 1,
    threadId,
    nextSequence: 0,
    contentBytes: 0,
    messages: [],
  });

  const prefix: ContextTransform<Prefix> = {
    id: "prefix",
    version: "1",
    apply: (messages) => Effect.as(Prefix, messages),
  };

  const suffix: ContextTransform<Suffix> = {
    id: "suffix",
    version: "1",
    apply: (messages) => Effect.as(Suffix, messages),
  };

  const fromTuple = prepareModelContext(snapshot, [prefix, suffix]);
  const transforms = [prefix, suffix];
  const fromArray = prepareModelContext(snapshot, transforms);

  expectTypeOf<Effect.Services<typeof fromTuple>>().toEqualTypeOf<Prefix | Suffix>();
  expectTypeOf<Effect.Services<typeof fromArray>>().toEqualTypeOf<Prefix | Suffix>();
  expectTypeOf<Effect.Error<typeof fromTuple>>().toEqualTypeOf<ContextTransformError>();
};

declare const expectTypeOf: typeof ExpectTypeOf;
