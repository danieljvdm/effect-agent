import { Context, Effect } from "effect";
import * as Memory from "effect-agent/memory";
import {
  type MemoryLookup,
  MemoryRecallError,
  MemoryRecallLimits,
} from "effect-agent/memory-reference";

const limits = MemoryRecallLimits.make({
  maxSources: 8,
  maxItems: 8,
  maxBytes: 16_384,
  maxTokens: 16_384,
  timeoutMillis: 1_000,
});

class Corpus extends Context.Service<
  Corpus,
  { readonly read: Effect.Effect<MemoryLookup, "remote-failure"> }
>()("memory-test/Corpus") {}

export const verifyPreservesReaderERAndDischargesOnlyTheRecallOwnedScope = () => {
  const program = Memory.recall(
    [
      {
        id: "typed",
        essential: true,
        read: Effect.acquireRelease(
          Effect.flatMap(Corpus, (corpus) => corpus.read),
          () => Effect.void,
        ),
      },
    ],
    limits,
  );

  const error: Effect.Error<typeof program> = "remote-failure";

  const taggedError: Effect.Error<typeof program> = MemoryRecallError.make({
    reason: "timeout",
    message: "timed out",
  });

  const requirement: [Effect.Services<typeof program>] extends [Corpus] ? true : false = true;
  const readerRequired: [Corpus] extends [Effect.Services<typeof program>] ? true : false = true;

  const exactErrors: [Effect.Error<typeof program>] extends ["remote-failure" | MemoryRecallError]
    ? true
    : false = true;

  void [error, taggedError._tag, requirement, readerRequired, exactErrors];
};
