import { ThreadId } from "@effect-agent/core/Identifiers";
import {
  ActivityProcessorKey,
  type ActivityProcessorStore,
  type ActivityStoreFailure,
  type PreparedActivity,
} from "@effect-agent/thread/ActivityStore";
import {
  ActivityPassLimits,
  processCommittedActivity,
  type ActivityProcessingError,
} from "@effect-agent/thread/CommittedActivity";
import { type DigestError } from "@effect-agent/thread/Digest";
import { type PersistedJson } from "@effect-agent/thread/Records";
import {
  type ThreadNotMaterialized,
  type ThreadStore,
  type ThreadStoreError,
} from "@effect-agent/thread/ThreadStore";
import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Schema, type Crypto, type Scope } from "effect";

type Equal<L, R> =
  (<T>() => T extends L ? 1 : 2) extends <T>() => T extends R ? 1 : 2
    ? (<T>() => T extends R ? 1 : 2) extends <T>() => T extends L ? 1 : 2
      ? true
      : false
    : false;
type Assert<T extends true> = T;

class ExtractFailure extends Schema.TaggedError<ExtractFailure>()("ExtractFailure", {}) {}
class ApplyFailure extends Schema.TaggedError<ApplyFailure>()("ApplyFailure", {}) {}
class Extractor extends Context.Service<
  Extractor,
  {
    readonly read: Effect.Effect<PersistedJson, ExtractFailure, Scope.Scope>;
  }
>()("test/activity/Extractor") {}
class Destination extends Context.Service<
  Destination,
  {
    readonly apply: (work: PreparedActivity) => Effect.Effect<void, ApplyFailure, Scope.Scope>;
  }
>()("test/activity/Destination") {}

const pass = processCommittedActivity({
  key: ActivityProcessorKey.make({
    processorId: "test",
    processorVersion: "1",
    threadId: Schema.decodeSync(ThreadId)("test"),
  }),
  owner: "worker",
  limits: ActivityPassLimits.make({
    maxRecords: 1,
    pageSize: 1,
    timeoutMillis: 100,
    leaseMillis: 1_100,
  }),
  extract: () =>
    Effect.gen(function* () {
      return yield* (yield* Extractor).read;
    }),
  apply: (work) =>
    Effect.gen(function* () {
      yield* (yield* Destination).apply(work);
    }),
});

type Errors = Assert<
  Equal<
    Effect.Error<typeof pass>,
    | ExtractFailure
    | ApplyFailure
    | ActivityStoreFailure
    | ActivityProcessingError
    | DigestError
    | ThreadStoreError
    | ThreadNotMaterialized
  >
>;
type Requirements = Assert<
  Equal<
    Effect.Services<typeof pass>,
    Extractor | Destination | ActivityProcessorStore | ThreadStore | Crypto.Crypto
  >
>;

describe("committed activity public composition", () => {
  it("preserves application failures and requirements while owning callback scopes", () => {
    const proof: readonly [Errors, Requirements] = [true, true];

    expect(proof).toEqual([true, true]);
  });
});
