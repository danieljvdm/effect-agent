import type { MemoryLookup } from "@effect-agent/core";
import {
  MemoryAccess,
  MemoryMutationFailpoint,
  MemoryMutationFailure,
  MemoryNamespace,
  MemoryPassage,
  MemoryRecallLimits,
  MemoryWrite,
  type MemoryMutationPoint,
} from "@effect-agent/core";
import {
  MemoryOwnerAuthorizer,
  MemoryOwnerIdentity,
  MemoryRpcError,
} from "@effect-agent/storage-cloudflare";
import { Deferred, Effect, Layer, Schema } from "effect";
import { DurableObjectState } from "effect-cf";

export const MemoryProjects = MemoryNamespace.define({
  name: "test/projects",
  version: 1,
  identity: Schema.String,
});
export const memoryAccess = (project: string) =>
  MemoryAccess.make({ namespace: MemoryProjects.make(project), scope: "team" });
export const memoryRecallLimits = MemoryRecallLimits.make({
  maxSources: 16,
  maxItems: 128,
  maxBytes: 100_000,
  maxTokens: 100_000,
  maxInputBytes: 1_000_000,
  timeoutMillis: 1_000,
});
export const memoryPut = (
  project: string,
  id: string,
  operationId = `put-${id}`,
  expectedRevision: string | null = null,
  text = `text-${id}`,
) => {
  const write = MemoryWrite.make({
    _tag: "Put",
    key: { namespace: MemoryProjects.make(project), id },
    operationId,
    expectedRevision,
    locator: `memory://${id}`,
    scopes: ["team"],
    content: {
      text,
      attributions: [
        {
          originId: `origin-${id}`,
          speaker: "Dan",
          observers: ["caller-a"],
          locator: "thread://a/input",
          activityAt: 1,
          interpretation: "statement",
        },
      ],
      metadata: {},
      recordedAt: 2,
    },
  });
  if (write._tag !== "Put") throw new Error("Expected Put fixture");
  return write;
};
export const memoryCandidates = (ids: ReadonlyArray<string>): MemoryLookup => ({
  _tag: "Found",
  passages: ids.map((id) =>
    MemoryPassage.make({
      version: 1,
      source: { id, revision: "stale", locator: "cache://not-authoritative" },
      passageId: "cached",
      content: memoryPut("ignored", id).content,
    }),
  ),
});

export const memoryCalls = new Map<string, number>();
export const memoryFaults = new Map<
  string,
  { point: MemoryMutationPoint; kind: "fail" | "abort" }
>();
export const slowStarted = new Map<string, Deferred.Deferred<void>>();
export const slowFinished = new Map<string, Deferred.Deferred<void>>();
export const memoryAuthorizer = Layer.effect(
  MemoryOwnerAuthorizer,
  Effect.gen(function* () {
    const { namespace } = yield* MemoryOwnerIdentity;
    yield* MemoryProjects.restore(namespace.address);
    return {
      authorize: Effect.fn("test.memory.authorize")(function* (request) {
        if (request.principal === "defect") return yield* Effect.die("authorization defect");
        if (request.principal === "slow") {
          return yield* Effect.acquireUseRelease(
            Effect.gen(function* () {
              const started = slowStarted.get(namespace.address);
              if (started) yield* Deferred.succeed(started, undefined);
            }),
            () => Effect.never,
            () =>
              Effect.gen(function* () {
                const finished = slowFinished.get(namespace.address);
                if (finished) yield* Deferred.succeed(finished, undefined);
              }),
          );
        }
        if (request.principal !== "application" || request.access.scope !== "team")
          return yield* MemoryRpcError.make({ reason: "denied" });
      }),
    };
  }),
);
export const memoryFailpoints = Layer.effect(
  MemoryMutationFailpoint,
  Effect.gen(function* () {
    const state = yield* DurableObjectState.DurableObjectState;
    return {
      hit: (point: MemoryMutationPoint) =>
        Effect.suspend(() => {
          const name = state.raw.id.name ?? "";
          const fault = memoryFaults.get(name);
          if (!fault || fault.point !== point) return Effect.void;
          memoryFaults.delete(name);
          return fault.kind === "fail"
            ? Effect.fail(MemoryMutationFailure.make({ point }))
            : Effect.sync(() => state.raw.abort(`memory fault ${point}`));
        }),
    };
  }),
);
