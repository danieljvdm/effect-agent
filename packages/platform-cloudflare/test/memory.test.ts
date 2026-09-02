import {
  MemoryWrite,
  MemoryMutationPoint,
  MemoryIndexCandidate,
  MemoryIndexSearch,
  SemanticMemoryProfile,
  SemanticCandidateLimits,
  MemoryReader,
  MemoryWriter,
} from "@effect-agent/core";
import {
  decodeMemoryWire,
  encodeMemoryWire,
  MemoryOwnerRequest,
  MemoryOwnerResponse,
  defaultMemoryRpcLimits,
  DoMemoryStorageLimits,
  doMemoryStoreLayer,
  handleMemoryOwnerRequest,
  MemoryOwnerAuthorizer,
  MemoryOwnerIdentity,
} from "@effect-agent/storage-cloudflare";
import { env, runInDurableObject } from "cloudflare:test";
import { Clock, Deferred, Effect, Fiber, Schema } from "effect";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import { CloudflareMemoryClient, MemoryObjectNamespace } from "../src/index.ts";
import {
  memoryAccess,
  memoryCalls,
  memoryCandidates,
  memoryFaults,
  MemoryProjects,
  memoryPut,
  memoryRecallLimits,
  slowStarted,
  slowFinished,
} from "./memory-fixtures.ts";
import type { TestMemoryObject, TestThreadObject } from "./worker.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      MEMORIES: DurableObjectNamespace<TestMemoryObject>;
    }
  }
}

let counter = 0;
const project = () => `memory-${counter++}`;
const stub = (name: string) => env.MEMORIES.getByName(MemoryProjects.make(name).address);
const client = (name: string, principal = "application") =>
  CloudflareMemoryClient.fromBinding(env.MEMORIES, { access: memoryAccess(name), principal });
const decode = (encoded: string) =>
  Schema.decodeSync(Schema.fromJsonString(MemoryOwnerResponse))(encoded);
const encodedCandidates = (ids: ReadonlyArray<string>) =>
  Schema.encodeSync(MemoryOwnerRequest.members[0].fields.lookup)(memoryCandidates(ids));

describe("shared Cloudflare memory owner", () => {
  it("bounds owner response and source bytes and reads duplicate sources only once", async () => {
    const name = project();
    await runInDurableObject(stub(name), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          const reader = yield* MemoryReader;
          yield* writer.change(memoryPut(name, "source"));
          let reads = 0;
          const counted = MemoryReader.fromAdapter({
            get: (key) =>
              Effect.suspend(() => {
                reads++;
                return reader.get(key);
              }),
          });
          const request: MemoryOwnerRequest = {
            _tag: "Revalidate",
            version: 1,
            access: memoryAccess(name),
            principal: "application",
            lookup: memoryCandidates(["source", "source"]),
            limits: memoryRecallLimits,
            deadlineMillis: (yield* Clock.currentTimeMillis) + 1000,
          };
          const encoded = yield* encodeMemoryWire(MemoryOwnerRequest, request, 1_048_576);
          const smallResponse = yield* handleMemoryOwnerRequest(encoded, {
            ...defaultMemoryRpcLimits,
            maxResponseBytes: 256,
          }).pipe(Effect.provideService(MemoryReader, counted));
          expect(reads).toBe(1);
          expect(smallResponse.length).toBeLessThanOrEqual(256);
          expect(decode(smallResponse)).toMatchObject({
            _tag: "Failed",
            failure: { _tag: "MemoryRpcError", reason: "budget" },
          });
          expect(
            decode(
              yield* handleMemoryOwnerRequest(encoded, {
                ...defaultMemoryRpcLimits,
                maxSourceBytes: 1,
              }),
            ),
          ).toMatchObject({ failure: { _tag: "MemoryRecallError", reason: "budget" } });
          expect(decode(yield* handleMemoryOwnerRequest("x".repeat(1_048_577)))).toMatchObject({
            failure: { _tag: "MemoryRpcError", reason: "budget" },
          });
        }).pipe(
          Effect.provideService(MemoryOwnerIdentity, { namespace: MemoryProjects.make(name) }),
          Effect.provideService(MemoryOwnerAuthorizer, { authorize: () => Effect.void }),
          Effect.provide(doMemoryStoreLayer(state.storage)),
        ),
      ),
    );
  });
  it("revalidates ranked semantic candidates in one RPC and excludes stale scores and forged byte ranges", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const memory = yield* client(name);
        const first = yield* memory.change(memoryPut(name, "a"));
        const second = yield* memory.change(memoryPut(name, "b"));
        const profile = SemanticMemoryProfile.make({
          version: 1,
          provider: "test",
          model: "test",
          modelRevision: "1",
          dimensions: 1,
          chunker: "utf8-codepoint@1",
          maxChunkBytes: 8192,
          distance: "cosine",
        });
        const candidate = (document: typeof first) =>
          MemoryIndexCandidate.make({
            key: document.key,
            source: document.source,
            sourceGeneration: document.generation,
            passageId: "chunk-0",
            ordinal: 0,
            startByte: 0,
            endByte: 6,
            text: `text-${document.key.id}`,
            score: 1,
            indexedAt: 1,
          });
        const candidates = [candidate(second), candidate(first), candidate(second)];
        const found = MemoryIndexSearch.make({ candidates, scannedChunks: 3 });
        const limits = { maxCandidates: 16, maxScannedChunks: 16, minScore: 0 };
        const before = memoryCalls.get(MemoryProjects.make(name).address) ?? 0;
        const result = yield* memory.revalidateSemantic(found, profile, limits);
        expect(memoryCalls.get(MemoryProjects.make(name).address)).toBe(before + 1);
        expect(result.lookup).toMatchObject({
          _tag: "Found",
          passages: [
            { content: { text: "text-b" } },
            { content: { text: "text-a" } },
            { content: { text: "text-b" } },
          ],
        });
        yield* memory.change(memoryPut(name, "a", "update", "1", "updated"));
        const next = yield* memory.revalidateSemantic(
          MemoryIndexSearch.make({
            candidates: [
              candidate(first),
              MemoryIndexCandidate.make({ ...candidate(second), startByte: 1 }),
            ],
            scannedChunks: 2,
          }),
          profile,
          limits,
        );
        expect(next.lookup).toEqual({ _tag: "NoMatch" });
        expect(next.staleExcluded).toBe(2);
      }),
    ));
  it("rejects duplicate-expanded semantic output before reading later sources or encoding the RPC response", async () => {
    const name = project();
    await runInDurableObject(stub(name), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          const reader = yield* MemoryReader;
          const put = memoryPut(name, "a");
          const first = yield* writer.change(
            MemoryWrite.make({
              ...put,
              content: {
                ...put.content,
                attributions: put.content.attributions.map((attribution) => ({
                  ...attribution,
                  interpretation: '"🌊"\\'.repeat(256),
                })),
                metadata: { evidence: "🌊".repeat(128) },
              },
            }),
          );
          const second = yield* writer.change(memoryPut(name, "b"));
          const candidate = (document: typeof first) =>
            MemoryIndexCandidate.make({
              key: document.key,
              source: document.source,
              sourceGeneration: document.generation,
              passageId: "chunk-0",
              ordinal: 0,
              startByte: 0,
              endByte: 6,
              text: `text-${document.key.id}`,
              score: 1,
              indexedAt: 1,
            });
          const request: MemoryOwnerRequest = {
            _tag: "RevalidateSemantic",
            version: 1,
            access: memoryAccess(name),
            principal: "application",
            deadlineMillis: (yield* Clock.currentTimeMillis) + 1000,
            profile: SemanticMemoryProfile.make({
              version: 1,
              provider: "test",
              model: "test",
              modelRevision: "1",
              dimensions: 1,
              chunker: "utf8-codepoint@1",
              maxChunkBytes: 8192,
              distance: "cosine",
            }),
            found: MemoryIndexSearch.make({ candidates: [candidate(first)], scannedChunks: 1 }),
            limits: SemanticCandidateLimits.make({
              maxCandidates: 128,
              maxScannedChunks: 128,
              minScore: 0,
            }),
          };
          const ownerLimits = { ...defaultMemoryRpcLimits, maxResponseBytes: 4096 };
          const reads: Array<string> = [];
          const counted = MemoryReader.fromAdapter({
            get: (key) =>
              Effect.suspend(() => {
                reads.push(key.id);
                return reader.get(key);
              }),
          });
          const single = yield* handleMemoryOwnerRequest(
            yield* encodeMemoryWire(MemoryOwnerRequest, request, 1_048_576),
            ownerLimits,
          );
          expect(decode(single)).toMatchObject({ _tag: "Semantic" });
          const expanded = {
            ...request,
            found: MemoryIndexSearch.make({
              candidates: [
                ...Array.from({ length: 127 }, () => candidate(first)),
                candidate(second),
              ],
              scannedChunks: 128,
            }),
          };
          const response = yield* handleMemoryOwnerRequest(
            yield* encodeMemoryWire(MemoryOwnerRequest, expanded, 1_048_576),
            ownerLimits,
          ).pipe(Effect.provideService(MemoryReader, counted));
          expect(decode(response)).toMatchObject({
            _tag: "Failed",
            failure: {
              _tag: "SemanticMemoryError",
              operation: "query output bytes",
              reason: "budget",
            },
          });
          expect(reads).toEqual(["a"]);
          expect(new TextEncoder().encode(response).byteLength).toBeLessThanOrEqual(4096);
        }).pipe(
          Effect.provideService(MemoryOwnerIdentity, { namespace: MemoryProjects.make(name) }),
          Effect.provideService(MemoryOwnerAuthorizer, { authorize: () => Effect.void }),
          Effect.provide(doMemoryStoreLayer(state.storage)),
        ),
      ),
    );
  });
  it("shares updates between two Thread Objects and uses one owner RPC for multi-source recall", async () => {
    const name = project();
    // The binding's production interface omits test-only methods; resolve the actual fixture type.
    const threads: DurableObjectNamespace<TestThreadObject> = env.THREADS;
    const a = threads.getByName(`${name}-thread-a`);
    const b = threads.getByName(`${name}-thread-b`);
    await a.memoryChange(name, Schema.encodeSync(MemoryWrite.Wire)(memoryPut(name, "a")));
    await a.memoryChange(name, Schema.encodeSync(MemoryWrite.Wire)(memoryPut(name, "b")));
    await a.memoryChange(
      name,
      Schema.encodeSync(MemoryWrite.Wire)(memoryPut(name, "a", "update-a", "1", "corrected")),
    );
    const before = memoryCalls.get(MemoryProjects.make(name).address) ?? 0;
    const recalled = await b.memoryRecall(name, encodedCandidates(["b", "a", "b"]));
    expect(memoryCalls.get(MemoryProjects.make(name).address)).toBe(before + 1);
    expect(recalled.passages.map((p) => p.content.text)).toEqual(["text-b", "corrected"]);
    expect(recalled.passages[1]?.content.attributions[0]).toMatchObject({
      speaker: "Dan",
      locator: "thread://a/input",
    });
    expect(recalled.outcomes[0]?.deduplicated).toBe(1);
    expect(recalled.text).not.toContain("cache://not-authoritative");
  });

  it("serializes concurrent revision checks, replays exact receipts, and rejects changed operation IDs", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const memory = yield* client(name);
        const initial = yield* memory.change(memoryPut(name, "source"));
        const results = yield* Effect.all(
          [
            memory.change(memoryPut(name, "source", "a", "1", "left")).pipe(Effect.result),
            memory.change(memoryPut(name, "source", "b", "1", "right")).pipe(Effect.result),
          ],
          { concurrency: 2 },
        );
        expect(results.filter((r) => r._tag === "Success")).toHaveLength(1);
        expect(results.filter((r) => r._tag === "Failure").map((r) => r.failure._tag)).toEqual([
          "MemoryConflict",
        ]);
        expect(yield* memory.change(memoryPut(name, "source"))).toEqual(initial);
        expect(
          yield* memory
            .change(memoryPut(name, "source", "put-source", null, "changed"))
            .pipe(Effect.flip),
        ).toMatchObject({ _tag: "MemoryOperationConflict" });
      }),
    ));

  it("rechecks access and terminal withdrawal instead of trusting stale or cached candidates", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const memory = yield* client(name);
        yield* memory.change(memoryPut(name, "source"));
        const captured = yield* memory.revalidate(memoryCandidates(["source"]), memoryRecallLimits);
        yield* memory.change(
          MemoryWrite.make({ ...memoryPut(name, "source", "revoke", "1"), scopes: [] }),
        );
        expect(yield* memory.revalidate(memoryCandidates(["source"]), memoryRecallLimits)).toEqual({
          _tag: "NoMatch",
        });
        yield* memory.change({
          _tag: "Withdraw",
          key: memoryPut(name, "source").key,
          operationId: "withdraw",
          expectedRevision: "2",
          reason: "removed",
        });
        expect(yield* memory.revalidate(captured, memoryRecallLimits)).toEqual({ _tag: "NoMatch" });
        expect(captured._tag).toBe("Found");
        expect(
          yield* memory.change(memoryPut(name, "source", "restore", "3")).pipe(Effect.flip),
        ).toMatchObject({ _tag: "MemoryWithdrawn" });
      }),
    ));

  it("denies cross-namespace, cross-scope and unauthenticated requests at the owner", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const foreign = project();
        const request: MemoryOwnerRequest = {
          _tag: "Revalidate",
          version: 1,
          access: memoryAccess(foreign),
          principal: "application",
          lookup: { _tag: "NoMatch" },
          limits: memoryRecallLimits,
          deadlineMillis: (yield* Clock.currentTimeMillis) + 1000,
        };
        const encoded = yield* encodeMemoryWire(MemoryOwnerRequest, request, 1_048_576);
        const response = yield* Effect.promise(() => stub(name).memory(encoded));
        expect(decode(response)).toMatchObject({
          _tag: "Failed",
          failure: { _tag: "MemoryRpcError", reason: "denied" },
        });
        const denied = yield* client(name, "untrusted");
        expect(
          yield* denied.revalidate({ _tag: "NoMatch" }, memoryRecallLimits).pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        const scope = yield* CloudflareMemoryClient.make(
          { ...memoryAccess(name), scope: "foreign" },
          "application",
        ).pipe(Effect.provideService(MemoryObjectNamespace, { namespace: env.MEMORIES }));
        expect(
          yield* scope
            .revalidate(memoryCandidates(["source"]), memoryRecallLimits)
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
      }),
    ));

  it("rejects oversized source counts, aggregate request/response bytes, and expired deadlines without splitting", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const memory = yield* client(name);
        expect(
          yield* memory
            .revalidate(
              memoryCandidates(Array.from({ length: 17 }, (_, i) => `${i}`)),
              memoryRecallLimits,
            )
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect(memoryCalls.get(MemoryProjects.make(name).address)).toBe(1);
        const bounded = CloudflareMemoryClient.fromBinding(env.MEMORIES, {
          access: memoryAccess(name),
          principal: "application",
          rpcLimits: { ...defaultMemoryRpcLimits, maxRequestBytes: 256 },
        });
        expectTypeOf<Effect.Services<typeof bounded>>().toEqualTypeOf<never>();
        const injected = CloudflareMemoryClient.make(memoryAccess(name), "application");
        expectTypeOf<Effect.Services<typeof injected>>().toEqualTypeOf<MemoryObjectNamespace>();
        const tiny = yield* bounded;
        expect(
          yield* tiny
            .revalidate(memoryCandidates(["source"]), memoryRecallLimits)
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect(memoryCalls.get(MemoryProjects.make(name).address)).toBe(1);
        const request: MemoryOwnerRequest = {
          _tag: "Revalidate",
          version: 1,
          access: memoryAccess(name),
          principal: "application",
          lookup: { _tag: "NoMatch" },
          limits: memoryRecallLimits,
          deadlineMillis: 0,
        };
        const encoded = yield* encodeMemoryWire(MemoryOwnerRequest, request, 1_048_576);
        expect(decode(yield* Effect.promise(() => stub(name).memory(encoded)))).toMatchObject({
          failure: { reason: "timeout" },
        });
        expect(
          yield* decodeMemoryWire(MemoryOwnerResponse, "x".repeat(257), 256).pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
      }),
    ));

  for (const point of MemoryMutationPoint.literals) {
    it(`recovers after owner eviction at ${point}`, async () => {
      const name = project();
      // Initialize separately so only the targeted durable change is interrupted.
      if (point.startsWith("memory:change:")) {
        await Effect.runPromise(
          Effect.flatMap(client(name), (c) =>
            c.revalidate({ _tag: "NoMatch" }, memoryRecallLimits),
          ),
        );
      }
      memoryFaults.set(MemoryProjects.make(name).address, { point, kind: "abort" });
      const write = Effect.flatMap(client(name), (c) => c.change(memoryPut(name, "source")));
      const first = await Effect.runPromise(Effect.result(write));
      expect(first._tag).toBe("Failure");
      expect(memoryFaults.has(MemoryProjects.make(name).address)).toBe(false);
      const recovered = await Effect.runPromise(write);
      expect(recovered.generation).toBe(1);
      expect(await Effect.runPromise(write)).toEqual(recovered);
      const lookup = await Effect.runPromise(
        Effect.flatMap(client(name), (c) =>
          c.revalidate(memoryCandidates(["source"]), memoryRecallLimits),
        ),
      );
      expect(lookup).toMatchObject({ _tag: "Found", passages: [{ source: { revision: "1" } }] });
    });
  }

  it("retains exact receipts across lost acknowledgements and restart", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const memory = yield* client(name);
        memoryFaults.set(MemoryProjects.make(name).address, {
          point: "memory:change:after",
          kind: "fail",
        });
        const command = memoryPut(name, "source");
        expect(yield* memory.change(command).pipe(Effect.flip)).toMatchObject({
          _tag: "MemoryMutationFailure",
          point: "memory:change:after",
        });
        const replay = yield* memory.change(command);
        expect(replay.generation).toBe(1);
        expect(yield* memory.change(command)).toEqual(replay);
      }),
    ));

  it("bounds local stored rows and receipt count atomically while allowing exact replay at capacity", async () => {
    const name = project();
    await runInDurableObject(stub(name), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          const reader = yield* MemoryReader;
          const command = memoryPut(name, "source");
          expect(
            yield* writer
              .change(memoryPut(name, "oversized", "too-big", null, "x".repeat(5000)))
              .pipe(Effect.flip),
          ).toMatchObject({ operation: "memory row byte limit", reason: "invalid-input" });
          expect(yield* reader.get(memoryPut(name, "oversized").key)).toBeNull();
          const first = yield* writer.change(command);
          expect(yield* writer.change(command)).toEqual(first);
          expect(yield* writer.change(memoryPut(name, "second")).pipe(Effect.flip)).toMatchObject({
            _tag: "MemoryStorageError",
            reason: "invalid-input",
          });
          expect(yield* reader.get(memoryPut(name, "second").key)).toBeNull();
        }).pipe(
          Effect.provide(
            doMemoryStoreLayer(
              state.storage,
              DoMemoryStorageLimits.make({
                maxRowBytes: 5000,
                maxDocuments: 1,
                maxReceipts: 1,
                maxStorageBytes: 10_000,
              }),
            ),
          ),
        ),
      ),
    );
  });

  it("finalizes owner authorization on timeout and lets a client interrupt without waiting for the owner", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const address = MemoryProjects.make(name).address;
        const started = yield* Deferred.make<void>();
        const finished = yield* Deferred.make<void>();
        slowStarted.set(address, started);
        slowFinished.set(address, finished);
        const memory = yield* client(name, "slow");
        const pending = yield* memory
          .revalidate({ _tag: "NoMatch" }, { ...memoryRecallLimits, timeoutMillis: 100 })
          .pipe(Effect.forkChild);
        yield* Deferred.await(started);
        yield* Fiber.interrupt(pending);
        yield* Deferred.await(finished);
        const timeout = yield* memory
          .revalidate({ _tag: "NoMatch" }, { ...memoryRecallLimits, timeoutMillis: 10 })
          .pipe(Effect.flip);
        expect(timeout).toMatchObject({ _tag: "MemoryRpcError", reason: "timeout" });
        const normal = yield* client(name);
        expect(yield* normal.revalidate({ _tag: "NoMatch" }, memoryRecallLimits)).toEqual({
          _tag: "NoMatch",
        });
      }).pipe(Effect.scoped),
    ));

  it("fails closed when authorization defects", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const memory = yield* client(project(), "defect");
        expect(
          yield* memory.revalidate({ _tag: "NoMatch" }, memoryRecallLimits).pipe(Effect.flip),
        ).toMatchObject({ _tag: "MemoryRpcError", reason: "unavailable" });
      }),
    ));
});
