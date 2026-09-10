import type { MemoryDocument } from "@effect-agent/core/MemoryStore";
import {
  MemoryKey,
  MemoryReader,
  MemoryScope,
  MemoryStorageError,
  MemoryWriter,
} from "@effect-agent/core/MemoryStore";
import { CloudflareMemoryClient } from "@effect-agent/platform-cloudflare/CloudflareMemory";
import { doMemoryStoreLayer } from "@effect-agent/storage-cloudflare/DoMemoryStore";
import {
  defaultMemoryRpcLimits,
  encodeMemoryWire,
  handleMemoryOwnerRequest,
  MemoryOwnerAuthorizer,
  MemoryOwnerIdentity,
  MemoryOwnerRequest,
  MemoryOwnerResponse,
  type MemoryOwnerFailure,
} from "@effect-agent/storage-cloudflare/MemoryProtocol";
import { Principal } from "@effect-agent/thread/SubmissionLedger";
import { env, runInDurableObject } from "cloudflare:test";
import { Clock, Deferred, Effect, Fiber, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import {
  memoryAccess,
  memoryCalls,
  memoryClocks,
  memoryDeniedSources,
  memoryPrincipal,
  MemoryProjects,
  memoryPut,
  memoryReplies,
  memoryRequests,
  slowFinished,
  slowStarted,
} from "./memory-fixtures.ts";

let counter = 0;
const project = () => `memory-get-${counter++}`;
const stub = (name: string) => env.MEMORIES.getByName(MemoryProjects.make(name).address);

const client = (name: string, principal = memoryPrincipal, timeoutMillis = 1000) =>
  CloudflareMemoryClient.fromBinding(env.MEMORIES, {
    access: memoryAccess(name),
    principal,
    rpcLimits: { ...defaultMemoryRpcLimits, timeoutMillis },
  });

const decode = Schema.decodeSync(Schema.fromJsonString(MemoryOwnerResponse));

describe("CloudflareMemoryClient.get", () => {
  it("reads the current revision, absence, and terminal withdrawal in one request each", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const address = MemoryProjects.make(name).address;
        const memory = yield* client(name);
        const write = memoryPut(name, "known");
        const read = memory.get(write.key);

        expectTypeOf<Effect.Success<typeof read>>().toEqualTypeOf<MemoryDocument<
          ReturnType<typeof MemoryProjects.make>
        > | null>();
        expectTypeOf<Effect.Error<typeof read>>().toEqualTypeOf<MemoryOwnerFailure>();
        expectTypeOf<Effect.Services<typeof read>>().toEqualTypeOf<never>();
        expect(yield* read).toBeNull();
        const first = yield* memory.change(write);
        const before = memoryCalls.get(address) ?? 0;

        expect(yield* read).toEqual(first);
        expect(memoryCalls.get(address)).toBe(before + 1);
        expect(memoryRequests.get(address)?.at(-1)).toMatchObject({ _tag: "Get", key: write.key });

        const corrected = yield* memory.change(memoryPut(name, "known", "correct", "1", "new"));

        expect(yield* read).toEqual(corrected);

        const withdrawn = yield* memory.change({
          _tag: "Withdraw",
          key: write.key,
          operationId: "withdraw",
          expectedRevision: "2",
          reason: "removed",
        });

        expect(yield* read).toEqual(withdrawn);
        // An old write receipt still replays exactly without restoring the current head.
        expect(yield* memory.change(write)).toEqual(first);
        expect(yield* read).toEqual(withdrawn);
        expect(memoryRequests.get(address)?.map((request) => request._tag)).not.toContain(
          "Revalidate",
        );
        expect(memoryRequests.get(address)?.map((request) => request._tag)).not.toContain(
          "RevalidateSemantic",
        );
      }),
    ));

  it("authorizes the exact key, principal and scope even for missing or withdrawn sources", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const memory = yield* client(name);
        const write = memoryPut(name, "known");

        yield* memory.change(write);
        const untrusted = yield* client(name, Principal.make("untrusted"));

        expect(yield* untrusted.get(write.key).pipe(Effect.flip)).toMatchObject({
          reason: "denied",
        });

        const foreignScope = yield* CloudflareMemoryClient.fromBinding(env.MEMORIES, {
          access: { ...memoryAccess(name), scope: MemoryScope.make("foreign") },
          principal: memoryPrincipal,
        });

        expect(yield* foreignScope.get(write.key).pipe(Effect.flip)).toMatchObject({
          reason: "denied",
        });
        expect(
          yield* memory.get(memoryPut(project(), "known").key).pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });

        memoryDeniedSources.set(MemoryProjects.make(name).address, new Set(["known", "missing"]));
        expect(yield* memory.get(write.key).pipe(Effect.flip)).toMatchObject({ reason: "denied" });
        expect(yield* memory.get({ ...write.key, id: "missing" }).pipe(Effect.flip)).toMatchObject({
          reason: "denied",
        });
        yield* memory.change({
          _tag: "Withdraw",
          key: write.key,
          operationId: "withdraw",
          expectedRevision: "1",
          reason: "removed",
        });
        expect(yield* memory.get(write.key).pipe(Effect.flip)).toMatchObject({ reason: "denied" });

        const revoked = memoryPut(name, "revoked");

        yield* memory.change(revoked);
        yield* memory.change({
          ...revoked,
          operationId: "revoke-scope",
          expectedRevision: "1",
          scopes: [],
        });
        expect(yield* memory.get(revoked.key).pipe(Effect.flip)).toMatchObject({
          reason: "denied",
        });
      }),
    ));

  it("rejects malformed keys, mismatched owner namespaces, expired requests and byte overflows", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const key = memoryPut(name, "known").key;

        const request: MemoryOwnerRequest = {
          _tag: "Get",
          version: 1,
          access: memoryAccess(name),
          principal: memoryPrincipal,
          key,
          deadlineMillis: (yield* Clock.currentTimeMillis) + 1000,
        };

        for (const id of ["", "x".repeat(1025)]) {
          const raw = JSON.stringify({ ...request, key: { ...key, id } });

          expect(decode(yield* Effect.promise(() => stub(name).memory(raw)))).toMatchObject({
            failure: { reason: "protocol" },
          });
        }
        const foreign = JSON.stringify({ ...request, key: memoryPut(project(), "known").key });

        expect(decode(yield* Effect.promise(() => stub(name).memory(foreign)))).toMatchObject({
          failure: { reason: "denied" },
        });
        expect(
          decode(
            yield* Effect.promise(() =>
              stub(name).memory(JSON.stringify({ ...request, deadlineMillis: 0 })),
            ),
          ),
        ).toMatchObject({ failure: { reason: "timeout" } });
        expect(
          decode(yield* Effect.promise(() => stub(name).memory("x".repeat(1_048_577)))),
        ).toMatchObject({ failure: { reason: "budget" } });

        const memory = yield* client(name);

        yield* memory.change(memoryPut(name, "known"));
        for (const bound of ["maxRequestBytes", "maxResponseBytes", "maxSourceBytes"] as const) {
          const limited = yield* CloudflareMemoryClient.fromBinding(env.MEMORIES, {
            access: memoryAccess(name),
            principal: memoryPrincipal,
            rpcLimits: { ...defaultMemoryRpcLimits, [bound]: 256 },
          });

          expect(yield* limited.get(key).pipe(Effect.flip)).toMatchObject({ reason: "budget" });
        }
      }),
    ));

  it("validates reply envelopes, exact identity and document schemas before exposing a result", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const memory = yield* client(name);
        const document = yield* memory.change(memoryPut(name, "known"));

        const response = {
          _tag: "Document",
          access: memoryAccess(name),
          key: document.key,
          document,
        };

        const address = MemoryProjects.make(name).address;

        yield* Effect.addFinalizer(() => Effect.sync(() => memoryReplies.delete(address)));
        for (const malformed of [
          { ...response, _tag: "Lookup", lookup: { _tag: "NoMatch" } },
          { ...response, access: memoryAccess(project()) },
          { ...response, access: { ...response.access, scope: "foreign" } },
          { ...response, key: { ...document.key, id: "other" }, document: null },
          { ...response, key: memoryPut(project(), "known").key, document: null },
          { ...response, document: { ...document, generation: 0 } },
          { ...response, document: { ...document, key: { ...document.key, id: "other" } } },
          { ...response, document: { ...document, source: { ...document.source, id: "other" } } },
          { ...response, document: { ...document, scopes: [] } },
        ]) {
          memoryReplies.set(address, JSON.stringify(malformed));
          expect(yield* memory.get(document.key).pipe(Effect.flip)).toMatchObject({
            reason: "protocol",
          });
        }
        memoryReplies.set(
          address,
          JSON.stringify({
            ...response,
            document: { ...document, key: memoryPut(project(), "known").key },
          }),
        );
        expect(yield* memory.get(document.key).pipe(Effect.flip)).toMatchObject({
          _tag: "MemoryStorageError",
          reason: "corrupt",
        });
      }).pipe(Effect.scoped),
    ));

  it("performs one local read without writing, enforces owner limits and preserves typed read failures", async () => {
    const name = project();

    await runInDurableObject(stub(name), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const reader = yield* MemoryReader;
          const writer = yield* MemoryWriter;
          const document = yield* writer.change(memoryPut(name, "known"));

          const request: MemoryOwnerRequest = {
            _tag: "Get",
            version: 1,
            access: memoryAccess(name),
            principal: memoryPrincipal,
            key: document.key,
            deadlineMillis: (yield* Clock.currentTimeMillis) + 1000,
          };

          const encoded = yield* encodeMemoryWire(MemoryOwnerRequest, request, 1_048_576);
          let reads = 0;

          const counted = MemoryReader.fromAdapter({
            get: (key) =>
              Effect.suspend(() => {
                reads++;

                return reader.get(key);
              }),
          });

          const before = state.storage.sql
            .exec("SELECT * FROM effect_agent_memory_usage_v1")
            .toArray();

          expect(
            decode(
              yield* handleMemoryOwnerRequest(encoded).pipe(
                Effect.provideService(MemoryReader, counted),
                Effect.provideService(MemoryWriter, {
                  change: () => Effect.die("read must not write"),
                }),
              ),
            ),
          ).toMatchObject({ _tag: "Document", document });
          expect(reads).toBe(1);
          expect(
            state.storage.sql.exec("SELECT * FROM effect_agent_memory_usage_v1").toArray(),
          ).toEqual(before);
          for (const bound of ["maxResponseBytes", "maxSourceBytes"] as const) {
            expect(
              decode(
                yield* handleMemoryOwnerRequest(encoded, {
                  ...defaultMemoryRpcLimits,
                  [bound]: 256,
                }),
              ),
            ).toMatchObject({ failure: { reason: "budget" } });
          }

          const unavailable = MemoryStorageError.make({
            operation: "current read",
            reason: "unavailable",
          });

          expect(
            decode(
              yield* handleMemoryOwnerRequest(encoded).pipe(
                Effect.provideService(MemoryReader, { get: () => Effect.fail(unavailable) }),
              ),
            ),
          ).toMatchObject({ failure: unavailable });
          const other = yield* writer.change(memoryPut(name, "other"));

          expect(
            decode(
              yield* handleMemoryOwnerRequest(encoded).pipe(
                Effect.provideService(
                  MemoryReader,
                  MemoryReader.fromAdapter({ get: () => Effect.succeed(other) }),
                ),
              ),
            ),
          ).toMatchObject({ failure: { _tag: "MemoryStorageError", reason: "corrupt" } });
          state.storage.sql.exec(
            "UPDATE effect_agent_memory_documents_v1 SET document_json = '{}' WHERE source_id = 'known'",
          );
        }).pipe(
          Effect.provideService(MemoryOwnerIdentity, { namespace: MemoryProjects.make(name) }),
          Effect.provideService(MemoryOwnerAuthorizer, { authorize: () => Effect.void }),
          Effect.provide(doMemoryStoreLayer(state.storage)),
        ),
      ),
    );

    const failure = await Effect.runPromise(
      Effect.gen(function* () {
        const memory = yield* client(name);

        return yield* memory.get(memoryPut(name, "known").key).pipe(Effect.flip);
      }),
    );

    expect(failure).toMatchObject({ _tag: "MemoryStorageError", reason: "corrupt" });
  });

  it("fails closed on owner defects", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const memory = yield* client(name, Principal.make("defect"));

        expect(yield* memory.get(memoryPut(name, "known").key).pipe(Effect.flip)).toMatchObject({
          reason: "unavailable",
        });
      }),
    ));

  it("finalizes authorization after timeout or caller interruption", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const name = project();
        const address = MemoryProjects.make(name).address;

        memoryClocks.set(address, yield* Clock.Clock);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            memoryClocks.delete(address);
            slowStarted.delete(address);
            slowFinished.delete(address);
          }),
        );
        const memory = yield* client(name, Principal.make("slow"), 100);

        for (const interrupt of [true, false]) {
          const started = yield* Deferred.make<void>();
          const finished = yield* Deferred.make<void>();

          slowStarted.set(address, started);
          slowFinished.set(address, finished);

          const pending = yield* memory
            .get(MemoryKey.make({ namespace: MemoryProjects.make(name), id: "known" }))
            .pipe(Effect.result, Effect.forkChild);

          yield* Deferred.await(started);
          if (interrupt) yield* Fiber.interrupt(pending);
          expect(yield* Deferred.isDone(finished)).toBe(false);
          yield* TestClock.adjust("100 millis");
          yield* Deferred.await(finished);
          if (!interrupt)
            expect(yield* Fiber.join(pending)).toMatchObject({
              _tag: "Failure",
              failure: { reason: "timeout" },
            });
        }
        const normal = yield* client(name);

        expect(yield* normal.get(memoryPut(name, "known").key)).toBeNull();
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));
});
