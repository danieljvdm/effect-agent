import { CloudflareMemoryClient } from "@effect-agent/platform-cloudflare/cloudflare-memory";
import { env } from "cloudflare:test";
import { Effect } from "effect";
import { MemoryWrite } from "effect-agent/memory-store";
import { describe, expect, it } from "vite-plus/test";

import {
  memoryAccess,
  memoryPrincipal,
  memoryCandidates,
  memoryFaults,
  MemoryProjects,
  memoryPut,
  memoryRecallLimits,
} from "./memory-fixtures.ts";
import type { TestMemoryObject } from "./worker.ts";

let counter = 0;
const project = () => `memory-${counter++}`;

const client = (name: string, principal = memoryPrincipal) =>
  CloudflareMemoryClient.fromBinding(env.MEMORIES, { access: memoryAccess(name), principal });

describe("shared Cloudflare memory owner", () => {
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

  for (const point of ["memory:change:after"] as const) {
    it(`recovers after owner eviction at ${point}`, async () => {
      const name = project();

      // Initialize separately so only the targeted durable change is interrupted.
      {
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
});

declare global {
  namespace Cloudflare {
    interface Env {
      MEMORIES: DurableObjectNamespace<TestMemoryObject>;
    }
  }
}
