import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";

/**
 * The WP5 Miniflare restart lane (plan §3, §6): the eviction gate's STRONGEST form — a FULL
 * runtime restart over persisted Durable Object storage, the true "runtime restart" analogue
 * of the DN reopen tests. A representative armed-failpoint subset strands one Travel Planner
 * Submission in runtime 1 (each armed location keeps firing, so runtime 1 provably cannot
 * finish the work); a FRESH Miniflare over the SAME persist directory then converges it.
 *
 * Honest no-incoming-request evidence, per the WP0 probe's alarm-redelivery finding (probe 4:
 * Miniflare re-delivers a persisted overdue alarm after reopen without any request): the
 * reopened worker's instrumentation proves the Object received its redelivered alarm(s) and
 * ZERO client entries of any kind before the test ever reads the outcome; the reads that
 * follow (`awaitSettlement`, `observePage`) never run a maintenance pass — recovery was
 * alarm-driven alone.
 */

const workerEntry = join(import.meta.dirname, "travel-planner-restart-worker.ts");

let workerScript = "";

const openRuntime = (persistDirectory: string): Miniflare =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: workerScript,
      compatibilityDate: "2025-05-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: {
        THREADS: { className: "TravelPlannerRestartObject", useSQLite: true },
      },
      resourcePersistencePath: persistDirectory,
    }),
  );

const call = async <A>(runtime: Miniflare, path: string, body?: unknown): Promise<A> => {
  const response = await runtime.dispatchFetch(`http://placeholder${path}`, {
    method: body === undefined ? "GET" : "POST",
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const payload: unknown = await response.json();
  if (!response.ok) {
    throw new Error(`${path} failed: ${JSON.stringify(payload)}`);
  }
  return payload as A;
};

const pollUntil = async (
  probe: () => Promise<boolean>,
  what: string,
  deadlineMillis = 60_000,
): Promise<void> => {
  const deadline = Date.now() + deadlineMillis;
  while (Date.now() < deadline) {
    if (await probe()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for ${what}`);
};

interface Introspection {
  readonly alarmDeliveries: number;
  readonly clientEntries: ReadonlyArray<string>;
}

interface RestartRow {
  readonly name: string;
  readonly arms: ReadonlyArray<{
    readonly kind: "runtime" | "storage";
    readonly location: string;
    readonly count: number;
  }>;
  /** Runtime 1 has provably engaged its blocking loop once ≤ this many arms remain. */
  readonly blockedAtRemaining: number;
}

/**
 * The representative subset (distinct recovery classes across the restart): a lane stranded
 * before its claim ever sticks, a lane stranded between canonical input and its marker, and a
 * lane stranded between the reserved settlement and its ledger finalization. Every armed
 * location is re-crossed by each retry in runtime 1, so runtime 1 can never converge; the
 * reopened runtime starts unarmed and finishes the work from storage alone.
 */
const rows: ReadonlyArray<RestartRow> = [
  {
    name: "claim:after-claim strands the claimed lane; the reopened runtime reclaims and completes it",
    arms: [{ kind: "runtime", location: "claim:after-claim", count: 50 }],
    blockedAtRemaining: 48,
  },
  {
    name: "ledger:mark-input-applied:before strands the committed input; the reopened runtime repairs the marker and resumes",
    arms: [{ kind: "storage", location: "ledger:mark-input-applied:before", count: 50 }],
    blockedAtRemaining: 48,
  },
  {
    name: "terminalize:after-reserve then ledger:finalize-settlement:before strand the reserved settlement; the reopened runtime finalizes from history",
    arms: [
      { kind: "runtime", location: "terminalize:after-reserve", count: 1 },
      { kind: "storage", location: "ledger:finalize-settlement:before", count: 50 },
    ],
    blockedAtRemaining: 49,
  },
];

describe("Miniflare restart lane — Travel Planner armed-failpoint convergence", () => {
  const cleanups: Array<() => Promise<void>> = [];

  beforeAll(async () => {
    const bundled = await build({
      entryPoints: [workerEntry],
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      logLevel: "silent",
    });
    const output = bundled.outputFiles[0];
    if (output === undefined) throw new Error("esbuild produced no worker bundle");
    // Miniflare's module locator rejects dynamic `import()` expressions. The only ones in the
    // bundle are DEAD code (`@effect/sql`'s filesystem Migrator loader; the Object uses the
    // in-memory `fromRecord` migrations), so neutralize them behind a never-invoked stub.
    workerScript = `const __disabledDynamicImport = () => Promise.reject(new Error("dynamic import is disabled in the restart-lane bundle"));\n${output.text.replaceAll(
      /\bimport\s*\(/g,
      "__disabledDynamicImport(",
    )}`;
  }, 120_000);

  afterAll(async () => {
    for (const cleanup of cleanups.reverse()) {
      await cleanup();
    }
  });

  for (const [index, row] of rows.entries()) {
    it(`a Miniflare runtime restart over persisted DO storage converges the armed failpoint scenario: ${row.name}`, async () => {
      const thread = `tp-restart-${index}`;
      const persistDirectory = await mkdtemp(join(tmpdir(), "tp-restart-"));
      cleanups.push(() => rm(persistDirectory, { recursive: true, force: true }));
      const armedTotal = row.arms.reduce((total, arm) => total + arm.count, 0);

      // Runtime 1: arm, submit, and prove the block engaged (the armed location fired at
      // least once AND a retry hit it again — runtime 1 cannot finish this Submission).
      const first = openRuntime(persistDirectory);
      for (const arm of row.arms) {
        await call(first, "/arm", { _tag: arm.kind, thread, ...arm });
      }
      const { receipt } = await call<{ receipt: unknown }>(first, "/submit", {
        thread,
        key: `${thread}-key`,
      });
      await pollUntil(async () => {
        const armed = await call<{ remaining: number }>(
          first,
          `/armed?thread=${encodeURIComponent(thread)}`,
        );
        return armed.remaining <= row.blockedAtRemaining;
      }, `runtime 1 to hit ${row.name} repeatedly (started at ${armedTotal})`);
      await first.dispose();

      // Runtime 2 over the SAME directory: fresh module state (unarmed), persisted storage.
      // The redelivered alarm alone must converge the accepted work: wait for delivery
      // evidence WITHOUT touching the Object (the /introspect route reads worker module
      // state only), then assert no client entry of any kind preceded it.
      const second = openRuntime(persistDirectory);
      cleanups.push(() => second.dispose());
      await second.ready;
      await pollUntil(async () => {
        const seen = await call<Introspection>(second, "/introspect");
        return seen.alarmDeliveries >= 1;
      }, "the persisted alarm to re-deliver after reopen");
      const beforeAnyRead = await call<Introspection>(second, "/introspect");
      expect(beforeAnyRead.clientEntries).toEqual([]);

      // The reads below never run a maintenance pass; they only OBSERVE the outcome the
      // redelivered alarms already converged.
      const settled = await call<{ outcome: string }>(second, "/await", { receipt });
      expect(settled.outcome).toBe("completed");
      const { tags } = await call<{ tags: ReadonlyArray<string> }>(second, "/records", {
        thread,
      });
      expect(tags.filter((tag) => tag === "SubmissionSettled")).toHaveLength(1);
      expect(tags).toContain("ModelResponseRecorded");
    }, 120_000);
  }
});
