import {
  submissionInputRecordId,
  submissionSettlementRecordId,
  type CanonicalRecordEnvelope,
  type Receipt,
} from "@effect-agent/thread";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareThreadClient } from "../src/index.ts";
import {
  armRuntimeEviction,
  armedEvictionsRemaining,
  plannerDefinition,
  searchDefinition,
  submitOptions,
} from "./fixtures.ts";
import {
  allSettled,
  assertConvergence,
  drainAlarmsUntil,
  laneRows,
  readCanonical,
  runClient,
  stubFor,
} from "./harness.ts";

/**
 * No important in-memory state (exit gate; plan §3): a run whose Durable Object is aborted
 * between EVERY pair of host operations produces the same normalized canonical evidence as
 * an unchaosed control run — everything that matters was in storage. Plus the startup-
 * reconciliation ordering gate: an armed repair executes BEFORE the pass claims new work.
 *
 * The P7 WP4 seeded variant below randomizes the abort/alarm interleaving ACROSS two lanes
 * from one root seed (`CHAOS_SEED` env override; the failure output prints it), so the
 * alarm-delivery ORDER between lanes is itself chaosed while every round stays bounded.
 */

let laneCounter = 0;
const lane = (label: string): string => `cf-chaos-${label}-${laneCounter++}`;

/** Root seed for the seeded variant; replay any failure with `CHAOS_SEED=<seed>`. */
const CHAOS_ROOT_SEED = (() => {
  const raw = process.env["CHAOS_SEED"];

  if (raw === undefined || raw === "") return 20260813;
  const parsed = Number.parseInt(raw, 10);

  return Number.isSafeInteger(parsed) ? parsed : 20260813;
})();

/** Deterministic PRNG (mulberry32) for the seeded abort/alarm schedule. */
const mulberry32 = (seed: number): (() => number) => {
  let state = seed | 0;

  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);

    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;

    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
};

const submitTo = (
  definition: typeof searchDefinition | typeof plannerDefinition,
  thread: string,
): Promise<Receipt> =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition },
        { question: "chaos equivalence", ref: thread },
        submitOptions(thread, `${thread}-key`),
      );
    }),
  );

/** Abort the CURRENT incarnation between host operations (in-memory state dies). */
const abortIncarnation = (thread: string): Promise<void> =>
  runInDurableObject(stubFor(thread), (_instance, state) => {
    state.abort("chaos abort");
  }).then(
    () => undefined,
    () => undefined,
  );

/**
 * The cross-run normal form: canonical payloads in order, with repair-audit records dropped
 * (`RepairAnnotated` is DUR-013 audit evidence of recovery itself — the chaos run legally
 * has them, the control run legally does not), and every run-specific identity scrubbed:
 * the two minted identities (and everything derived from them) plus commit timestamps.
 */
const normalizedEvidence = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  receipt: Receipt,
  thread: string,
): ReadonlyArray<string> =>
  records
    .filter((envelope) => envelope.record.payload._tag !== "RepairAnnotated")
    .map((envelope) =>
      JSON.stringify({ recordId: envelope.record.recordId, record: envelope.record })
        .replaceAll(receipt.submissionId, "{submissionId}")
        .replaceAll(receipt.receiptId, "{receiptId}")
        .replaceAll(thread, "{threadId}")
        .replaceAll(/\d{4}-\d{2}-\d{2}T[0-9:.]+Z/g, "{timestamp}")
        // Digests hash the RAW content (which legally embeds the Thread identity), so
        // they can never be byte-equal across two lanes; the digest CHAIN's integrity is
        // asserted separately by the adapters and `assertConvergence`.
        .replaceAll(/"[0-9a-f]{64}"/g, '"{digest}"'),
    );

describe("DC chaos-abort evidence equivalence", () => {
  it("chaos-abort between every host operation preserves the normalized canonical evidence", async () => {
    // Control: one uninterrupted run.
    const control = lane("control");
    const controlReceipt = await submitTo(searchDefinition, control);

    await drainAlarmsUntil(control, allSettled(control));
    await assertConvergence(control);

    // Chaos: abort the incarnation after the submit and between every alarm delivery.
    const chaos = lane("chaos");
    const chaosReceipt = await submitTo(searchDefinition, chaos);

    for (let round = 0; round < 200; round++) {
      await abortIncarnation(chaos);
      const rows = await laneRows(chaos);

      if (rows.length > 0 && rows.every((row) => row.state === "settled")) break;
      try {
        await runDurableObjectAlarm(stubFor(chaos));
      } catch {
        // The aborted incarnation may reject the delivery; the alarm stays committed.
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    await drainAlarmsUntil(chaos, allSettled(chaos));
    await assertConvergence(chaos);

    // Same canonical outcome, byte-equal after normalization: nothing that mattered ever
    // lived in a Durable Object memory field.
    expect(normalizedEvidence(await readCanonical(chaos), chaosReceipt, chaos)).toEqual(
      normalizedEvidence(await readCanonical(control), controlReceipt, control),
    );
  }, 120_000);

  it("startup reconciliation ordering: the armed repair executes before the pass claims new work", async () => {
    const thread = lane("reconcile-first");

    // Strand S1 mid-terminalization: the settlement is reserved but not canonical.
    armRuntimeEviction(thread, "terminalize:after-reserve");
    const receipt1 = await submitTo(plannerDefinition, thread);

    await drainAlarmsUntil(thread, () => Promise.resolve(armedEvictionsRemaining(thread) === 0));

    // New work arrives while the lane still owes S1's repair.
    const receipt2 = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.submit(
          { definition: plannerDefinition },
          { question: "queued behind the repair", ref: thread },
          submitOptions(thread, `${thread}-key-2`),
        );
      }),
    );

    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);

    // The repaired settlement of S1 was appended BEFORE S2's canonical input: every pass
    // runs `runRecovery` before `processThreadResolved` claims anything (plan §1.4).
    const recordIds = (await readCanonical(thread)).map((envelope) => envelope.record.recordId);
    const s1Settlement = recordIds.indexOf(submissionSettlementRecordId(receipt1.submissionId));
    const s2Input = recordIds.indexOf(submissionInputRecordId(receipt2.submissionId));

    expect(s1Settlement).toBeGreaterThanOrEqual(0);
    expect(s2Input).toBeGreaterThanOrEqual(0);
    expect(s1Settlement).toBeLessThan(s2Input);
  }, 60_000);

  it("CHAOS: seeded random ctx.abort()/alarm-order interleaving across two lanes converges within bounded rounds and preserves normalized evidence", async () => {
    const random = mulberry32(CHAOS_ROOT_SEED);

    try {
      // Controls: one uninterrupted run per definition.
      const searchControl = lane("seeded-control-search");
      const searchControlReceipt = await submitTo(searchDefinition, searchControl);

      await drainAlarmsUntil(searchControl, allSettled(searchControl));
      const plannerControl = lane("seeded-control-planner");
      const plannerControlReceipt = await submitTo(plannerDefinition, plannerControl);

      await drainAlarmsUntil(plannerControl, allSettled(plannerControl));

      // Chaos: two lanes advance ONLY through seeded abort/alarm actions, so both the abort
      // positions and the alarm-delivery ORDER between the lanes are randomized (bounded).
      const lanes = [
        { thread: lane("seeded-chaos-search"), definition: searchDefinition },
        { thread: lane("seeded-chaos-planner"), definition: plannerDefinition },
      ] as const;

      const receipts = [
        await submitTo(lanes[0].definition, lanes[0].thread),
        await submitTo(lanes[1].definition, lanes[1].thread),
      ] as const;

      const settled = async (thread: string): Promise<boolean> => {
        const rows = await laneRows(thread);

        return rows.length > 0 && rows.every((row) => row.state === "settled");
      };

      const MAX_ROUNDS = 240;

      for (let round = 0; round < MAX_ROUNDS; round++) {
        const pending = [] as Array<string>;

        for (const { thread } of lanes) {
          if (!(await settled(thread))) pending.push(thread);
        }
        if (pending.length === 0) break;
        const target = pending[Math.floor(random() * pending.length)]!;
        const dice = random();

        if (dice < 0.4) {
          // Evict the current incarnation between host operations.
          await abortIncarnation(target);
        } else if (dice < 0.95) {
          try {
            await runDurableObjectAlarm(stubFor(target));
          } catch {
            // The aborted incarnation may reject the delivery; the alarm stays committed.
          }
        } else {
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
      }
      // Bounded-round convergence, then the full canonical drain and the shared claims.
      for (const { thread } of lanes) {
        await drainAlarmsUntil(thread, allSettled(thread));
        await assertConvergence(thread);
      }
      expect(
        normalizedEvidence(await readCanonical(lanes[0].thread), receipts[0], lanes[0].thread),
      ).toEqual(
        normalizedEvidence(await readCanonical(searchControl), searchControlReceipt, searchControl),
      );
      expect(
        normalizedEvidence(await readCanonical(lanes[1].thread), receipts[1], lanes[1].thread),
      ).toEqual(
        normalizedEvidence(
          await readCanonical(plannerControl),
          plannerControlReceipt,
          plannerControl,
        ),
      );
    } catch (error) {
      throw new Error(
        `DC seeded chaos failed — replay with CHAOS_SEED=${CHAOS_ROOT_SEED}: ${String(error)}`,
        { cause: error },
      );
    }
  }, 120_000);
});
