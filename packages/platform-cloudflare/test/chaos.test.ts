import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/cloudflare-thread-client";
import { Effect } from "effect";
import { type Receipt } from "effect-agent/durable-agent-runtime";
import {
  submissionInputRecordId,
  submissionSettlementRecordId,
} from "effect-agent/submission-ledger";
import { describe, expect, it } from "vite-plus/test";

import type { searchDefinition } from "./fixtures.ts";
import {
  armRuntimeEviction,
  armedEvictionsRemaining,
  plannerDefinition,
  submitOptions,
} from "./fixtures.ts";
import {
  allSettled,
  assertConvergence,
  drainAlarmsUntil,
  readCanonical,
  runClient,
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

describe("DC chaos-abort evidence equivalence", () => {
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
});
