import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";

import {
  submissionInputRecordId,
  submissionSettlementRecordId,
  type CanonicalRecordEnvelope,
  type Receipt,
} from "@effect-agent/session";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareConversationClient } from "../src/index.ts";
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
 */

let laneCounter = 0;
const lane = (label: string): string => `cf-chaos-${label}-${laneCounter++}`;

const submitTo = (
  definition: typeof searchDefinition | typeof plannerDefinition,
  conversation: string,
): Promise<Receipt> =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.submit(
        { definition },
        { question: "chaos equivalence", ref: conversation },
        submitOptions(conversation, `${conversation}-key`),
      );
    }),
  );

/** Abort the CURRENT incarnation between host operations (in-memory state dies). */
const abortIncarnation = (conversation: string): Promise<void> =>
  runInDurableObject(stubFor(conversation), (_instance, state) => {
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
  conversation: string,
): ReadonlyArray<string> =>
  records
    .filter((envelope) => envelope.record.payload._tag !== "RepairAnnotated")
    .map((envelope) =>
      JSON.stringify({ recordId: envelope.record.recordId, record: envelope.record })
        .replaceAll(receipt.submissionId, "{submissionId}")
        .replaceAll(receipt.receiptId, "{receiptId}")
        .replaceAll(conversation, "{conversationId}")
        .replaceAll(/\d{4}-\d{2}-\d{2}T[0-9:.]+Z/g, "{timestamp}")
        // Digests hash the RAW content (which legally embeds the Conversation identity), so
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
    const conversation = lane("reconcile-first");
    // Strand S1 mid-terminalization: the settlement is reserved but not canonical.
    armRuntimeEviction(conversation, "terminalize:after-reserve");
    const receipt1 = await submitTo(plannerDefinition, conversation);
    await drainAlarmsUntil(conversation, () =>
      Promise.resolve(armedEvictionsRemaining(conversation) === 0),
    );
    // New work arrives while the lane still owes S1's repair.
    const receipt2 = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.submit(
          { definition: plannerDefinition },
          { question: "queued behind the repair", ref: conversation },
          submitOptions(conversation, `${conversation}-key-2`),
        );
      }),
    );
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await assertConvergence(conversation);

    // The repaired settlement of S1 was appended BEFORE S2's canonical input: every pass
    // runs `runRecovery` before `processConversationResolved` claims anything (plan §1.4).
    const recordIds = (await readCanonical(conversation)).map(
      (envelope) => envelope.record.recordId,
    );
    const s1Settlement = recordIds.indexOf(submissionSettlementRecordId(receipt1.submissionId));
    const s2Input = recordIds.indexOf(submissionInputRecordId(receipt2.submissionId));
    expect(s1Settlement).toBeGreaterThanOrEqual(0);
    expect(s2Input).toBeGreaterThanOrEqual(0);
    expect(s1Settlement).toBeLessThan(s2Input);
  }, 60_000);
});
