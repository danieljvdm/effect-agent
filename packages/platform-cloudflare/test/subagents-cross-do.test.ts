import {
  LedgerRecordChildSettledCall,
  decodePortResponse,
  encodePortRequest,
} from "@effect-agent/storage-cloudflare";
import {
  AbortCommand,
  ChildSettledNotification,
  type DurableRuntimeFailpointLocation,
  type Receipt,
} from "@effect-agent/thread";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareThreadClient, ThreadObject } from "../src/index.ts";
import {
  armRuntimeEviction,
  armStorageEviction,
  armedEvictionsRemaining,
  decodeThreadId,
  lostBookReplies,
  supplierCountsFor,
  submitOptions,
} from "./fixtures.ts";
import {
  anyInState,
  laneRows,
  readCanonical,
  runClient,
  scheduledAlarm,
  stubFor,
} from "./harness.ts";
import {
  armTransportFault,
  childModelInvocations,
  coordinatorDefinition,
  delegateCallIdFor,
  gateChildModel,
  healTransportFault,
  releaseChildModel,
  siblingCoordinatorDefinition,
  siblingLookupInvocations,
  uncertainChildRefs,
} from "./subagent-fixtures.ts";
import {
  SUBAGENTS,
  allLanesSettled,
  assertDelegationConverged,
  childThreadOf,
  completedDelegation,
  drainDelegationUntil,
  parentOutcomeOf,
  payloadsOf,
  reservationStatuses,
  settlementMarkers,
  waitFor,
} from "./subagent-harness.ts";

/**
 * Cross-Object subagent crash matrix. Every `subagent:*` coordinator failpoint re-runs with the parent and
 * child Threads in DIFFERENT Durable Objects (the identity rule maps each Thread
 * to its own Object), the kill lever is the platform's real failure mode (`ctx.abort()`), and
 * convergence is proven by alarm delivery alone — `drainDelegationUntil` fires only the two
 * Objects' persisted alarms, never a client entry point. The crash-matrix row names are the
 * SAME `DurableRuntimeFailpointLocation` strings the Node process-kill suite uses.
 *
 * On top of the failpoint rows: `recordChildSettled` at-least-once redelivery (not-waiting,
 * idempotent), pre-finalization durable parent wake, cross-Object abort propagation
 * (request-abort-and-join), and the DO-unreachable `Indeterminate`
 * establishment row (SUB-031: an unreachable admission authority NEVER admits a second
 * child; convergence resumes when transport heals).
 */

let laneCounter = 0;

const lane = (location: string): string =>
  `cf-s2-${location.replaceAll(":", "-")}-${laneCounter++}`;

const submitCoordinator = (
  fixture: "coordinator" | "sibling",
  thread: string,
  key: string,
): Promise<Receipt> =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        {
          definition: fixture === "sibling" ? siblingCoordinatorDefinition : coordinatorDefinition,
        },
        { mission: "cross-Object delegation", ref: thread },
        submitOptions(thread, key),
      );
    }),
    SUBAGENTS,
  );

/**
 * Fire-and-forget delivery of a possibly-HANGING lane's alarm: a gated researcher blocks its
 * Object's maintenance pass mid-model-stream, so the delivery promise must never be awaited
 * (the pool also auto-fires due alarms; this only removes the dependence on that cadence).
 */
const kickWithoutAwaiting = (thread: string): void => {
  void runDurableObjectAlarm(stubFor(thread, SUBAGENTS)).catch(() => {});
};

const abortParent = (thread: string, receipt: Receipt) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.abort(
        decodeThreadId(thread),
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "cross-Object abort propagation",
        }),
      );
    }),
    SUBAGENTS,
  );

/**
 * One coordinator eviction row: arm the location on the PARENT lane, submit, and let the two
 * Objects' persisted alarms alone converge parent and child to one established, joined,
 * settled delegation. The armed location must actually have fired.
 */
const coordinatorEvictionRow = async (
  location: DurableRuntimeFailpointLocation,
  fixture: "coordinator" | "sibling" = "coordinator",
): Promise<{ readonly ref: string; readonly receipt: Receipt }> => {
  const ref = lane(location);
  const key = `${ref}-key`;

  armRuntimeEviction(ref, location);
  const receipt = await submitCoordinator(fixture, ref, key);
  const child = childThreadOf(receipt, ref);

  await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
  expect(armedEvictionsRemaining(ref), `${location} must actually have fired`).toBe(0);
  await assertDelegationConverged(completedDelegation(receipt, ref));

  return { ref, receipt };
};

describe("DC cross-Object subagent matrix (parent and child in different Durable Objects)", () => {
  // -------------------------------------------------------------------------
  // Establishment ladder — every step's eviction converges on ONE child
  // Receipt, Thread, and join (SUB-016/SUB-017/SUB-031).
  // -------------------------------------------------------------------------

  it("eviction at subagent:after-reserve: the replayed batch re-establishes the one child", async () => {
    await coordinatorEvictionRow("subagent:after-reserve");
  }, 40_000);

  it("eviction at subagent:after-request-append: binding-free recovery admits the ONE intended child from the canonical request", async () => {
    await coordinatorEvictionRow("subagent:after-request-append");
  }, 40_000);

  it("eviction at subagent:after-admit: the routed admission committed in the child's Object survives and resolveAdmission reattaches to it", async () => {
    await coordinatorEvictionRow("subagent:after-admit");
  }, 40_000);

  it("eviction at subagent:after-child-ready: readiness and the start link replay idempotently", async () => {
    await coordinatorEvictionRow("subagent:after-child-ready");
  }, 40_000);

  it("eviction at subagent:after-start-append: recovery restores the waitingForChild suspension", async () => {
    await coordinatorEvictionRow("subagent:after-start-append");
  }, 40_000);

  it("eviction at subagent:after-suspend: the committed suspension holds and the woken parent joins", async () => {
    // In DC the autonomous child Object can settle BEFORE the parent's suspend transaction
    // commits (the engine then joins inline and never suspends), so the suspension-path rows
    // gate the researcher until the armed eviction has provably fired.
    const location: DurableRuntimeFailpointLocation = "subagent:after-suspend";
    const ref = lane(location);

    gateChildModel(ref);
    armRuntimeEviction(ref, location);
    const receipt = await submitCoordinator("coordinator", ref, `${ref}-key`);
    const child = childThreadOf(receipt, ref);

    await drainDelegationUntil([ref], async () => armedEvictionsRemaining(ref) === 0);
    releaseChildModel(ref);
    await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
    await assertDelegationConverged(completedDelegation(receipt, ref));
  }, 40_000);

  it("the chained establishment ladder (five evictions in order) converges on one child Receipt, Thread, and join", async () => {
    const ladder: ReadonlyArray<DurableRuntimeFailpointLocation> = [
      "subagent:after-reserve",
      "subagent:after-request-append",
      "subagent:after-admit",
      "subagent:after-child-ready",
      "subagent:after-start-append",
    ];

    const ref = lane("establishment-ladder");
    const key = `${ref}-key`;

    // Chained rows arm the WHOLE ladder up front: due alarms auto-fire in the pool, so each
    // recovery pass can run within milliseconds of the previous abort.
    armRuntimeEviction(ref, ...ladder);
    const receipt = await submitCoordinator("coordinator", ref, key);
    const child = childThreadOf(receipt, ref);

    await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
    expect(armedEvictionsRemaining(ref), "every armed ladder step must have fired").toBe(0);
    // Five doomed incarnations later: ONE child, ONE join, one researcher invocation.
    await assertDelegationConverged(completedDelegation(receipt, ref));
  }, 60_000);

  it("a child never runs a Turn before its lineage record is canonical", async () => {
    // The admitted, lineage-less child must defer to the parent's establishment
    // (SUB-016), never run ahead of it. This row pins the discipline cross-Object: the parent
    // Object dies at subagent:after-admit on EVERY pass while the child's own alarms run.
    const location: DurableRuntimeFailpointLocation = "subagent:after-admit";
    const ref = lane("child-awaits-lineage");
    const faultSuffix = `:${delegateCallIdFor(ref)}`;

    // An after-admit eviction buffer covers the window between the admission committing and
    // the transport fault arming (the pool auto-fires due alarms in the background, burning
    // roughly one armed eviction per millisecond); once the fault is armed, every parent
    // recovery pass answers Indeterminate (SUB-031) and the parent provably CANNOT complete
    // establishment during the child-side probes.
    armRuntimeEviction(ref, ...Array.from({ length: 64 }, () => location));
    const receipt = await submitCoordinator("coordinator", ref, `${ref}-key`);
    const child = childThreadOf(receipt, ref);

    // Drive the parent until the routed admission is durable in the CHILD's Object — the
    // same pass then dies at after-admit, so no lineage record exists there. Then cut the
    // parent→child transport so the admission stays authoritative but unreachable.
    await drainDelegationUntil([ref], async () => (await laneRows(child, SUBAGENTS)).length === 1);
    armTransportFault(faultSuffix);

    // The transport fault must actually be holding the parent back before the probe window
    // means anything: the buffer stops burning once every parent pass answers Indeterminate.
    await waitFor(async () => {
      const before = armedEvictionsRemaining(ref);

      await new Promise((resolve) => setTimeout(resolve, 25));

      return before > 0 && armedEvictionsRemaining(ref) === before;
    }, "the armed-eviction burn to stop under the transport fault");

    // Child-side probe window: fire ONLY the child's own alarm passes (the fault blocks its
    // portCall/wake entry points, never its own alarm). The classifier must answer
    // AwaitParentEstablishment — the child stays `admitted`, never claims, and the
    // researcher model never runs.
    for (let round = 0; round < 5; round++) {
      try {
        await runDurableObjectAlarm(stubFor(child, SUBAGENTS));
      } catch {
        // A child pass may reject while its cross-Object parent hint races the parent's own
        // doomed incarnation; the probes below carry the claim.
      }
      const rows = await laneRows(child, SUBAGENTS);

      expect(rows).toHaveLength(1);
      expect(rows[0]?.state, `child state in probe round ${round}`).toBe("admitted");
    }
    expect(childModelInvocations(ref)).toBe(0);
    expect(
      await scheduledAlarm(child, SUBAGENTS),
      "AwaitParentEstablishment is a stable external wait and must quiesce (#93)",
    ).toBeNull();

    // The WP1 admin surface names the deferral: the child Object explains its own lane.
    const explainedRaw: unknown = await Promise.resolve(
      stubFor(child, SUBAGENTS).explainEncoded({}),
    );

    const explained = await Effect.runPromise(ThreadObject.decodeAdminResponse(explainedRaw));

    expect(explained._tag).toBe("ExplainedRecovery");
    if (explained._tag === "ExplainedRecovery") {
      expect(explained.explanations).toHaveLength(1);
      expect(explained.explanations[0]?.decision._tag).toBe("AwaitParentEstablishment");
      expect(explained.explanations[0]?.disposition).toBe("deferred");
    }

    // Heal the transport: the parent's idempotent establishment burns the remaining armed
    // evictions, completes the child (lineage BEFORE readiness), and the delegation
    // converges on one invocation.
    healTransportFault(faultSuffix);
    await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
    expect(armedEvictionsRemaining(ref), "every armed eviction must have fired").toBe(0);
    await assertDelegationConverged(completedDelegation(receipt, ref));
    const childRecords = await readCanonical(child, SUBAGENTS);
    const tags = childRecords.map((envelope) => envelope.record.payload._tag);
    const lineageIndex = tags.indexOf("SubagentLineageRecorded");
    const firstTurnIndex = tags.indexOf("ModelResponseRecorded");

    expect(lineageIndex, "the child log must carry its lineage record").toBeGreaterThanOrEqual(0);
    expect(firstTurnIndex, "the child must have run exactly after establishment").toBeGreaterThan(
      lineageIndex,
    );
    expect(childModelInvocations(ref)).toBe(1);
  }, 60_000);

  // -------------------------------------------------------------------------
  // The suspension seam and the join/release ladder — a completed child is
  // never re-executed on a lost acknowledgment (§16.4).
  // -------------------------------------------------------------------------

  it("eviction at subagent:after-sibling-settle: the settled sibling is injected on resume, never re-executed", async () => {
    // Gated researcher (see the after-suspend row): the per-call sibling late-settle seam
    // only exists while the delegation actually suspends the batch.
    const location: DurableRuntimeFailpointLocation = "subagent:after-sibling-settle";
    const ref = lane(location);

    gateChildModel(ref);
    armRuntimeEviction(ref, location);
    const receipt = await submitCoordinator("sibling", ref, `${ref}-key`);
    const child = childThreadOf(receipt, ref);

    await drainDelegationUntil([ref], async () => armedEvictionsRemaining(ref) === 0);
    releaseChildModel(ref);
    await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
    await assertDelegationConverged(completedDelegation(receipt, ref));
    // The ordinary sibling's handler ran exactly once; its committed per-call settle survived
    // the eviction and the resumed batch injected it instead of re-executing (SUB-013).
    expect(siblingLookupInvocations(ref)).toBe(1);
    const records = await readCanonical(ref, SUBAGENTS);

    const siblingSettles = records.filter(
      (envelope) =>
        envelope.record.payload._tag === "ToolCallSettled" &&
        envelope.record.payload.toolCallId === `lookup-${ref}`,
    );

    expect(siblingSettles).toHaveLength(1);
  }, 40_000);

  it("eviction at subagent:after-join-append: the canonical join replays its accounting, the child never re-executes", async () => {
    await coordinatorEvictionRow("subagent:after-join-append");
  }, 40_000);

  it("eviction at subagent:after-release-pending: the frozen accounting applies exactly once", async () => {
    await coordinatorEvictionRow("subagent:after-release-pending");
  }, 40_000);

  it("eviction at subagent:after-release: the replay observes the settled call and released reservation", async () => {
    await coordinatorEvictionRow("subagent:after-release");
  }, 40_000);

  // -------------------------------------------------------------------------
  // recordChildSettled — at-least-once redelivery across the Object boundary.
  // -------------------------------------------------------------------------

  it("recordChildSettled redelivery answers not-waiting idempotently and mutates nothing", async () => {
    const ref = lane("child-settled-redelivery");
    const receipt = await submitCoordinator("coordinator", ref, `${ref}-key`);
    const child = childThreadOf(receipt, ref);

    await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
    const started = await assertDelegationConverged(completedDelegation(receipt, ref));

    // Re-deliver the child→parent settlement notification straight into the parent Object's
    // portCall — the exact envelope a redelivered cross-Object RPC would carry.
    const redelivery = await Effect.runPromise(
      encodePortRequest(
        LedgerRecordChildSettledCall.make({
          request: ChildSettledNotification.make({
            parentSubmissionId: receipt.submissionId,
            childSubmissionId: started.childSubmissionId,
          }),
        }),
      ),
    );

    const before = await readCanonical(ref, SUBAGENTS);
    const markersBefore = await settlementMarkers(ref);

    const instrumented = await runInDurableObject(stubFor(ref, SUBAGENTS), async (instance) => {
      const requestDescriptor = Object.getOwnPropertyDescriptor(redelivery, "request");

      if (requestDescriptor === undefined || !("value" in requestDescriptor)) {
        throw new Error("Expected an encoded port request with an own request data property");
      }
      let requestReads = 0;
      const envelope = Object.defineProperties({}, Object.getOwnPropertyDescriptors(redelivery));

      Object.defineProperty(envelope, "request", {
        enumerable: true,
        get: () => {
          requestReads += 1;

          return requestDescriptor.value;
        },
      });
      const response = await instance.portCall(envelope);

      return { requestReads, response };
    });

    expect(instrumented.requestReads).toBe(1);
    expect((await Effect.runPromise(decodePortResponse(instrumented.response)))._tag).toBe(
      "PortSucceeded",
    );
    for (let delivery = 0; delivery < 2; delivery++) {
      const decoded = await Effect.runPromise(
        decodePortResponse(await stubFor(ref, SUBAGENTS).portCall(redelivery)),
      );

      expect(decoded._tag).toBe("PortSucceeded");
      if (decoded._tag === "PortSucceeded") {
        expect(decoded.result._tag).toBe("LedgerRecordChildSettledResult");
        if (decoded.result._tag === "LedgerRecordChildSettledResult") {
          // The parent is settled, not suspended waiting on this child: `not-waiting`, the
          // port's documented idempotent replay answer.
          expect(decoded.result.outcome).toBe("not-waiting");
        }
      }
    }
    // Nothing moved: no canonical append, no marker growth, the lane stays settled.
    expect(await readCanonical(ref, SUBAGENTS)).toEqual(before);
    expect(await settlementMarkers(ref)).toEqual(markersBefore);
    const rows = await laneRows(ref, SUBAGENTS);

    expect(rows.map((row) => row.state)).toEqual(["settled"]);
  }, 40_000);

  // -------------------------------------------------------------------------
  // Issue #93: parent wake is durable before child finalization can commit.
  // -------------------------------------------------------------------------

  it("issue #93: child eviction after finalize cannot lose its precommitted parent wake", async () => {
    const ref = lane("lost-notification");
    const key = `${ref}-key`;

    // The researcher hangs mid-stream so the eviction can be armed with the child provably
    // active — no race against the child's own alarm-driven progress.
    gateChildModel(ref);
    const receipt = await submitCoordinator("coordinator", ref, key);
    const child = childThreadOf(receipt, ref);

    await drainDelegationUntil([ref], anyInState(ref, "suspended", SUBAGENTS));
    await waitFor(() => {
      kickWithoutAwaiting(child);

      return childModelInvocations(ref) === 1;
    }, "the hanging researcher invocation");
    await waitFor(
      async () => (await scheduledAlarm(ref, SUBAGENTS)) === null,
      "the parent AwaitChildSettlement alarm to quiesce (#93)",
    );

    // The child's Object dies right AFTER its settlement finalize commits. The protocol must
    // have durably routed `recordChildSettled` and dirtied the parent BEFORE this can happen.
    armStorageEviction(child, "ledger:finalize-settlement:after");
    releaseChildModel(ref);
    await waitFor(async () => {
      const rows = await laneRows(child, SUBAGENTS);

      return rows.length === 1 && rows[0]?.state === "settled";
    }, "the child settlement to commit before the eviction");
    expect(armedEvictionsRemaining(child)).toBe(0);
    expect(await settlementMarkers(ref)).toHaveLength(1);

    // Fire ONLY the parent's persisted alarm: the precommitted marker/generation resumes the
    // join after quiescence; no background polling contract is needed.
    await drainDelegationUntil([ref], allLanesSettled(ref, child));
    await assertDelegationConverged(completedDelegation(receipt, ref));
  }, 40_000);

  // -------------------------------------------------------------------------
  // Abort propagation parent→child across Objects (request-abort-and-join).
  // -------------------------------------------------------------------------

  it.each(["active", "unknown"])(
    "aborting the waiting parent joins its %s child before settlement",
    async (childState) => {
      const ref = lane(`abort-propagation-${childState}`);
      const key = `${ref}-key`;

      gateChildModel(ref);
      if (childState === "unknown") {
        uncertainChildRefs.add(ref);
        lostBookReplies.add(ref);
      }
      const receipt = await submitCoordinator("coordinator", ref, key);
      const child = childThreadOf(receipt, ref);

      await drainDelegationUntil([ref], anyInState(ref, "suspended", SUBAGENTS));
      await waitFor(() => {
        kickWithoutAwaiting(child);

        return childModelInvocations(ref) === 1;
      }, "the hanging researcher invocation");

      if (childState === "unknown") {
        armStorageEviction(child, "ledger:mark-unknown:after");
        releaseChildModel(ref);
        await drainDelegationUntil([child], anyInState(child, "unknown", SUBAGENTS));
        expect(supplierCountsFor(ref).book).toBe(1);
      }

      await abortParent(ref, receipt);
      // The parent's alarm passes propagate the abort across the Object boundary; the child's
      // active Attempt observes its durable intent, interrupts the hanging stream, settles
      // aborted, and the join wakes the parent (spec §13.1).
      await drainDelegationUntil([ref, child], allLanesSettled(ref, child));

      // Exactly one canonical child abort command, authored by the propagation (DUR-012).
      const childRecords = await readCanonical(child, SUBAGENTS);
      const abortRecords = payloadsOf(childRecords, "AbortRequested");

      expect(abortRecords).toHaveLength(1);
      const abortPayload = abortRecords[0]?.record.payload;

      if (abortPayload?._tag === "AbortRequested") {
        expect(abortPayload.author).toBe("subagent-parent-abort");
      }
      const childSettled = payloadsOf(childRecords, "SubmissionSettled")[0]?.record.payload;

      if (childSettled?._tag === "SubmissionSettled") {
        expect(childSettled.outcome).toBe("aborted");
      }

      // The join committed with the child's ACTUAL outcome, strictly BEFORE the parent's
      // aborted settlement ("the parent settles only after the joins").
      const parentRecords = await readCanonical(ref, SUBAGENTS);
      const joined = payloadsOf(parentRecords, "SubagentJoined");

      expect(joined).toHaveLength(1);
      const joinedPayload = joined[0]?.record.payload;

      if (joinedPayload?._tag !== "SubagentJoined") throw new Error("Expected SubagentJoined");
      expect(joinedPayload.childOutcome).toBe("aborted");
      const settledEnvelope = payloadsOf(parentRecords, "SubmissionSettled")[0];

      expect(settledEnvelope).toBeDefined();
      if (settledEnvelope !== undefined && joined[0] !== undefined) {
        expect(Number(joined[0].sequence)).toBeLessThan(Number(settledEnvelope.sequence));
      }
      expect(await parentOutcomeOf(ref)).toBe("aborted");
      expect(await reservationStatuses(ref)).toEqual(["released"]);
      // The interrupted researcher was invoked exactly once and never re-executed to abort it.
      expect(childModelInvocations(ref)).toBe(1);
      if (childState === "unknown") {
        expect(supplierCountsFor(ref).book).toBe(1);
        expect(payloadsOf(childRecords, "ToolCallUnknown")).toHaveLength(1);
        expect(payloadsOf(childRecords, "ToolCallSettled")).toHaveLength(0);
        expect(armedEvictionsRemaining(child)).toBe(0);
      }
    },
    40_000,
  );

  it("eviction at subagent:after-child-abort-intent: the replayed propagation is a no-op, never a second cross-Object command", async () => {
    const ref = lane("subagent:after-child-abort-intent");
    const key = `${ref}-key`;

    gateChildModel(ref);
    const receipt = await submitCoordinator("coordinator", ref, key);
    const child = childThreadOf(receipt, ref);

    await drainDelegationUntil([ref], anyInState(ref, "suspended", SUBAGENTS));
    await waitFor(() => {
      kickWithoutAwaiting(child);

      return childModelInvocations(ref) === 1;
    }, "the hanging researcher invocation");

    // The parent's propagation pass dies right AFTER the routed child abort intent commits
    // in the CHILD's Object — the recorded intent IS the propagation marker (spec §14).
    armRuntimeEviction(ref, "subagent:after-child-abort-intent");
    await abortParent(ref, receipt);
    await drainDelegationUntil([ref], allLanesSettled(ref, child));
    expect(armedEvictionsRemaining(ref), "the armed location must actually have fired").toBe(0);

    // One canonical child abort command across the killed pass and every replay: the
    // redelivered idempotent requestAbort returned the recorded intent unchanged (DUR-012).
    const childRecords = await readCanonical(child, SUBAGENTS);

    expect(payloadsOf(childRecords, "AbortRequested")).toHaveLength(1);
    const parentRecords = await readCanonical(ref, SUBAGENTS);

    expect(payloadsOf(parentRecords, "SubagentJoined")).toHaveLength(1);
    expect(await parentOutcomeOf(ref)).toBe("aborted");
    expect(await reservationStatuses(ref)).toEqual(["released"]);
    expect(childModelInvocations(ref)).toBe(1);
  }, 40_000);

  // -------------------------------------------------------------------------
  // The DO-unreachable establishment row — AdmissionIndeterminate is real
  // in DC, and it NEVER becomes a second admission (SUB-031).
  // -------------------------------------------------------------------------

  it("an unreachable child Object during establishment recovery yields Indeterminate, never a duplicate child, and converges when transport heals", async () => {
    const ref = lane("indeterminate-establishment");
    const key = `${ref}-key`;
    const faultSuffix = `:${delegateCallIdFor(ref)}`;

    // Both levers arm BEFORE the submit: the child Thread's name is suffix-derivable
    // from the ref, so the fault covers the child Object before it can ever be reached.
    armTransportFault(faultSuffix);
    armRuntimeEviction(ref, "subagent:after-reserve");
    const receipt = await submitCoordinator("coordinator", ref, key);
    const child = childThreadOf(receipt, ref);

    // Let the doomed incarnation die at after-reserve, then let MANY recovery passes retry
    // establishment against the unreachable admission authority.
    await drainDelegationUntil([ref], async () => armedEvictionsRemaining(ref) === 0);
    for (let round = 0; round < 20; round++) {
      try {
        await runDurableObjectAlarm(stubFor(ref, SUBAGENTS));
      } catch {
        // Typed pass failures (the SUB-031 wait/retry LedgerError) reject the delivery; the
        // pre-armed alarm survives for the next round.
      }
      await new Promise((resolve) => setTimeout(resolve, 15));
    }

    // SUB-031 under a genuinely unreachable authority: the parent lane is still owed its
    // settlement, NO child Submission exists in the child's Object, no start link was
    // recorded, and the researcher never ran — an Indeterminate answer admitted nothing.
    const parentLane = await laneRows(ref, SUBAGENTS);

    expect(parentLane).toHaveLength(1);
    expect(parentLane[0]?.state).not.toBe("settled");
    expect(await laneRows(child, SUBAGENTS)).toHaveLength(0);
    const duringFault = await readCanonical(ref, SUBAGENTS);

    expect(payloadsOf(duringFault, "SubagentStarted")).toHaveLength(0);
    expect(childModelInvocations(ref)).toBe(0);
    // Each recovery pass re-arms the alarm inside its own delivery, and workerd auto-retries
    // a rejected delivery on its own timers — an instant probe can land mid-delivery, after
    // the alarm slot was consumed but before the pass re-arms it. The durable SUB-031 claim
    // is that autonomous retry state keeps an alarm armed, so poll for it.
    await waitFor(
      async () => (await scheduledAlarm(ref, SUBAGENTS)) !== null,
      "indeterminate establishment to retain its autonomous retry alarm (#93)",
    );

    // Heal the transport: the next alarm passes resolve the admission (fresh), establish the
    // ONE child, and the delegation completes — no duplicate was ever possible.
    healTransportFault(faultSuffix);
    await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
    const parentRecords = await readCanonical(ref, SUBAGENTS);

    expect(payloadsOf(parentRecords, "SubagentRequested")).toHaveLength(1);
    await assertDelegationConverged(completedDelegation(receipt, ref));
  }, 60_000);

  // -------------------------------------------------------------------------
  // Clean cross-Object baseline: the whole S2 protocol driven by alarms alone.
  // -------------------------------------------------------------------------

  it("coordinator→researcher delegation joins across two Durable Objects with no eviction armed", async () => {
    const ref = lane("clean-delegation");
    const receipt = await submitCoordinator("coordinator", ref, `${ref}-key`);
    const child = childThreadOf(receipt, ref);

    await drainDelegationUntil([ref, child], allLanesSettled(ref, child));
    const started = await assertDelegationConverged(completedDelegation(receipt, ref));

    // The two lanes really are different Durable Objects: distinct Threads, and the
    // child's records live in the child Object's OWN storage (probed independently above).
    expect(started.childThreadId).not.toBe(ref);
  }, 40_000);
});
