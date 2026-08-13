import { NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Duration,
  Effect,
  Exit,
  FileSystem,
  Option,
  PlatformError,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";

import { SubagentDurableAccounting, SubagentReservationAmounts } from "@effect-agent/capabilities";
import { ConversationId, ToolCallId, type SubmissionId } from "@effect-agent/core";
import {
  NodeDurableHost,
  NodeDurableRuntime,
  type NodeDurableRuntimeOptions,
} from "@effect-agent/platform-node";
import {
  AbortCommand,
  AdmissionRequest,
  ClaimRequest,
  ConversationRead,
  ConversationStore,
  DurableAgentRuntime,
  DurableRuntimeFailpointError,
  IdempotencyKey,
  ParentLinkage,
  PersistedJson,
  Principal,
  ProducerId,
  RecoverySnapshotRequest,
  ResolutionCompletedWithResult,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  UnknownResolutionCommand,
  childConversationIdFor,
  runIdForSubmission,
  type CanonicalRecordEnvelope,
  type DurableRuntimeFailpointHandler,
  type DurableRuntimeFailpointLocation,
  type Receipt,
} from "@effect-agent/session";

import {
  DestinationShortlist,
  TravelPlannerSubagentDurabilityProfile,
  coordinatorConfidentialMarker,
  durableChildLookupCallId,
  durableResearchAllocation,
  durableResearchCallId,
  durableResearchFinding,
  durableResearchShortlist,
  encodedDestinationFacts,
  makeDurableResearchHarness,
  missionConfidentialMarker,
  researchMission,
  s2CoordinatorSubmitAgent,
  s2TravelPlannerDeploymentId,
  s2TravelPlannerPrincipal,
  s2TravelPlannerProducerId,
  s2TravelPlannerProfile,
  s2TravelPlannerSubmitOptions,
} from "../src/index.ts";

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodeProducerId = Schema.decodeSync(ProducerId);
const decodePrincipal = Schema.decodeSync(Principal);
const decodePersistedJson = Schema.decodeUnknownSync(PersistedJson);
const decodeAmounts = Schema.decodeUnknownEffect(SubagentReservationAmounts);
const decodeAccounting = Schema.decodeUnknownEffect(SubagentDurableAccounting);
const decodeShortlist = Schema.decodeUnknownEffect(DestinationShortlist);

const DELEGATE_CALL = decodeToolCallId(durableResearchCallId);

const dimensionKeys = [
  "turns",
  "toolCalls",
  "durationMillis",
  "inputTokens",
  "outputTokens",
  "costMicrousd",
  "resultBytes",
] as const;

/** Mutable failpoint switch: one SQLite stack, armed and cleared between drives. */
interface FailpointArm {
  location: DurableRuntimeFailpointLocation | undefined;
}

const armableFailpoint =
  (arm: FailpointArm): DurableRuntimeFailpointHandler =>
  (location) =>
    arm.location === location
      ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
      : Effect.void;

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableRuntimeOptions>,
): NodeDurableRuntimeOptions => ({
  filename,
  deploymentId: s2TravelPlannerDeploymentId,
  producerId: s2TravelPlannerProducerId,
  observationPollInterval: 1,
  ...overrides,
});

/**
 * Ownership lease for the in-process failpoint rows: a typed failpoint "kill" abandons its
 * claim without draining, and the SQLite ledger honors the live lease against every new claim
 * (D5). `expireAbandonedLease` advances the virtual clock past this bound so the SAME stack
 * re-claims with a bumped producer epoch — the crashed owner stays fenced exactly as a real
 * process replacement would be (the WP6 crash harness proves the same rows with real kills).
 */
const FAILPOINT_LEASE_MILLIS = 100;
const expireAbandonedLease = TestClock.adjust(Duration.millis(FAILPOINT_LEASE_MILLIS + 50));

const withTemporaryDirectory = <A, E>(
  use: (directory: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-travel-planner-s2-",
      });
      return yield* use(directory);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const submitParent = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    return yield* runtime.submit(
      s2CoordinatorSubmitAgent,
      researchMission,
      s2TravelPlannerSubmitOptions(decodeConversationId(conversation), decodeIdempotencyKey(key)),
    );
  });

/** Drive one Conversation lane through the S2 multi-binding worker entry point. */
const drive = (conversationId: ConversationId) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    return yield* runtime.processConversationResolved(conversationId);
  });

const readLog = (conversationId: ConversationId) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(ConversationRead.make({ conversationId, limit: 1_024 })),
    );
  });

const recordIds = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.recordId as string);

const payloadsOf = <Tag extends string>(
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tag: Tag,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  records.filter((envelope) => envelope.record.payload._tag === tag);

const parentState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value;
  });

const childReservations = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId }),
    );
    return snapshot.childReservations;
  });

const failureOf = <A, E>(exit: Exit.Exit<A, E>): unknown => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  return failure.value;
};

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  const error = failureOf(exit);
  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

/**
 * Deterministic virtual-time wait: poll the ledger between bounded
 * `TestClock` advances so wake-scan fallbacks and settlement polls fire on
 * the virtual clock while forked worker fibers make hint-driven progress. No
 * wall-clock time is involved (plan §7: deterministic rows only; real
 * process kills live in the WP6 crash harness).
 */
const awaitSettledState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    for (let iteration = 0; iteration < 600; iteration += 1) {
      const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
      if (Option.isSome(snapshot) && snapshot.value.state === "settled") {
        return snapshot.value;
      }
      yield* TestClock.adjust(Duration.millis(250));
    }
    return yield* Effect.die(
      new Error("The Submission did not settle within the bounded virtual-time window"),
    );
  });

/** Drive parent → child → parent to full settlement and return the shared identities. */
const runHappyPath = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const receipt = yield* submitParent(conversation, key);
    const childConversationId = childConversationIdFor(receipt.submissionId, DELEGATE_CALL);
    yield* drive(receipt.conversationId);
    const childSettlements = yield* drive(childConversationId);
    expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
    const settlements = yield* drive(receipt.conversationId);
    expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
    return { receipt, childConversationId };
  });

const shortlistFromSettlement = (records: ReadonlyArray<CanonicalRecordEnvelope>) =>
  Effect.gen(function* () {
    const settled = payloadsOf(records, "SubmissionSettled")[0]?.record.payload;
    if (settled?._tag !== "SubmissionSettled") throw new Error("Expected SubmissionSettled");
    expect(settled.outcome).toBe("completed");
    return yield* decodeShortlist(settled.result);
  });

describe("TEST-014 S2 durable Travel Planner Subagent delegation (DN)", () => {
  it("pins the DN durable attached Subagent claim and never claims exactly-once child effects", () => {
    const decoded = Schema.decodeUnknownSync(TravelPlannerSubagentDurabilityProfile)(
      Schema.encodeSync(TravelPlannerSubagentDurabilityProfile)(s2TravelPlannerProfile),
    );
    expect(decoded).toEqual(s2TravelPlannerProfile);
    expect(s2TravelPlannerProfile).toEqual({
      deploymentClass: "DN",
      durableAttachedSubagents: true,
      canonicalSchemaVersion: 1,
      subagentReplaySafe: true,
      childExternalEffectsExactlyOnce: false,
      cloudflareEquivalence: false,
    });
  });

  it.effect(
    "re-runs the S1 delegation as accepted work on SQLite: establish, waitingForChild without a permit, durable wake, verified join, conserved reservation",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDurableResearchHarness();
          yield* Effect.gen(function* () {
            const receipt = yield* submitParent("travel-planner-s2-happy", "s2-happy-1");
            const parentRunId = runIdForSubmission(receipt.submissionId);
            const childConversationId = childConversationIdFor(receipt.submissionId, DELEGATE_CALL);

            // Phase 1: establishment ends with the parent suspended waitingForChild. The lane
            // holds no worker permit and is not claimable (SUB-030), and no in-process child
            // fiber ever ran (spec §12 step 10).
            const first = yield* drive(receipt.conversationId);
            expect(first).toHaveLength(0);
            expect((yield* parentState(receipt.submissionId)).state).toBe("suspended");
            const ledger = yield* SubmissionLedger;
            const claimed = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: receipt.conversationId,
                producerId: decodeProducerId("travel-planner-s2-probe"),
              }),
            );
            expect(Option.isNone(claimed)).toBe(true);
            expect(yield* harness.childModelCalls).toBe(0);
            const afterEstablish = yield* readLog(receipt.conversationId);
            expect(recordIds(afterEstablish)).toContain(
              `subagent-requested:${parentRunId}:${durableResearchCallId}`,
            );
            expect(recordIds(afterEstablish)).toContain(
              `subagent-started:${parentRunId}:${durableResearchCallId}`,
            );
            // The parent-owned reservation is open with the policy-derived allocation
            // (SUB-010): budget is unavailable while the child obligation is outstanding.
            const reserved = yield* childReservations(receipt.submissionId);
            expect(reserved.map((row) => row.status)).toEqual(["reserved"]);
            const reservedRow = reserved[0];
            if (reservedRow === undefined) throw new Error("Expected the child reservation row");
            expect(yield* decodeAmounts(reservedRow.allocation)).toEqual(durableResearchAllocation);

            // Phase 2: the child lane runs to Settlement under its own Attempt ownership and
            // wakes the parent durably (recordChildSettled → input-applied).
            const childSettlements = yield* drive(childConversationId);
            expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            expect(yield* harness.childModelCalls).toBe(2);
            expect(yield* harness.guideInvocations).toBe(1);
            expect((yield* parentState(receipt.submissionId)).state).toBe("input-applied");
            const childLog = yield* readLog(childConversationId);
            expect(recordIds(childLog)).toContain(`subagent-lineage:${childConversationId}`);
            // Context isolation (SUB-006/SUB-015): the child saw only the projected brief,
            // never the coordinator's transcript or the mission's confidential markers.
            for (const prompt of yield* harness.childPrompts) {
              expect(prompt).toContain("research:museums");
              expect(prompt).not.toContain(missionConfidentialMarker);
              expect(prompt).not.toContain(coordinatorConfidentialMarker);
            }

            // Phase 3: the woken parent joins the VERIFIED child Settlement in one atomic
            // batch (SUB-019) and settles completed with the projected shortlist.
            const settlements = yield* drive(receipt.conversationId);
            expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            const log = yield* readLog(receipt.conversationId);
            expect(yield* shortlistFromSettlement(log)).toEqual(durableResearchShortlist("LHR"));
            const joined = payloadsOf(log, "SubagentJoined");
            expect(joined).toHaveLength(1);
            expect(joined[0]?.batchId).toBe(
              `subagent-join:${parentRunId}:${durableResearchCallId}`,
            );
            const joinSettle = log.find(
              (envelope) =>
                envelope.record.recordId ===
                `tool-settled:${parentRunId}:1:${durableResearchCallId}`,
            );
            expect(joinSettle?.batchId).toBe(
              `subagent-join:${parentRunId}:${durableResearchCallId}`,
            );
            expect(
              joinSettle?.record.payload._tag === "ToolCallSettled"
                ? joinSettle.record.payload.result
                : undefined,
            ).toEqual(durableResearchFinding("LHR"));

            // Reservation conservation across the durable join (spec §7/§12 join step 6,
            // D11): the canonical SubagentJoined carries the final accounting decision; the
            // released row froze exactly that decision, and consumed + released equals the
            // allocation in every dimension. Structural usage is rebuilt from canonical child
            // evidence; token/cost dimensions consume the reservation conservatively.
            const joinedPayload = joined[0]?.record.payload;
            if (joinedPayload?._tag !== "SubagentJoined")
              throw new Error("Expected SubagentJoined");
            const accounting = yield* decodeAccounting(joinedPayload.finalAccounting);
            expect(accounting.basis).toBe("reserved-conservative");
            expect(accounting.allocation).toEqual(durableResearchAllocation);
            for (const key of dimensionKeys) {
              expect(accounting.consumed[key] + accounting.released[key]).toBe(
                accounting.allocation[key],
              );
            }
            expect(joinedPayload.usageSummary).toEqual({ turns: 2, toolCalls: 1 });
            const released = yield* childReservations(receipt.submissionId);
            expect(released.map((row) => row.status)).toEqual(["released"]);
            expect(released[0]?.accounting).toEqual(joinedPayload.finalAccounting);

            // The parent log never copies the child transcript (SUB-015): the guide's
            // highlights stay in the child Conversation; only the advisory crossed.
            const parentLogJson = JSON.stringify(log.map((envelope) => envelope.record.payload));
            expect(parentLogJson).not.toContain("Barbican brutalism walk");
            const finalPrompt = (yield* harness.parentPrompts).at(-1);
            expect(finalPrompt).toBeDefined();
            expect(finalPrompt).toContain("London favors museum mornings");
            expect(finalPrompt).not.toContain("Barbican brutalism walk");
            expect(yield* harness.parentModelCalls).toBe(2);
          }).pipe(
            Effect.provide(
              NodeDurableRuntime.layer(
                runtimeOptions(`${directory}/happy.sqlite`, { bindings: harness.bindings }),
              ),
            ),
          );
        }),
      ),
  );

  it.effect(
    "a kill at subagent:after-request-append resumes to the same child Receipt and one child Conversation (SUB-016/SUB-017)",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDurableResearchHarness();
          const arm: FailpointArm = { location: undefined };
          yield* Effect.gen(function* () {
            const receipt = yield* submitParent("travel-planner-s2-duplicate", "s2-duplicate-1");
            const childConversationId = childConversationIdFor(receipt.submissionId, DELEGATE_CALL);

            arm.location = "subagent:after-request-append";
            const exit = yield* Effect.exit(drive(receipt.conversationId));
            expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
            arm.location = undefined;
            yield* expireAbandonedLease;

            // Idempotent handler re-entry replays establishment from the canonical
            // SubagentRequested record: the derived admission idempotency key converges on ONE
            // child — a duplicate admission attempt can never create a second Conversation.
            yield* drive(receipt.conversationId);
            const childSettlements = yield* drive(childConversationId);
            expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            const settlements = yield* drive(receipt.conversationId);
            expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            expect(yield* harness.childModelCalls).toBe(2);

            const log = yield* readLog(receipt.conversationId);
            expect(payloadsOf(log, "SubagentRequested")).toHaveLength(1);
            expect(payloadsOf(log, "SubagentStarted")).toHaveLength(1);
            expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
            const childLog = yield* readLog(childConversationId);
            expect(payloadsOf(childLog, "ConversationCreated")).toHaveLength(1);
            expect(payloadsOf(childLog, "SubagentLineageRecorded")).toHaveLength(1);

            // The one recorded Receipt is the one admitted child's Receipt (SUB-017): the
            // canonical start link and the authoritative admission row agree byte-for-byte.
            const started = payloadsOf(log, "SubagentStarted")[0]?.record.payload;
            if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");
            const child = yield* parentState(started.childSubmissionId);
            expect(child.receiptId).toBe(started.childReceiptId);
            expect(child.state).toBe("settled");
            const ledger = yield* SubmissionLedger;
            const resolution = yield* ledger.resolveAdmission(
              SubmissionLookupByKey.make({
                conversationId: childConversationId,
                principal: s2TravelPlannerPrincipal,
                idempotencyKey: decodeIdempotencyKey(
                  `subagent:${runIdForSubmission(receipt.submissionId)}:${durableResearchCallId}`,
                ),
              }),
            );
            expect(resolution._tag).toBe("Admitted");
          }).pipe(
            Effect.provide(
              NodeDurableRuntime.layer(
                runtimeOptions(`${directory}/duplicate.sqlite`, {
                  bindings: harness.bindings,
                  runtimeFailpoint: armableFailpoint(arm),
                  ownershipLeaseDuration: FAILPOINT_LEASE_MILLIS,
                }),
              ),
            ),
          );
        }),
      ),
  );

  it.effect(
    "a lost join acknowledgment replays the accounting and never re-executes the completed child",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const locations = [
            "subagent:after-join-append",
            "subagent:after-release-pending",
            "subagent:after-release",
          ] satisfies ReadonlyArray<DurableRuntimeFailpointLocation>;
          for (const location of locations) {
            const slug = location.replaceAll(":", "-");
            const harness = yield* makeDurableResearchHarness();
            const arm: FailpointArm = { location: undefined };
            yield* Effect.gen(function* () {
              const receipt = yield* submitParent(`travel-planner-s2-${slug}`, `s2-join-${slug}`);
              const childConversationId = childConversationIdFor(
                receipt.submissionId,
                DELEGATE_CALL,
              );

              yield* drive(receipt.conversationId);
              const childSettlements = yield* drive(childConversationId);
              expect(childSettlements.map((settlement) => settlement.outcome)).toEqual([
                "completed",
              ]);
              expect(yield* harness.childModelCalls).toBe(2);

              arm.location = location;
              const exit = yield* Effect.exit(drive(receipt.conversationId));
              expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
              arm.location = undefined;
              yield* expireAbandonedLease;

              // The canonical SubagentJoined record (or the frozen releasePending decision) is
              // the replay source: re-entry completes the release idempotently and the settled
              // child is NEVER re-executed merely because the acknowledgment was lost.
              const settlements = yield* drive(receipt.conversationId);
              expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
              expect(yield* harness.childModelCalls).toBe(2);
              expect(yield* harness.guideInvocations).toBe(1);
              const log = yield* readLog(receipt.conversationId);
              expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
              const reservations = yield* childReservations(receipt.submissionId);
              expect(reservations.map((row) => row.status)).toEqual(["released"]);
            }).pipe(
              Effect.provide(
                NodeDurableRuntime.layer(
                  runtimeOptions(`${directory}/${slug}.sqlite`, {
                    bindings: harness.bindings,
                    runtimeFailpoint: armableFailpoint(arm),
                    ownershipLeaseDuration: FAILPOINT_LEASE_MILLIS,
                  }),
                ),
              ),
            );
          }
        }),
      ),
  );

  it.effect(
    "workerConcurrency=1: the suspended parent frees the single worker, which runs the child to Settlement and the woken parent joins (spec §12 smallest-pool proof)",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDurableResearchHarness();
          yield* Effect.gen(function* () {
            const host = yield* NodeDurableHost;
            const receipt = yield* host.submit(
              s2CoordinatorSubmitAgent,
              researchMission,
              s2TravelPlannerSubmitOptions(
                decodeConversationId("travel-planner-s2-smallest-pool"),
                decodeIdempotencyKey("s2-pool-1"),
              ),
            );
            // ONE worker loop serves BOTH lanes. If the waiting parent held its execution
            // permit, this pool could never run the child and the bounded virtual-time wait
            // below would fail — completing at all is the SUB-030 permit-release proof.
            yield* Effect.forkScoped(host.runResolvedWorkers);
            const settled = yield* awaitSettledState(receipt.submissionId);
            expect(settled.settledOutcome).toBe("completed");

            // The single pool executed the parent (2 model calls) and the child (2 model
            // calls) exactly once each, joined the verified Settlement, and released the
            // reservation.
            expect(yield* harness.parentModelCalls).toBe(2);
            expect(yield* harness.childModelCalls).toBe(2);
            expect(yield* harness.guideInvocations).toBe(1);
            const log = yield* readLog(receipt.conversationId);
            expect(yield* shortlistFromSettlement(log)).toEqual(durableResearchShortlist("LHR"));
            expect(payloadsOf(log, "SubagentRequested")).toHaveLength(1);
            expect(payloadsOf(log, "SubagentStarted")).toHaveLength(1);
            expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
            expect(
              (yield* childReservations(receipt.submissionId)).map((row) => row.status),
            ).toEqual(["released"]);
          }).pipe(
            // The fork Scope closes (interrupting the worker loop) BEFORE the host stack
            // drains and the SQLite resources close — no fiber outlives its subscriber.
            Effect.scoped,
            Effect.provide(
              NodeDurableHost.layerStack(
                runtimeOptions(`${directory}/pool.sqlite`, {
                  bindings: harness.bindings,
                  workerConcurrency: 1,
                }),
              ),
            ),
          );
        }),
      ),
  );

  it.effect(
    "an unknown child Tool outcome blocks the parent; resolveUnknown from a second runtime handle converges the child, wakes the parent, and joins",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDurableResearchHarness();
          const filename = `${directory}/unknown.sqlite`;
          const arm: FailpointArm = { location: undefined };

          // Handle 1: the parent establishes and suspends; the child Attempt dies after its
          // guide lookup became prepared-without-outcome (the external effect MAY have
          // happened). Closing this stack's Scope drains the held child ownership so the
          // second handle observes a fenced, classifiable lane.
          const crashed = yield* Effect.gen(function* () {
            const receipt = yield* submitParent("travel-planner-s2-unknown", "s2-unknown-1");
            const childConversationId = childConversationIdFor(receipt.submissionId, DELEGATE_CALL);
            yield* drive(receipt.conversationId);
            expect((yield* parentState(receipt.submissionId)).state).toBe("suspended");
            const started = payloadsOf(yield* readLog(receipt.conversationId), "SubagentStarted")[0]
              ?.record.payload;
            if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");

            arm.location = "tools:after-prepared-append";
            const exit = yield* Effect.exit(drive(childConversationId));
            expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
            arm.location = undefined;
            expect(yield* harness.childModelCalls).toBe(1);
            expect(yield* harness.guideInvocations).toBe(0);
            return { receipt, childConversationId, childSubmissionId: started.childSubmissionId };
          }).pipe(
            Effect.provide(
              NodeDurableRuntime.layer(
                runtimeOptions(filename, {
                  bindings: harness.bindings,
                  runtimeFailpoint: armableFailpoint(arm),
                }),
              ),
            ),
          );

          // Handle 2 ("second process"): startup recovery classifies honestly — the child
          // becomes an aged Unknown-Outcome obligation (never replayed, DUR-009/SUB-021) and
          // the suspended parent keeps waiting without consuming a permit.
          yield* Effect.gen(function* () {
            const host = yield* NodeDurableHost;
            const runtime = yield* DurableAgentRuntime;
            const childReport = host.startupRecovery.find(
              (report) => report.submissionId === crashed.childSubmissionId,
            );
            expect(childReport?.decision._tag).toBe("MarkUnknown");
            expect(childReport?.disposition).toBe("unknown");
            const parentReport = host.startupRecovery.find(
              (report) => report.submissionId === crashed.receipt.submissionId,
            );
            expect(parentReport?.decision._tag).toBe("AwaitChildSettlement");
            expect(parentReport?.disposition).toBe("deferred");
            expect((yield* parentState(crashed.childSubmissionId)).state).toBe("unknown");
            expect((yield* parentState(crashed.receipt.submissionId)).state).toBe("suspended");

            // The blocked lanes grant no claims and fabricate nothing: driving either lane
            // settles nothing and the child model/guide never re-execute.
            expect(yield* drive(crashed.childConversationId)).toEqual([]);
            expect(yield* drive(crashed.receipt.conversationId)).toEqual([]);
            expect(yield* harness.childModelCalls).toBe(1);
            expect(yield* harness.guideInvocations).toBe(0);
            const childRunId = runIdForSubmission(crashed.childSubmissionId);
            const lookupCallId = durableChildLookupCallId("LHR");
            expect(recordIds(yield* readLog(crashed.childConversationId))).toContain(
              `tool-unknown:${childRunId}:1:${lookupCallId}`,
            );

            // The authorized DUR-017 resolution path records the operator-recovered guide
            // truth; the framework never guessed or replayed the lookup.
            yield* runtime.resolveUnknown(
              UnknownResolutionCommand.make({
                submissionId: crashed.childSubmissionId,
                toolCallId: decodeToolCallId(lookupCallId),
                author: "travel-ops",
                reason: "The deterministic guide confirmed the lookup completed.",
                resolution: ResolutionCompletedWithResult.make({
                  result: decodePersistedJson(encodedDestinationFacts("LHR")),
                  isFailure: false,
                }),
              }),
            );
            const childSettlements = yield* drive(crashed.childConversationId);
            expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            // Turn 1 was never re-invoked; only the report Turn ran after resolution.
            expect(yield* harness.childModelCalls).toBe(2);
            expect(yield* harness.guideInvocations).toBe(0);
            const childLog = yield* readLog(crashed.childConversationId);
            expect(payloadsOf(childLog, "ToolCallUnknown")).toHaveLength(1);
            expect(payloadsOf(childLog, "ToolCallResolved")).toHaveLength(1);
            const settledLookup = childLog.find(
              (envelope) =>
                envelope.record.recordId === `tool-settled:${childRunId}:1:${lookupCallId}`,
            )?.record.payload;
            expect(
              settledLookup?._tag === "ToolCallSettled" ? settledLookup.result : undefined,
            ).toEqual(encodedDestinationFacts("LHR"));

            // The wake converges the parent: verified join, completed settlement.
            const settlements = yield* drive(crashed.receipt.conversationId);
            expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
            const log = yield* readLog(crashed.receipt.conversationId);
            expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
            expect(yield* shortlistFromSettlement(log)).toEqual(durableResearchShortlist("LHR"));
          }).pipe(
            Effect.provide(
              NodeDurableHost.layerStack(runtimeOptions(filename, { bindings: harness.bindings })),
            ),
          );
        }),
      ),
  );

  it.effect(
    "request-abort-and-join: the aborted child joins as the framework failure and the parent settles aborted strictly after the join (spec §13.1)",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDurableResearchHarness();
          yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;
            const receipt = yield* submitParent("travel-planner-s2-abort", "s2-abort-1");
            const parentRunId = runIdForSubmission(receipt.submissionId);

            yield* drive(receipt.conversationId);
            expect((yield* parentState(receipt.submissionId)).state).toBe("suspended");
            const started = payloadsOf(yield* readLog(receipt.conversationId), "SubagentStarted")[0]
              ?.record.payload;
            if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");

            yield* runtime.abort(
              AbortCommand.make({
                submissionId: receipt.submissionId,
                author: "traveler",
                reason: "The mission was cancelled while research was outstanding.",
              }),
            );

            // PropagateChildAbort issues the ONE idempotent durable child abort command while
            // the parent stays suspended for the join (spec §13.1); no delegation or child
            // code runs anywhere on the abort path.
            const reports = yield* runtime.runRecovery;
            const parentReport = reports.find(
              (report) => report.submissionId === receipt.submissionId,
            );
            expect(parentReport?.decision._tag).toBe("PropagateChildAbort");
            expect(parentReport?.disposition).toBe("repaired");

            // The durable child abort settles the never-started child aborted through
            // recovery. Which pass carries SettleAborted depends only on adapter scan order,
            // so converge boundedly instead of pinning per-pass composition.
            let child = yield* parentState(started.childSubmissionId);
            for (let pass = 0; pass < 3 && child.state !== "settled"; pass += 1) {
              yield* runtime.runRecovery;
              child = yield* parentState(started.childSubmissionId);
            }
            expect(child.state).toBe("settled");
            expect(child.settledOutcome).toBe("aborted");
            expect(yield* harness.childModelCalls).toBe(0);

            // The settled child wakes the parent (ResumeWaitingParent replays the idempotent
            // ownership-free wake when the settlement-time notification raced the crash); the
            // parent NEVER settles in recovery while its join obligation is open.
            let parent = yield* parentState(receipt.submissionId);
            for (let pass = 0; pass < 3 && parent.state === "suspended"; pass += 1) {
              yield* runtime.runRecovery;
              parent = yield* parentState(receipt.submissionId);
            }
            expect(parent.state).toBe("input-applied");

            // The woken parent joins the child's ACTUAL aborted outcome, then settles aborted.
            const settlements = yield* drive(receipt.conversationId);
            expect(settlements.map((settlement) => settlement.outcome)).toEqual(["aborted"]);

            const log = yield* readLog(receipt.conversationId);
            const joined = payloadsOf(log, "SubagentJoined");
            expect(joined).toHaveLength(1);
            const joinedPayload = joined[0]?.record.payload;
            if (joinedPayload?._tag !== "SubagentJoined")
              throw new Error("Expected SubagentJoined");
            expect(joinedPayload.childOutcome).toBe("aborted");
            const accounting = joinedPayload.finalAccounting;
            expect(
              typeof accounting === "object" && accounting !== null && "basis" in accounting
                ? accounting.basis
                : undefined,
            ).toBe("aborted-conservative");
            // The join committed BEFORE the aborted parent settlement (append-only order).
            const settledEnvelope = log.find(
              (envelope) => envelope.record.recordId === `settlement:${receipt.submissionId}`,
            );
            expect(joined[0] !== undefined && settledEnvelope !== undefined).toBe(true);
            if (joined[0] !== undefined && settledEnvelope !== undefined) {
              expect(Number(joined[0].sequence)).toBeLessThan(Number(settledEnvelope.sequence));
            }
            // The bounded parent-aborted projection settles the delegation call as a failure.
            const joinSettle = log.find(
              (envelope) =>
                envelope.record.recordId ===
                `tool-settled:${parentRunId}:1:${durableResearchCallId}`,
            )?.record.payload;
            if (joinSettle?._tag !== "ToolCallSettled") throw new Error("Expected ToolCallSettled");
            expect(joinSettle.isFailure).toBe(true);
            expect(
              typeof joinSettle.result === "object" &&
                joinSettle.result !== null &&
                "errorTag" in joinSettle.result
                ? joinSettle.result.errorTag
                : undefined,
            ).toBe("SubagentParentAborted");
            expect(
              (yield* childReservations(receipt.submissionId)).map((row) => row.status),
            ).toEqual(["released"]);
          }).pipe(
            Effect.provide(
              NodeDurableRuntime.layer(
                runtimeOptions(`${directory}/abort.sqlite`, { bindings: harness.bindings }),
              ),
            ),
          );
        }),
      ),
  );

  it.effect(
    "a fabricated child admission at the derived child identity fails Parent Link verification fail-closed (IDOR, D10)",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDurableResearchHarness();
          const arm: FailpointArm = { location: undefined };
          yield* Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const receipt = yield* submitParent("travel-planner-s2-idor", "s2-idor-1");

            // Crash after the canonical request: the intended child identity (Conversation,
            // principal, idempotency key) is now deterministic, guessable knowledge.
            arm.location = "subagent:after-request-append";
            const exit = yield* Effect.exit(drive(receipt.conversationId));
            expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
            arm.location = undefined;
            yield* expireAbandonedLease;
            const requested = payloadsOf(
              yield* readLog(receipt.conversationId),
              "SubagentRequested",
            )[0]?.record.payload;
            if (requested?._tag !== "SubagentRequested") {
              throw new Error("Expected SubagentRequested");
            }

            // An attacker squats the derived address with every IMMUTABLE fact matching
            // except the Parent Link, which names a forged parent Tool Call. Identifier
            // knowledge is never a capability (D10): the admission exists, but it can never
            // become "the same child".
            const fabricated = yield* ledger.admit(
              AdmissionRequest.make({
                conversationId: requested.childConversationId,
                principal: decodePrincipal(requested.childPrincipal),
                idempotencyKey: decodeIdempotencyKey(requested.childIdempotencyKey),
                agentId: requested.targetAgentId,
                agentDigests: requested.targetDigests,
                deploymentId: s2TravelPlannerDeploymentId,
                inputPayload: requested.childInput,
                inputDigest: requested.childInputDigest,
                parentLinkage: ParentLinkage.make({
                  parentSubmissionId: receipt.submissionId,
                  parentToolCallId: decodeToolCallId("research-forged-1"),
                }),
              }),
            );

            // The resumed establishment verifies the admitted row against the canonical
            // request and fails closed (SUB-016): no start link, no join, no child execution,
            // and the reservation stays an unavailable, visible obligation.
            const resumed = yield* Effect.exit(drive(receipt.conversationId));
            expect(failureTag(resumed)).toBe("LedgerError");
            const failure = failureOf(resumed);
            expect(
              typeof failure === "object" && failure !== null && "message" in failure
                ? String(failure.message)
                : "",
            ).toContain("fails closed");
            const log = yield* readLog(receipt.conversationId);
            expect(payloadsOf(log, "SubagentStarted")).toHaveLength(0);
            expect(payloadsOf(log, "SubagentJoined")).toHaveLength(0);
            expect(yield* harness.childModelCalls).toBe(0);
            expect(yield* harness.guideInvocations).toBe(0);
            expect(
              (yield* childReservations(receipt.submissionId)).map((row) => row.status),
            ).toEqual(["reserved"]);
            expect((yield* parentState(receipt.submissionId)).state).not.toBe("settled");
            // The squatted admission never acquired this parent's linkage.
            const fake = yield* parentState(fabricated.submissionId);
            expect(fake.parentLinkage?.parentToolCallId).toBe("research-forged-1");
            expect(fake.state).toBe("admitted");
          }).pipe(
            Effect.provide(
              NodeDurableRuntime.layer(
                runtimeOptions(`${directory}/idor.sqlite`, {
                  bindings: harness.bindings,
                  runtimeFailpoint: armableFailpoint(arm),
                  ownershipLeaseDuration: FAILPOINT_LEASE_MILLIS,
                }),
              ),
            ),
          );
        }),
      ),
  );

  it.effect(
    "a Receipt for an unrelated Conversation observes nothing of the child log (D10 honest scope)",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const harness = yield* makeDurableResearchHarness();
          yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;
            const settled = yield* runHappyPath("travel-planner-s2-observed", "s2-observe-1");
            const started = payloadsOf(
              yield* readLog(settled.receipt.conversationId),
              "SubagentStarted",
            )[0]?.record.payload;
            if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");

            // A legitimately obtained Receipt for a DIFFERENT Conversation: its observation
            // stream is scoped to its own lane and carries nothing of the parent/child
            // delegation — no lineage, no child identity, no child transcript content.
            // (Authenticated per-read Principal/Tenant authorization is a P7 deliverable;
            // service possession is the S2 trust boundary, stated as a non-claim like
            // P5's DUR-017 scoping.)
            const unrelated: Receipt = yield* submitParent(
              "travel-planner-s2-unrelated",
              "s2-unrelated-1",
            );
            const unrelatedLog = yield* readLog(unrelated.conversationId);
            expect(unrelatedLog.length).toBeGreaterThan(0);
            const observed = yield* runtime
              .observe(unrelated)
              .pipe(Stream.take(unrelatedLog.length), Stream.runCollect);
            expect(recordIds(observed)).toEqual(recordIds(unrelatedLog));

            const observedJson = JSON.stringify(observed.map((envelope) => envelope.record));
            expect(observedJson).not.toContain(settled.childConversationId);
            expect(observedJson).not.toContain(started.childSubmissionId);
            expect(observedJson).not.toContain("SubagentLineageRecorded");
            expect(observedJson).not.toContain("SubagentRequested");
            expect(observedJson).not.toContain("Barbican brutalism walk");
            expect(observedJson).not.toContain("London favors museum mornings");
          }).pipe(
            Effect.provide(
              NodeDurableRuntime.layer(
                runtimeOptions(`${directory}/observe.sqlite`, { bindings: harness.bindings }),
              ),
            ),
          );
        }),
      ),
  );
});
