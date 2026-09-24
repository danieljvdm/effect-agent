import {
  NodeDurableAgentRuntime,
  type NodeDurableAgentRuntimeOptions,
} from "@effect-agent/platform-node/node-durable-agent-runtime";
import {
  makeDurableResearchHarness,
  researchMission,
  s2CoordinatorSubmitAgent,
  s2TravelPlannerDeploymentId,
  s2TravelPlannerProducerId,
  s2TravelPlannerSubmitOptions,
} from "@effect-agent/testing/travel-planner";
import { NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Cause, Duration, Effect, Exit, FileSystem, Option, Schema, Stream } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointHandler,
  type DurableRuntimeFailpointLocation,
} from "effect-agent/durable-failpoint";
import { ThreadId, ToolCallId, type SubmissionId } from "effect-agent/identifiers";
import { type CanonicalRecordEnvelope } from "effect-agent/records";
import {
  AdmissionRequest,
  IdempotencyKey,
  ParentLinkage,
  Principal,
  RecoverySnapshotRequest,
  SubmissionLedger,
  SubmissionLookupById,
} from "effect-agent/submission-ledger";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { TestClock } from "effect/testing";

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodePrincipal = Schema.decodeSync(Principal);

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
  overrides?: Partial<NodeDurableAgentRuntimeOptions>,
): NodeDurableAgentRuntimeOptions => ({
  filename,
  deploymentId: s2TravelPlannerDeploymentId,
  producerId: s2TravelPlannerProducerId,
  observationPollInterval: 1,
  ...overrides,
});

// The failpoint abandons its claim; advance past the SQLite lease before resuming.
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

const submitParent = (thread: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;

    return yield* runtime.submit(
      s2CoordinatorSubmitAgent,
      researchMission,
      s2TravelPlannerSubmitOptions(decodeThreadId(thread), decodeIdempotencyKey(key)),
    );
  });

/** Drive one Thread lane through the S2 multi-binding worker entry point. */
const drive = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;

    return yield* runtime.processThreadResolved(threadId);
  });

const readLog = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    return yield* Stream.runCollect(store.read(ThreadRead.make({ threadId, limit: 1_024 })));
  });

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

describe("SQLite child Parent Link verification", () => {
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

            // Crash after the canonical request: the intended child identity (Thread,
            // principal, idempotency key) is now deterministic, guessable knowledge.
            arm.location = "subagent:after-request-append";
            const exit = yield* Effect.exit(drive(receipt.threadId));

            expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
            arm.location = undefined;
            yield* expireAbandonedLease;

            const requested = payloadsOf(yield* readLog(receipt.threadId), "SubagentRequested")[0]
              ?.record.payload;

            if (requested?._tag !== "SubagentRequested") {
              throw new Error("Expected SubagentRequested");
            }

            // An attacker squats the derived address with every IMMUTABLE fact matching
            // except the Parent Link, which names a forged parent Tool Call. Identifier
            // knowledge is never a capability (D10): the admission exists, but it can never
            // become "the same child".
            const fabricated = yield* ledger.admit(
              AdmissionRequest.make({
                threadId: requested.childThreadId,
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
            const resumed = yield* Effect.exit(drive(receipt.threadId));

            expect(failureTag(resumed)).toBe("LedgerError");
            const failure = failureOf(resumed);

            expect(
              typeof failure === "object" && failure !== null && "message" in failure
                ? String(failure.message)
                : "",
            ).toContain("fails closed");
            const log = yield* readLog(receipt.threadId);

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
              NodeDurableAgentRuntime.layerWithBindings(
                harness.bindings,
                runtimeOptions(`${directory}/idor.sqlite`, {
                  runtimeFailpoint: armableFailpoint(arm),
                  ownershipLeaseDuration: FAILPOINT_LEASE_MILLIS,
                }),
              ),
            ),
          );
        }),
      ),
  );
});
