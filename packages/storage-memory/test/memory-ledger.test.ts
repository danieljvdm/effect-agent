import {
  MemorySubmissionLedgerLive,
  memorySubmissionLedgerLayer,
} from "@effect-agent/storage-memory/memory-submission-ledger";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { AgentId, ThreadId } from "effect-agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest } from "effect-agent/records";
import {
  AdmissionRequest,
  AdmissionPolicyError,
  IdempotencyKey,
  Principal,
  SubmissionLedger,
  SubmissionAdmissionFence,
  SubmissionLookupByKey,
} from "effect-agent/submission-ledger";
import { submissionLedgerConformanceCases } from "effect-agent/testing/submission-ledger-conformance";

const testLayer = Layer.mergeAll(MemorySubmissionLedgerLive, NodeCrypto.layer);

const threadId = Schema.decodeSync(ThreadId)("thread-memory-ledger-1");
const principal = Schema.decodeSync(Principal)("principal-memory-ledger");
const agentId = Schema.decodeSync(AgentId)("agent-memory-ledger");
const deploymentId = Schema.decodeSync(DeploymentId)("deployment-memory-ledger");

const definitionDigest = Schema.decodeSync(Digest)("e".repeat(64));

const agentDigests = DefinitionDigests.make({
  agent: definitionDigest,
  model: definitionDigest,
  tools: definitionDigest,
});

const admissionRequest = (idempotencyKey: string, digestSeed: string): AdmissionRequest =>
  AdmissionRequest.make({
    threadId,
    principal,
    idempotencyKey: Schema.decodeSync(IdempotencyKey)(idempotencyKey),
    agentId,
    agentDigests,
    deploymentId,
    inputPayload: { work: idempotencyKey },
    inputDigest: Schema.decodeSync(Digest)(digestSeed.padEnd(64, "0")),
  });

describe("MemorySubmissionLedger", () => {
  it.effect("rejects asynchronous memory policy and closes its suspended resources", () => {
    let finalized = false;

    const fence = Layer.succeed(SubmissionAdmissionFence)({
      check: () =>
        Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                finalized = true;
              }),
            );

            return yield* Effect.never;
          }),
        ),
    });

    return Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const request = admissionRequest("async-policy", "ac");

      expect(yield* ledger.admit(request).pipe(Effect.flip)).toMatchObject({
        reason: "unavailable",
        code: "synchronous-memory-policy-required",
      });
      expect(finalized).toBe(true);
      expect((yield* ledger.resolveAdmission(SubmissionLookupByKey.make(request)))._tag).toBe(
        "NotAdmitted",
      );
    }).pipe(Effect.provide(memorySubmissionLedgerLayer().pipe(Layer.provide(fence))));
  });

  it.effect(
    "resolves exact structurally equivalent receipts before the current admission fence",
    () => {
      let allowed = true;

      return Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        const request = AdmissionRequest.make({
          ...admissionRequest("fenced", "aa"),
          admissionGroup: "watch",
          admissionFence: { revision: "1", key: "task", policyId: "test" },
        });

        const first = yield* ledger.admit(request);

        allowed = false;

        const replay = yield* ledger.admit(
          AdmissionRequest.make({
            ...request,
            admissionFence: { key: "task", policyId: "test", revision: "1" },
          }),
        );

        expect(replay.submissionId).toBe(first.submissionId);
        expect(
          yield* ledger
            .admit(
              AdmissionRequest.make({
                ...admissionRequest("new-fenced", "ab"),
                admissionGroup: "watch",
                admissionFence: { key: "task", policyId: "test", revision: "1" },
              }),
            )
            .pipe(Effect.flip),
        ).toMatchObject({ _tag: "AdmissionPolicyError", reason: "refused" });
      }).pipe(
        Effect.provide(
          memorySubmissionLedgerLayer().pipe(
            Layer.provide(
              Layer.succeed(SubmissionAdmissionFence, {
                check: () =>
                  Effect.suspend(() => {
                    return allowed
                      ? Effect.void
                      : AdmissionPolicyError.make({
                          reason: "refused",
                          code: "revision-changed",
                        });
                  }),
              }),
            ),
          ),
        ),
      );
    },
  );

  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () => conformanceCase.run.pipe(Effect.provide(testLayer)));
    }
  });
});
