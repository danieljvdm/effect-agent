import { expect, it } from "@effect/vitest";
import { Context, Effect, type Layer, Schema, SchemaGetter } from "effect";
import * as Agent from "effect-agent/agent";
import { RunId, SettlementId, ThreadId } from "effect-agent/identifiers";
import { MessageStatus } from "effect-agent/messaging";
import { MessagingHost } from "effect-agent/messaging-host";
import { IdempotencyKey, Receipt } from "effect-agent/receipt";
import type { WorkerReportPreparationFailure, WorkerRunReport } from "effect-agent/subagent-host";
import { type Tool, Toolkit } from "effect/unstable/ai";

import { automaticReporting } from "../../src/capabilities/internal/subagent-reporting.ts";
import * as Messaging from "../../src/capabilities/Messaging.ts";
import * as Subagent from "../../src/capabilities/Subagent.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<T extends true> = T;
class Encode extends Context.Service<Encode, string>()("message-test/Encode") {}

const text = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.passthrough(),
    encode: SchemaGetter.transformEffect((value) => Effect.as(Encode, value)),
  }),
);

const child = Agent.make("projected-child", {
  input: Schema.NumberFromString,
  output: Schema.NumberFromString,
  instructions: "Return a number",
  toolkit: Toolkit.empty,
});

const declaration = Subagent.make("research", {
  target: child,
  success: Schema.NumberFromString,
  projectResult: (output, _completion, parameters) => Effect.succeed(output + parameters),
});

const report = automaticReporting(declaration);

const worker = Schema.decodeSync(Subagent.Worker(declaration))({
  schemaVersion: 1,
  delegationId: declaration.delegationId,
  targetAgentId: child.id,
  threadId: "worker",
});

const receipt = Schema.decodeSync(Receipt)({
  threadId: "worker",
  submissionId: "input",
  receiptId: "receipt",
  queueSequence: 1,
});

const run: WorkerRunReport = {
  worker,
  context: {
    source: { _tag: "programmatic", threadId: worker.threadId, agentId: child.id },
    policy: child.policy,
    depth: 1,
  },
  observation: {
    _tag: "Settled",
    receipt,
    runId: Schema.decodeSync(RunId)("run"),
    settlementId: Schema.decodeSync(SettlementId)("settlement"),
    outcome: "completed",
    encodedParameters: "7",
    encodedResult: "14",
    budgetExhausted: false,
  },
};

const peerAgent = Agent.make("peer", {
  input: text,
  output: Schema.String,
  instructions: "Reply",
  toolkit: Toolkit.empty,
});

const peer = Messaging.peer("advisor", { target: peerAgent });
const native = Messaging.sendTool(peer);
const key = Schema.decodeSync(IdempotencyKey)("message");
const send = Messaging.send(peer, "question", { idempotencyKey: key });

const proofs: [
  Assert<Equal<Effect.Services<ReturnType<typeof report.prepare>>, never>>,
  Assert<
    Equal<
      Effect.Error<ReturnType<typeof report.prepare>>,
      WorkerReportPreparationFailure | Subagent.SubagentProjectionFailure
    >
  >,
  Assert<Equal<Effect.Services<typeof send>, Encode | MessagingHost>>,
  Assert<Equal<Tool.Parameters<typeof native.tool>, string>>,
  Assert<Equal<Layer.Services<typeof native.layer>, Encode>>,
] = [true, true, true, true, true];

it.effect("projects saved child output and parameters into a standard typed report", () =>
  Effect.gen(function* () {
    expect((yield* report.prepare(run)).message.report).toMatchObject({
      outcome: "completed",
      result: "21",
    });
    expect(
      yield* report.prepare({
        ...run,
        observation: {
          ...run.observation,
          outcome: "aborted",
          encodedResult: { private: "ignored" },
        },
      }),
    ).toMatchObject({
      message: { report: { outcome: "aborted", failure: { classification: "child-aborted" } } },
    });
    expect(
      yield* report
        .prepare({ ...run, observation: { ...run.observation, encodedResult: { invalid: true } } })
        .pipe(Effect.result),
    ).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "WorkerReportPreparationFailure", stage: "projection" },
    });
    expect(proofs.every(Boolean)).toBe(true);
  }),
);

it.effect(
  "preserves peer input encoding and keeps source, principal and destination out of native parameters",
  () =>
    Effect.gen(function* () {
      const pending = MessageStatus.make({
        message: { ownerThreadId: Schema.decodeSync(ThreadId)("source"), messageId: key },
        status: "pending",
        receipt: null,
        settlement: null,
        reason: null,
      });

      const service: MessagingHost["Service"] = {
        ...MessagingHost.unavailable,
        send: (request) => {
          expect(request.target).toBe(peerAgent);
          expect(request.encodedInput).toBe("question");
          expect(Object.keys(request).sort()).toEqual([
            "encodedInput",
            "idempotencyKey",
            "name",
            "target",
          ]);

          return Effect.succeed(pending);
        },
      };

      expect(
        yield* send.pipe(
          Effect.provideService(MessagingHost, service),
          Effect.provideService(Encode, "encoder"),
        ),
      ).toEqual(pending);
      expect(
        yield* send.pipe(
          Effect.provideService(MessagingHost, MessagingHost.unavailable),
          Effect.provideService(Encode, "encoder"),
          Effect.result,
        ),
      ).toMatchObject({ _tag: "Failure", failure: { reason: "unavailable" } });
      expect(native.tool.name).toBe("advisor_send");
    }),
);
