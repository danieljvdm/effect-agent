import * as NodeHost from "@effect-agent/platform-node/node-durable-host";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Clock,
  Context,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import * as Agent from "effect-agent/agent";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpointError } from "effect-agent/durable-failpoint";
import { ThreadId } from "effect-agent/identifiers";
import { MessageDeliveryStore } from "effect-agent/message-delivery";
import { DefinitionDigestInput } from "effect-agent/records";
import * as Subagent from "effect-agent/subagent";
import { SubagentHost } from "effect-agent/subagent-host";
import {
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  SubmissionLedger,
} from "effect-agent/submission-ledger";
import { ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { WorkerCompletion, WorkerError } from "effect-agent/worker";
import { WorkerHostAuthorizer } from "effect-agent/worker-host";
import { LanguageModel, Model, Toolkit, type Response, type Tool } from "effect/unstable/ai";

const principal = Schema.decodeSync(Principal)("report-owner");
const sourceThreadId = Schema.decodeSync(ThreadId)("report-source");
const key = Schema.decodeSync(IdempotencyKey);
const definitions = DefinitionDigestInput.make({ agent: "report-v1", model: "v1", tools: [] });
const input = Schema.Struct({ question: Schema.String });
const output = Schema.Struct({ answer: Schema.String });

const withReportClock = Effect.fnUntraced(function* <A, E, R>(
  scenario: (advanceTo: (millis: number) => void) => Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E, Exclude<R, Scope.Scope>> {
  const native = yield* Clock.Clock;
  let offset = 0;

  // Only persisted wall-clock deadlines move; polling and timeouts keep native timers.
  const clock: Clock.Clock = {
    sleep: (duration) => native.sleep(duration),
    monotonicTimeNanosUnsafe: () => native.monotonicTimeNanosUnsafe(),
    monotonicTimeNanos: native.monotonicTimeNanos,
    currentTimeMillisUnsafe: () => native.currentTimeMillisUnsafe() + offset,
    currentTimeMillis: Effect.sync(() => native.currentTimeMillisUnsafe() + offset),
    currentTimeNanosUnsafe: () => native.currentTimeNanosUnsafe() + BigInt(offset) * 1000000n,
    currentTimeNanos: Effect.sync(
      () => native.currentTimeNanosUnsafe() + BigInt(offset) * 1000000n,
    ),
  };

  return yield* scenario((millis) => {
    offset = Math.max(offset, millis - native.currentTimeMillisUnsafe());
  }).pipe(Effect.scoped, Effect.provideService(Clock.Clock, clock));
});

const parts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "reply" },
  { type: "text-delta", id: "reply", delta: '{"answer":"done"}' },
  { type: "text-end", id: "reply" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const agent = <Tools extends Record<string, Tool.Any>>(
  id: string,
  toolkit: Toolkit.Toolkit<Tools>,
) =>
  Agent.withModel(
    Agent.make(id, {
      input,
      output,
      instructions: ({ question }) => `Answer as JSON for ${question}.`,
      inputPrompt: ({ question }) => `Application input: ${question}`,
      toolkit,
      policy: { maxTurns: 20, maxToolCalls: 20, maxDuration: "1 minute", toolConcurrency: 2 },
    }),
    Model.make(
      "scripted",
      id,
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.fromIterable(parts),
        }),
      ),
    ),
  );

it.live.each(["worker:after-report-append"] as const)(
  "recovers one prepared report for joined child inputs after %s and a Node restart",
  (failpoint) =>
    withReportClock((advanceTo) =>
      Effect.gen(function* () {
        const clock = yield* Clock.Clock;
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-report-" });
        const child = agent("report-child-agent", Toolkit.empty);

        const projected = yield* Ref.make(0);

        const declaration = Subagent.make("research", {
          target: child.definition,
          success: output,
          projectResult: (value) =>
            Ref.update(projected, (count) => count + 1).pipe(Effect.as(value)),
          policy: Subagent.SubagentPolicy.make({
            maxChildren: 8,
            maxConcurrency: 2,
            maxTurns: 2,
            maxToolCalls: 2,
            maxDuration: "2 seconds",
          }),
        });

        const background = Subagent.background(declaration, {
          start: true,
          followUp: true,
          reportToParent: true,
        });

        const source = agent("report-source-agent", background.toolkit);

        const registrations = [
          {
            agent: source,
            definitions,
          },
          { agent: child, definitions },
        ];

        const authority = Layer.succeed(WorkerHostAuthorizer)({
          authorize: (request) =>
            request.principal === principal
              ? Effect.succeed(principal)
              : WorkerError.make({ operation: request.operation, reason: "denied" }),
        });

        const options = {
          filename: `${directory}/runtime.sqlite`,
          deploymentId: "reports-v1",
          producerId: "report-node",
          workerConcurrency: 1,
          wakeScanInterval: 10,
          settlementPollInterval: 10,
        };

        const firstScope = yield* Scope.make();

        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

        const first = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(registrations, {
            ...options,
            runtimeFailpoint: (location) =>
              location === failpoint
                ? DurableRuntimeFailpointError.make({ location })
                : Effect.void,
          }).pipe(
            Layer.provide(authority),
            Layer.provide(background.layer),
            Layer.provide(Layer.succeedContext(Clock.Clock.context(clock))),
          ),
        ).pipe(Scope.provide(firstScope));

        const runtime = Context.get(first, DurableAgentRuntime);

        const sourceReceipt = yield* runtime.submitRegistered(
          source,
          { question: "launch complete" },
          {
            threadId: sourceThreadId,
            principal,
            idempotencyKey: key("source"),
          },
        );

        yield* runtime.processThreadResolved(sourceThreadId);
        const sourceSettlement = yield* runtime.awaitSettlement(sourceReceipt);

        expect(sourceSettlement.outcome).toBe("completed");

        const host = yield* runtime.workerHost({
          sourceThreadId,
          principal,
          sourceSubmissionId: sourceReceipt.submissionId,
        });

        const started = yield* Subagent.start(
          declaration,
          { question: "host input" },
          { idempotencyKey: key("first") },
        ).pipe(Effect.provideService(SubagentHost, host));

        const joined = yield* Subagent.followUp(
          declaration,
          started.worker,
          { question: "joined input" },
          { idempotencyKey: key("joined") },
        ).pipe(Effect.provideService(SubagentHost, host));

        const interrupted = yield* runtime
          .processThreadResolved(started.worker.threadId)
          .pipe(Effect.result);

        expect(interrupted).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "LedgerError", operation: "worker-completion" },
        });

        const firstLog = yield* Context.get(first, ThreadStore).export(
          ThreadExportRequest.make({ threadId: started.worker.threadId }),
        );

        const decisions = firstLog.records.flatMap(({ record }) =>
          record.payload._tag === "WorkerReportPrepared" ? [record.payload] : [],
        );

        expect(
          firstLog.records.flatMap(({ record }) =>
            record.payload._tag === "WorkerReportRefused" ? [record.payload] : [],
          ),
        ).toEqual([]);
        expect(decisions).toHaveLength(1);
        expect(yield* Ref.get(projected)).toBe(1);
        expect(
          (yield* Context.get(first, MessageDeliveryStore).list({
            ownerThreadId: started.worker.threadId,
            limit: 100,
          })).items,
        ).toHaveLength(0);
        yield* Scope.close(firstScope, Exit.void);

        // No live source Run or retained wake fiber survives this complete host restart.
        const second = yield* Layer.build(
          NodeHost.layer(registrations, options).pipe(
            Layer.provide(authority),
            Layer.provide(background.layer),
            Layer.provide(Layer.succeedContext(Clock.Clock.context(clock))),
          ),
        );

        const reopened = Context.get(second, DurableAgentRuntime);
        const store = Context.get(second, ThreadStore);
        const deliveries = Context.get(second, MessageDeliveryStore);
        const ledger = Context.get(second, SubmissionLedger);

        const report = yield* Effect.gen(function* () {
          for (;;) {
            const rows = yield* deliveries.list({
              ownerThreadId: started.worker.threadId,
              limit: 100,
            });

            const row = rows.items[0];

            if (rows.items.length === 1 && row !== undefined && row.receipt !== null)
              return { key: row.key, receipt: row.receipt };
            yield* Effect.sleep("10 millis");
          }
        }).pipe(Effect.timeout("10 seconds"));

        const firstResult = yield* reopened.awaitSettlement(started.delivery.receipt!);
        const joinedResult = yield* reopened.awaitSettlement(joined.receipt!);

        expect(firstResult.outcome).toBe("completed");
        expect(joinedResult.outcome).toBe("completed");
        expect((yield* reopened.awaitSettlement(report.receipt)).outcome).toBe("completed");

        // Canonical settlement may precede ledger finalization. Wait until every involved
        // submission has released ownership before advancing the report's persisted poll.
        yield* Effect.gen(function* () {
          for (;;) {
            let settled = true;

            for (const receipt of [
              sourceReceipt,
              started.delivery.receipt!,
              joined.receipt!,
              report.receipt,
            ]) {
              const snapshot = yield* ledger.loadRecoverySnapshot(
                RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
              );

              if (snapshot.submission.state !== "settled" || snapshot.ownership !== undefined)
                settled = false;
            }
            if (settled) return;
            yield* Effect.sleep("10 millis");
          }
        }).pipe(Effect.timeout("10 seconds"));

        const delivery = yield* deliveries.get(report.key);

        expect(delivery).not.toBeNull();
        if (delivery === null) throw new Error("Expected the completion report delivery");
        if (delivery.status !== "processed") {
          expect(delivery.status).toBe("accepted");
          expect(delivery.leaseUntilMillis).toBeNull();
          expect(
            Math.max(clock.currentTimeMillisUnsafe(), delivery.retry.nextAttemptAtMillis),
          ).toBeLessThan(delivery.deadlineAtMillis);
          // No yield between observing the idle delivery and changing its clock.
          advanceTo(delivery.retry.nextAttemptAtMillis);
        }

        yield* Effect.gen(function* () {
          for (;;) {
            const rows = yield* deliveries.list({
              ownerThreadId: started.worker.threadId,
              limit: 100,
            });

            if (rows.items.length === 1 && rows.items[0]?.status === "processed") return;
            yield* Effect.sleep("10 millis");
          }
        }).pipe(Effect.timeout("10 seconds"));

        const childLog = yield* store.export(
          ThreadExportRequest.make({ threadId: started.worker.threadId }),
        );

        const childSettlements = childLog.records.flatMap(({ record }) =>
          record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
        );

        const reports = childLog.records.flatMap(({ record }) =>
          record.payload._tag === "WorkerReportPrepared" ? [record.payload] : [],
        );

        const sourceLog = yield* store.export(
          ThreadExportRequest.make({ threadId: sourceThreadId }),
        );

        const inputs = sourceLog.records.flatMap(({ record }) =>
          record.payload._tag === "UserInputRecorded" ? [record.payload] : [],
        );

        expect(inputs).toHaveLength(2);
        {
          expect(inputs[1]?.input).toEqual({ question: "launch complete" });

          const message = yield* Schema.decodeUnknownEffect(WorkerCompletion)(
            inputs[1]?.messageAdmission,
          );

          expect(message).toMatchObject({
            _tag: "WorkerCompletion",
            budgetExhausted: false,
            report: {
              worker: started.worker,
              receipt: started.delivery.receipt!,
              runId: reports[0]?.runId,
              outcome: "completed",
              result: { answer: "done" },
            },
          });
          expect(JSON.stringify(sourceLog)).toContain("WorkerCompletion");
        }
        expect(inputs[1]?.runId).not.toBe(inputs[0]?.runId);

        const settlements = sourceLog.records.flatMap(({ record }) =>
          record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
        );

        expect(settlements).toHaveLength(2);
        expect(settlements.every((settlement) => settlement.outcome === "completed")).toBe(true);

        expect(reports).toHaveLength(1);
        expect(childSettlements).toHaveLength(2);
        expect(childSettlements.map((settlement) => settlement.runId)).toEqual([
          reports[0]?.runId,
          reports[0]?.runId,
        ]);
        expect(reports).toEqual(decisions);
        expect(
          childLog.records.filter(({ record }) => record.payload._tag === "RunCompleted"),
        ).toHaveLength(1);
        expect(
          childLog.records.filter(({ record }) => record.payload._tag === "ModelResponseRecorded"),
        ).toHaveLength(1);
        expect(yield* Ref.get(projected)).toBe(1);
        expect(
          (yield* deliveries.list({ ownerThreadId: started.worker.threadId, limit: 100 })).items,
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);
