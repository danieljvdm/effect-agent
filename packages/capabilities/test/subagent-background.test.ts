import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import {
  AgentId,
  DelegationId,
  RunId,
  SettlementId,
  ThreadId,
  ToolCallId,
} from "@effect-agent/core/Identifiers";
import { IdempotencyKey, JoinedToHost, Receipt } from "@effect-agent/core/Receipt";
import {
  BackgroundSpawnTool,
  SubagentGrant,
  isSubagentToolAllowed,
} from "@effect-agent/core/SubagentContract";
import {
  WorkerContext,
  WorkerError,
  WorkerOperationTool,
  WorkerStarted,
} from "@effect-agent/core/Worker";
import {
  BackgroundReporting,
  SubagentHost,
  type StartWorkerRequest,
  type FollowUpWorkerRequest,
  type WorkerRunReport,
} from "@effect-agent/engine/SubagentHost";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { Toolkit } from "effect/unstable/ai";

const policy = AgentPolicy.make({
  maxTurns: 5,
  maxToolCalls: 8,
  maxDuration: "1 minute",
  toolConcurrency: 2,
});

const caller = WorkerContext.make({
  source: {
    _tag: "programmatic",
    agentId: Schema.decodeSync(AgentId)("parent"),
    threadId: Schema.decodeSync(ThreadId)("source"),
  },
  policy,
  depth: 0,
});

const target = Agent.make("worker-target", {
  input: Schema.Struct({ amount: Schema.NumberFromString }),
  output: Schema.Struct({ answer: Schema.NumberFromString }),
  toolkit: Toolkit.empty,
  instructions: "Return the answer.",
  policy,
});

const delegation = Subagent.make("research", {
  target,
  parameters: Schema.Struct({ amount: Schema.NumberFromString }),
  success: Schema.Struct({ total: Schema.NumberFromString, exhausted: Schema.Boolean }),
  prepareInput: (parameters) => Effect.succeed(parameters),
  projectResult: (output, context, parameters) =>
    Effect.succeed({
      total: output.answer + parameters.amount,
      exhausted: context.budgetExhausted,
    }),
});

const receipt = Schema.decodeSync(Receipt)({
  threadId: "worker",
  receiptId: "receipt-1",
  submissionId: "submission-1",
  queueSequence: 1,
});

const nextReceipt = Schema.decodeSync(Receipt)({
  threadId: "worker",
  receiptId: "receipt-2",
  submissionId: "submission-2",
  queueSequence: 2,
});

const started = Schema.decodeSync(WorkerStarted)({
  worker: {
    schemaVersion: 1,
    delegationId: delegation.delegationId,
    targetAgentId: target.id,
    threadId: receipt.threadId,
  },
  receipt,
});

const worker = Schema.decodeSync(Subagent.Worker(delegation))(started.worker);
const key = Schema.decodeSync(IdempotencyKey)("explicit-start");

const settled: WorkerRunReport["observation"] = {
  _tag: "Settled",
  receipt,
  runId: Schema.decodeSync(RunId)("child-run"),
  settlementId: Schema.decodeSync(SettlementId)("settlement"),
  outcome: "completed",
  encodedParameters: { amount: "7" },
  encodedResult: { answer: "14" },
  budgetExhausted: true,
};

const host = (overrides: Partial<SubagentHost["Service"]> = {}): SubagentHost["Service"] => ({
  ...SubagentHost.unavailable,
  context: Effect.succeed(caller),
  resolveTargetPolicy: () => Effect.succeed(Option.none()),
  start: () => Effect.succeed(started),
  followUp: () => Effect.succeed(nextReceipt),
  inspect: () => Effect.succeed(settled),
  await: () => Effect.succeed(settled),
  list: () =>
    Effect.succeed({
      items: [{ worker: started.worker, latestReceipt: nextReceipt, state: "active" }],
      next: null,
    }),
  cancel: () => Effect.void,
  ...overrides,
});

class ProjectionDenied extends Schema.TaggedError<ProjectionDenied>()("ProjectionDenied", {}) {}

describe("Subagent background authoring", () => {
  it.effect("derives default background contracts directly from the exact Agent target", () =>
    Effect.gen(function* () {
      const background = Subagent.background(target, {
        start: true,
        followUp: true,
        inspect: true,
        reportToParent: true,
      });

      const directStarted = WorkerStarted.make({
        ...started,
        worker: { ...started.worker, delegationId: Schema.decodeSync(DelegationId)(target.id) },
      });

      const requests: Array<StartWorkerRequest | FollowUpWorkerRequest> = [];

      const service = host({
        context: Effect.succeed({
          ...caller,
          source: {
            ...caller.source,
            _tag: "tool",
            runId: Schema.decodeSync(RunId)("parent-run"),
            toolCallId: Schema.decodeSync(ToolCallId)("direct-call"),
          },
        }),
        start: (request) =>
          Effect.sync(() => {
            requests.push(request);

            return directStarted;
          }),
        followUp: (request) =>
          Effect.sync(() => {
            requests.push(request);

            return nextReceipt;
          }),
      });

      const toolkit = yield* background.toolkit.pipe(Effect.provide(background.layer));

      const invoke = toolkit
        .handle("worker-target_start", { amount: "7" })
        .pipe(Effect.flatMap(Stream.runCollect), Effect.provideService(SubagentHost, service));

      expect((yield* invoke)[0]?.result).toEqual(directStarted);
      yield* toolkit
        .handle("worker-target_follow_up", {
          worker: directStarted.worker,
          parameters: { amount: "9" },
        })
        .pipe(Effect.flatMap(Stream.runCollect), Effect.provideService(SubagentHost, service));

      const observed = yield* toolkit
        .handle("worker-target_inspect", {
          worker: directStarted.worker,
          receipt,
        })
        .pipe(Effect.flatMap(Stream.runCollect), Effect.provideService(SubagentHost, service));

      expect(observed[0]?.result).toMatchObject({
        outcome: "completed",
        result: { output: { answer: 14 }, budgetExhausted: true },
      });
      expect(requests.map((request) => request.encodedInput)).toEqual([
        { amount: "7" },
        { amount: "9" },
      ]);
      expect(requests.every((request) => request.target === target)).toBe(true);

      const descriptor = Context.get(
        background.tools["worker-target_start"].annotations,
        BackgroundReporting,
      );

      if (descriptor === undefined) return yield* Effect.die("Missing reporting descriptor");
      expect(descriptor.target).toBe(target);

      const prepared = yield* descriptor
        .prepare({
          worker: directStarted.worker,
          context: caller,
          observation: settled,
        })
        .pipe(Effect.provide(background.layer));

      expect(prepared.message?.report).toMatchObject({
        outcome: "completed",
        result: { output: { answer: "14" }, budgetExhausted: true },
      });
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it("rejects reporting from another version of the same direct Agent identity", () => {
    const otherVersion = Agent.make("worker-target", {
      input: target.input,
      output: target.output,
      toolkit: Toolkit.empty,
      instructions: "A different version.",
    });

    const reporting = Subagent.reporting(Subagent.make("worker-target", { target: otherVersion }), {
      input: Schema.String,
      prepare: () => Effect.succeed("done"),
    });

    expect(() => Subagent.background(target, { start: true, reportToParent: reporting })).toThrow(
      "same subagent declaration and target",
    );
  });

  it.effect("isolates versioned report projections and closes their scoped resources", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let released = 0;

        const version = (label: string) =>
          Subagent.background(
            Subagent.make("research", {
              target,
              success: Schema.String,
              projectResult: () =>
                Effect.acquireRelease(Effect.succeed(label), () =>
                  Effect.sync(() => {
                    released++;
                  }),
                ),
            }),
            { start: true, reportToParent: true },
          );

        const first = version("first");
        const second = version("second");
        const services = yield* Layer.build(Layer.merge(first.layer, second.layer));

        for (const [background, label] of [
          [first, "first"],
          [second, "second"],
        ] as const) {
          const descriptor = Context.get(
            background.tools.research_start.annotations,
            BackgroundReporting,
          );

          if (descriptor === undefined) return yield* Effect.die("Missing reporting descriptor");

          const prepared = yield* descriptor
            .prepare({ worker, context: caller, observation: settled })
            .pipe(Effect.provide(services));

          expect(prepared.message?.report).toMatchObject({ outcome: "completed", result: label });
        }
        expect(released).toBe(2);
        expect(
          Context.get(
            Subagent.background(delegation, { start: true }).tools.research_start.annotations,
            BackgroundReporting,
          ),
        ).toBeUndefined();
      }),
    ),
  );

  it.effect("bounds the encoded automatic projection before creating a completion", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const background = Subagent.background(
          Subagent.make("research", {
            ...delegation,
            policy: Subagent.SubagentPolicy.make({
              maxChildren: 1,
              maxConcurrency: 1,
              maxTurns: 1,
              maxToolCalls: 1,
              maxDuration: "1 second",
              maxResultBytes: 1,
            }),
          }),
          { start: true, reportToParent: true },
        );

        const descriptor = Context.get(
          background.tools.research_start.annotations,
          BackgroundReporting,
        );

        if (descriptor === undefined) return yield* Effect.die("Missing reporting descriptor");

        const refused = yield* descriptor
          .prepare({ worker, context: caller, observation: settled })
          .pipe(Effect.provide(background.layer), Effect.flip);

        expect(refused).toMatchObject({
          _tag: "WorkerReportPreparationFailure",
          stage: "projection",
        });
      }),
    ),
  );

  it("rejects an optional mapper belonging to another declaration", () => {
    const other = Subagent.make("other", { target });

    const reporting = Subagent.reporting(other, {
      input: Schema.String,
      prepare: () => Effect.succeed("done"),
    });

    expect(() =>
      Subagent.background(delegation, { start: true, reportToParent: reporting }),
    ).toThrow("same subagent declaration and target");
  });

  // Regression: https://github.com/danieljvdm/effect-agent/commit/43882d187248665eaf7fd46950b3bc617edcb73d
  it.effect(
    "resolves captured target policy after preparation and applies only explicit declaration narrowing",
    () =>
      Effect.gen(function* () {
        const events: Array<string> = [];

        const captured = AgentPolicy.make({
          maxTurns: 9,
          maxToolCalls: 12,
          maxDuration: "2 minutes",
          toolConcurrency: 3,
        });

        const declared = Subagent.make("research", {
          target,
          parameters: target.input,
          prepareInput: (input) =>
            Effect.sync(() => {
              events.push("prepare");

              return input;
            }),
          policy: Subagent.SubagentPolicy.make({
            maxChildren: 1,
            maxConcurrency: 1,
            descendantInvocations: 1,
            maxTurns: 7,
            maxToolCalls: 10,
            maxDuration: "3 minutes",
          }),
        });

        yield* Subagent.start(
          declared,
          { amount: 7 },
          { idempotencyKey: key, budgetScope: "worker-run" },
        ).pipe(
          Effect.provideService(
            SubagentHost,
            host({
              resolveTargetPolicy: (request) =>
                Effect.sync(() => {
                  events.push("resolve");
                  expect(request.target).toBe(target);
                  expect(request.encodedInput).toEqual({ amount: "7" });

                  return Option.some(captured);
                }),
              start: (request) =>
                Effect.sync(() => {
                  events.push("start");
                  expect(request.target).toBe(target);
                  expect(request.policy).toMatchObject({
                    maxTurns: 7,
                    maxToolCalls: 10,
                    toolConcurrency: 3,
                  });
                  expect(request.policy.tokenBudget).toBeUndefined();
                  expect(request.policy.costBudgetMicrousd).toBeUndefined();
                  expect(request.budget).toMatchObject({
                    allocation: { turns: 7, toolCalls: 10 },
                    descendantInvocations: 1,
                  });

                  return started;
                }),
            }),
          ),
        );
        expect(events).toEqual(["prepare", "resolve", "start"]);
      }),
  );

  it.effect(
    "encodes typed starts and same-thread follow-ups, and projects saved parameters and target output",
    () =>
      Effect.gen(function* () {
        const requests = yield* Ref.make<ReadonlyArray<StartWorkerRequest | FollowUpWorkerRequest>>(
          [],
        );

        const sources = yield* Ref.make<ReadonlyArray<Subagent.SubagentPrepareContext>>([]);

        const declared = Subagent.make("research", {
          ...delegation,
          prepareInput: (parameters, source) =>
            Ref.update(sources, (all) => [...all, source]).pipe(Effect.as(parameters)),
        });

        const service = host({
          start: (request) =>
            Ref.update(requests, (all) => [...all, request]).pipe(Effect.as(started)),
          followUp: (request) =>
            Ref.update(requests, (all) => [...all, request]).pipe(Effect.as(nextReceipt)),
        });

        const first = yield* Subagent.start(declared, { amount: 7 }, { idempotencyKey: key }).pipe(
          Effect.provideService(SubagentHost, service),
        );

        const second = yield* Subagent.followUp(
          declared,
          first.worker,
          { amount: 9 },
          { idempotencyKey: Schema.decodeSync(IdempotencyKey)("follow-up") },
        ).pipe(Effect.provideService(SubagentHost, service));

        const observed = yield* Subagent.inspect(declared, first.worker, first.receipt).pipe(
          Effect.provideService(SubagentHost, service),
        );

        expect(second).toEqual(nextReceipt);
        expect(observed).toEqual({
          _tag: "Settled",
          receipt,
          runId: "child-run",
          settlementId: "settlement",
          outcome: "completed",
          result: { total: 21, exhausted: true },
        });
        const recorded = yield* Ref.get(requests);

        expect(recorded[0]).toMatchObject({
          target,
          idempotencyKey: key,
          encodedInput: { amount: "7" },
          encodedParameters: { amount: "7" },
          budget: { allocation: { turns: 5, toolCalls: 8 } },
        });
        expect(recorded[0]?.target).toBe(target);
        expect(recorded[1]).toMatchObject({
          worker: first.worker,
          encodedInput: { amount: "9" },
          encodedParameters: { amount: "9" },
        });
        for (const source of yield* Ref.get(sources)) {
          expect(source).toEqual({
            source: "programmatic",
            delegationId: "research",
            parent: { agentId: "parent", threadId: "source" },
          });
          expect("toolCallId" in source).toBe(false);
        }
        expect(Schema.encodeSync(Subagent.Worker(declared))(first.worker)).toEqual(started.worker);
      }),
  );

  it.effect("preserves exact pending receipts and lists validated worker identities", () =>
    Effect.gen(function* () {
      const service = host({ inspect: () => Effect.succeed({ _tag: "Pending", receipt }) });

      expect(
        yield* Subagent.inspect(delegation, worker, receipt).pipe(
          Effect.provideService(SubagentHost, service),
        ),
      ).toEqual({ _tag: "Pending", receipt });
      expect(
        yield* Subagent.list(delegation, { limit: 1 }).pipe(
          Effect.provideService(SubagentHost, service),
        ),
      ).toEqual({ items: [{ worker, latestReceipt: nextReceipt, state: "active" }], next: null });

      const wrongReceipt = yield* Subagent.inspect(delegation, worker, receipt).pipe(
        Effect.provideService(
          SubagentHost,
          host({ inspect: () => Effect.succeed({ _tag: "Pending", receipt: nextReceipt }) }),
        ),
        Effect.flip,
      );

      expect(wrongReceipt).toMatchObject({
        _tag: "WorkerError",
        operation: "inspect",
        reason: "receipt-mismatch",
      });
    }),
  );

  it.effect(
    "rejects a different declaration target and a mismatched receipt before calling the host",
    () =>
      Effect.gen(function* () {
        const other = Subagent.make("research", {
          target: Agent.make("other-target", {
            input: Schema.String,
            output: Schema.String,
            toolkit: Toolkit.empty,
            instructions: "Other.",
          }),
        });

        const otherWorker = Schema.decodeSync(Subagent.Worker(other))({
          ...started.worker,
          targetAgentId: other.target.id,
        });

        const calls = yield* Ref.make(0);
        const service = host({ cancel: () => Ref.update(calls, (count) => count + 1) });

        expect(
          yield* Subagent.cancel(delegation, otherWorker, receipt).pipe(
            Effect.provideService(SubagentHost, service),
            Effect.flip,
          ),
        ).toMatchObject({ reason: "worker-mismatch" });
        const unrelated = Schema.decodeSync(Receipt)({ ...receipt, threadId: "unrelated" });

        expect(
          yield* Subagent.cancel(delegation, worker, unrelated).pipe(
            Effect.provideService(SubagentHost, service),
            Effect.flip,
          ),
        ).toMatchObject({ reason: "receipt-mismatch" });
        expect(yield* Ref.get(calls)).toBe(0);
      }),
  );

  it.effect("fails closed for unavailable and denying hosts and preserves depth denial", () =>
    Effect.gen(function* () {
      for (const service of [
        SubagentHost.unavailable,
        host({ start: () => WorkerError.make({ operation: "start", reason: "denied" }) }),
      ]) {
        const error = yield* Subagent.start(
          delegation,
          { amount: 7 },
          { idempotencyKey: key },
        ).pipe(Effect.provideService(SubagentHost, service), Effect.flip);

        expect(error._tag).toBe("WorkerError");
      }

      const denied = yield* Subagent.start(delegation, { amount: 7 }, { idempotencyKey: key }).pipe(
        Effect.provideService(
          SubagentHost,
          host({ context: Effect.succeed({ ...caller, depth: 1 }) }),
        ),
        Effect.flip,
      );

      expect(denied).toMatchObject({ _tag: "SubagentPrestartDenied", reason: "nested-delegation" });
    }),
  );

  it.effect("preserves author projection failures and rejects invalid saved schema values", () =>
    Effect.gen(function* () {
      const declared = Subagent.make("research", {
        ...delegation,
        failure: ProjectionDenied,
        projectResult: () => Effect.fail(ProjectionDenied.make({})),
      });

      expect(
        yield* Subagent.inspect(declared, worker, receipt).pipe(
          Effect.provideService(SubagentHost, host()),
          Effect.flip,
        ),
      ).toBeInstanceOf(ProjectionDenied);
      const malformed = { ...settled, encodedParameters: { amount: {} } };

      expect(
        yield* Subagent.inspect(delegation, worker, receipt).pipe(
          Effect.provideService(SubagentHost, host({ inspect: () => Effect.succeed(malformed) })),
          Effect.flip,
        ),
      ).toMatchObject({ _tag: "SubagentProjectionFailure", stage: "result" });
    }),
  );

  it.effect("projects failed and aborted settlements without leaking encoded child data", () =>
    Effect.gen(function* () {
      for (const outcome of ["failed", "aborted"] as const) {
        const result = yield* Subagent.inspect(delegation, worker, receipt).pipe(
          Effect.provideService(
            SubagentHost,
            host({
              inspect: () =>
                Effect.succeed({
                  ...settled,
                  outcome,
                  encodedResult: { message: "PRIVATE-CHILD-DATA" },
                }),
            }),
          ),
        );

        expect(result).toMatchObject({
          _tag: "Settled",
          outcome,
          failure: {
            _tag: "SubagentExecutionFailure",
            classification: outcome === "failed" ? "child-failed" : "child-aborted",
          },
        });
        expect(JSON.stringify(result)).not.toContain("PRIVATE-CHILD-DATA");
      }
    }),
  );

  it.effect("interrupts only the waiter and preserves JoinedToHost on explicit cancellation", () =>
    Effect.gen(function* () {
      const waiting = yield* Deferred.make<void>();
      const cancelled = yield* Ref.make(0);

      const conflict = JoinedToHost.make({
        submissionId: receipt.submissionId,
        hostSubmissionId: nextReceipt.submissionId,
      });

      const service = host({
        await: () => Deferred.succeed(waiting, undefined).pipe(Effect.andThen(Effect.never)),
        cancel: () =>
          Ref.update(cancelled, (count) => count + 1).pipe(Effect.andThen(Effect.fail(conflict))),
      });

      const fiber = yield* Subagent.await(delegation, worker, receipt).pipe(
        Effect.provideService(SubagentHost, service),
        Effect.forkChild,
      );

      yield* Deferred.await(waiting);
      yield* Fiber.interrupt(fiber);
      const interrupted = yield* Fiber.await(fiber);

      expect(Exit.isFailure(interrupted) && Cause.hasInterrupts(interrupted.cause)).toBe(true);
      expect(yield* Ref.get(cancelled)).toBe(0);
      expect(
        yield* Subagent.cancel(delegation, worker, receipt).pipe(
          Effect.provideService(SubagentHost, service),
          Effect.flip,
        ),
      ).toBe(conflict);
      expect(yield* Ref.get(cancelled)).toBe(1);
      expect(
        yield* Subagent.await(delegation, worker, receipt).pipe(
          Effect.provideService(SubagentHost, host()),
        ),
      ).toMatchObject({ outcome: "completed", result: { total: 21 } });
    }),
  );

  it.effect(
    "derives selected native tools and stable keys solely from the invocation-bound source",
    () =>
      Effect.gen(function* () {
        const background = Subagent.background(delegation, {
          start: true,
          followUp: true,
          inspect: true,
        });

        expect(Object.keys(background.tools)).toEqual([
          "research_start",
          "research_follow_up",
          "research_inspect",
        ]);
        for (const tool of Object.values(background.tools))
          expect(Context.get(tool.annotations, WorkerOperationTool)).toBe(true);
        expect(delegation.tool.name).toBe("research");
        const keys = yield* Ref.make<ReadonlyArray<IdempotencyKey>>([]);

        const service = host({
          context: Effect.succeed({
            ...caller,
            source: {
              ...caller.source,
              _tag: "tool",
              runId: Schema.decodeSync(RunId)("parent-run"),
              toolCallId: Schema.decodeSync(ToolCallId)("actual-call"),
            },
          }),
          start: (request) =>
            Ref.update(keys, (all) => [...all, request.idempotencyKey]).pipe(Effect.as(started)),
          followUp: (request) =>
            Ref.update(keys, (all) => [...all, request.idempotencyKey]).pipe(
              Effect.as(nextReceipt),
            ),
        });

        const toolkit = yield* background.toolkit.pipe(Effect.provide(background.layer));

        const invoke = (toolCallId: string) =>
          toolkit
            .handle("research_start", { amount: "7" }, toolCallId)
            .pipe(Effect.flatMap(Stream.runCollect), Effect.provideService(SubagentHost, service));

        const result = yield* invoke("untrusted-handler-id-1");

        yield* invoke("untrusted-handler-id-2");
        yield* toolkit
          .handle(
            "research_follow_up",
            { worker, parameters: { amount: "8" } },
            "untrusted-handler-id-3",
          )
          .pipe(Effect.flatMap(Stream.runCollect), Effect.provideService(SubagentHost, service));
        expect(result[0]?.result).toEqual(started);
        const recorded = yield* Ref.get(keys);

        expect(recorded[0]).toBe(recorded[1]);
        expect(recorded[0]).not.toBe(recorded[2]);
        expect(recorded[0]?.length).toBeLessThanOrEqual(256);

        const denied = yield* toolkit
          .handle("research_start", { amount: "7" }, "fake")
          .pipe(
            Effect.flatMap(Stream.runCollect),
            Effect.provideService(SubagentHost, host()),
            Effect.flip,
          );

        expect(denied).toMatchObject({ _tag: "WorkerError", reason: "denied" });
      }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.effect("keeps projector defects distinct and times out only the wait", () =>
    Effect.gen(function* () {
      const defective = Subagent.make("research", {
        ...delegation,
        projectResult: () => Effect.die("projector-defect"),
      });

      const defect = yield* Subagent.inspect(defective, worker, receipt).pipe(
        Effect.provideService(SubagentHost, host()),
        Effect.exit,
      );

      expect(Exit.isFailure(defect) && Cause.hasDies(defect.cause)).toBe(true);
      const entered = yield* Deferred.make<void>();
      const cancelled = yield* Ref.make(0);

      const waitingHost = host({
        await: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        cancel: () => Ref.update(cancelled, (count) => count + 1),
      });

      const fiber = yield* Subagent.await(delegation, worker, receipt).pipe(
        Effect.provideService(SubagentHost, waitingHost),
        Effect.timeout("1 second"),
        Effect.flip,
        Effect.forkChild,
      );

      yield* Deferred.await(entered);
      yield* TestClock.adjust("1 second");
      expect(yield* Fiber.join(fiber)).toMatchObject({ _tag: "TimeoutError" });
      expect(yield* Ref.get(cancelled)).toBe(0);
    }),
  );

  it.effect("narrows background authority and records reserved descendant slots", () =>
    Effect.gen(function* () {
      const declared = Subagent.make("research", {
        ...delegation,
        grant: SubagentGrant.make({
          allowedToolNames: ["read", "write"],
          maxDepth: 3,
          childLifetimes: ["attached", "background"],
        }),
        policy: Subagent.SubagentPolicy.make({
          maxChildren: 3,
          maxConcurrency: 3,
          maxTurns: 5,
          maxToolCalls: 8,
          maxDuration: "1 minute",
          descendantInvocations: 2,
        }),
      });

      const captured = yield* Ref.make<StartWorkerRequest | undefined>(undefined);

      const inherited = SubagentGrant.make({
        allowedToolNames: ["read"],
        maxDepth: 2,
        childLifetimes: ["background"],
      });

      const service = host({
        context: Effect.succeed({ ...caller, depth: 1, grant: inherited }),
        start: (request) => Ref.set(captured, request).pipe(Effect.as(started)),
      });

      yield* Subagent.start(declared, { amount: 7 }, { idempotencyKey: key }).pipe(
        Effect.provideService(SubagentHost, service),
      );
      expect((yield* Ref.get(captured))?.encodedGrant).toEqual({
        allowedToolNames: ["read"],
        maxDepth: 2,
        childLifetimes: ["background"],
      });
      expect((yield* Ref.get(captured))?.budget.descendantInvocations).toBe(2);

      const denied = yield* Subagent.start(declared, { amount: 7 }, { idempotencyKey: key }).pipe(
        Effect.provideService(
          SubagentHost,
          host({
            context: Effect.succeed({
              ...caller,
              grant: SubagentGrant.make({ ...inherited, childLifetimes: ["attached"] }),
            }),
          }),
        ),
        Effect.flip,
      );

      expect(denied).toMatchObject({ _tag: "SubagentPrestartDenied", reason: "grant-violation" });
    }),
  );

  it.effect("lets a root launch a background worker whose own children are attached-only", () =>
    Effect.gen(function* () {
      const declared = Subagent.make("research", {
        ...delegation,
        grant: SubagentGrant.make({
          allowedToolNames: [],
          maxDepth: 2,
          childLifetimes: ["attached"],
        }),
      });

      const captured = yield* Ref.make<StartWorkerRequest | undefined>(undefined);

      const result = yield* Subagent.start(declared, { amount: 7 }, { idempotencyKey: key }).pipe(
        Effect.provideService(
          SubagentHost,
          host({ start: (request) => Ref.set(captured, request).pipe(Effect.as(started)) }),
        ),
      );

      expect(result.receipt).toEqual(receipt);
      expect((yield* Ref.get(captured))?.encodedGrant).toEqual({
        allowedToolNames: [],
        maxDepth: 2,
        childLifetimes: ["attached"],
      });
    }),
  );

  it("marks only the selected launch tool as a background spawn", () => {
    const selected = Subagent.background(delegation, { start: true, inspect: true, cancel: true });
    const names = Object.keys(selected.tools);
    const grant = SubagentGrant.make({ allowedToolNames: names, maxDepth: 1 });

    expect(Context.get(selected.tools.research_start.annotations, BackgroundSpawnTool)).toBe(true);
    expect(Context.get(selected.tools.research_inspect.annotations, BackgroundSpawnTool)).toBe(
      false,
    );
    expect(
      names.filter((name) => {
        const tool = Object.values(selected.tools).find((value) => value.name === name);

        return tool !== undefined && isSubagentToolAllowed(grant, 1, name, tool.annotations);
      }),
    ).toEqual(["research_inspect", "research_cancel"]);
  });

  it.effect("does not capture an ambient host in the handler projection Layer", () =>
    Effect.gen(function* () {
      const background = Subagent.background(delegation, { inspect: true });
      const ambient = host({ inspect: () => Effect.succeed(settled) });

      const toolkit = yield* background.toolkit.pipe(
        Effect.provide(background.layer),
        Effect.provideService(SubagentHost, ambient),
      );

      const result = yield* toolkit
        .handle("research_inspect", { worker, receipt })
        .pipe(
          Effect.flatMap(Stream.runCollect),
          Effect.provideService(SubagentHost, SubagentHost.unavailable),
          Effect.flip,
        );

      expect(result).toMatchObject({
        _tag: "WorkerError",
        operation: "inspect",
        reason: "unavailable",
      });
    }),
  );
});
