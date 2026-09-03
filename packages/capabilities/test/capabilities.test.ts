import {
  ApprovalAudit,
  ApprovalAuditMemoryLive,
  ApprovalApproved,
  ApprovalDecisionMismatch,
  ApprovalDenied,
  ApprovalRequestDraft,
  ApprovalResolver,
  ApprovalResolverError,
  makeApprovalRequest,
  requestApproval,
} from "@effect-agent/capabilities/Approval";
import {
  BudgetExceeded,
  BudgetNodeConflict,
  makeUsageBudgetRoot,
  UsageBudgetLimits,
  UsageBudgetNodeConfig,
  UsageDelta,
} from "@effect-agent/capabilities/Budget";
import {
  FollowUpCommand,
  makeRunCommandQueue,
  RunCommandQueueConfig,
  SteeringCommand,
} from "@effect-agent/capabilities/Commands";
import {
  ThreadAppend,
  ThreadExport,
  ThreadHistoryDiverged,
  ThreadLimitExceeded,
  EphemeralThreads,
  EphemeralThreadsLive,
  threadPrompt,
} from "@effect-agent/capabilities/EphemeralThreads";
import {
  connectMcp,
  McpConnectionRequest,
  McpConnector,
  McpServerIdentity,
  validateMcpDiscovery,
} from "@effect-agent/capabilities/Mcp";
import {
  applyCompaction,
  CompactionArtifact,
  digestCompactionSource,
  ModelContextMessage,
  prepareModelContext,
  RetainedFact,
} from "@effect-agent/capabilities/ModelContext";
import { redactedTranscript, StructuralRedactorLive } from "@effect-agent/capabilities/Redaction";
import {
  toRunBudgetHook,
  toRunThreadOptions,
  toRunApprovalHook,
} from "@effect-agent/capabilities/RunHooks";
import { AgentId, ThreadId, RunId, ToolCallId, TurnId } from "@effect-agent/core/Identifiers";
import { RunStarted, TextDelta } from "@effect-agent/core/RunEvent";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Clock, DateTime, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { TestClock } from "effect/testing";
import { Prompt, Response, Tool, Toolkit } from "effect/unstable/ai";
import * as McpSchema from "effect/unstable/ai/McpSchema";

const threadId = Schema.decodeSync(ThreadId)("trip-1");
const runId = Schema.decodeSync(RunId)("run-1");
const toolCallId = Schema.decodeSync(ToolCallId)("hold-1");
const turnId = Schema.decodeSync(TurnId)("turn-1");

const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const textMessage = (role: "system" | "user" | "assistant", content: string): Prompt.Message => {
  const [message] = Prompt.make([{ role, content }]).content;

  if (message === undefined) {
    throw new Error("Expected Prompt.make to preserve its single input message");
  }

  return message;
};

const approvalDraft = (
  requestId: string,
  expiresAt: DateTime.Utc,
  resourceTargets: ReadonlyArray<string> = ["itinerary:trip-1"],
): ApprovalRequestDraft =>
  ApprovalRequestDraft.make({
    requestId,
    runId,
    threadId,
    toolCallId,
    toolName: "holdItinerary",
    actionSummary: "Place a 24-hour itinerary hold",
    resourceTargets,
    risk: "high",
    expiresAt,
    denial: "terminal",
  });

describe("capability contracts", () => {
  it.effect(
    "keeps one bounded ephemeral thread available to multiple Runs and exports a snapshot",
    () =>
      Effect.gen(function* () {
        const threads = yield* EphemeralThreads;

        yield* threads.create(threadId);
        yield* threads.append(
          threadId,
          ThreadAppend.make({
            runId,
            message: textMessage("user", "Find a hotel"),
          }),
        );

        const snapshot = yield* threads.append(
          threadId,
          ThreadAppend.make({
            message: textMessage("assistant", "Which dates?"),
          }),
        );

        const exported = yield* threads.export(threadId);

        expect(snapshot.messages.map((message) => message.sequence)).toEqual([0, 1]);
        expect(snapshot.messages[0]?.runId).toBe(runId);
        expect(snapshot.contentBytes).toBeGreaterThan(0);
        expect(exported.snapshot).toEqual(snapshot);
        expect(
          yield* Schema.decodeEffect(ThreadExport)(
            yield* Schema.encodeEffect(ThreadExport)(exported),
          ),
        ).toEqual(exported);
      }).pipe(Effect.provide(EphemeralThreadsLive)),
  );

  it.effect("round-trips structured Effect AI Prompt history through the engine adapter", () =>
    Effect.gen(function* () {
      const threads = yield* EphemeralThreads;

      yield* threads.create(threadId);

      const richAssistant = Prompt.assistantMessage({
        options: { provider: { trace: "native-option" } },
        content: [
          Prompt.reasoningPart({ text: "bounded reasoning summary" }),
          Prompt.toolCallPart({
            id: "call-1",
            name: "search",
            params: { query: "quiet hotel" },
            providerExecuted: false,
          }),
          Prompt.toolResultPart({
            id: "call-1",
            name: "search",
            isFailure: false,
            result: { hotel: "Harbor" },
            providerExecuted: false,
          }),
        ],
      });

      yield* threads.append(threadId, ThreadAppend.make({ runId, message: richAssistant }));

      const options = yield* toRunThreadOptions(threads, threadId, runId);

      expect(yield* Schema.encodeEffect(Prompt.Prompt)(options.history ?? Prompt.empty)).toEqual(
        yield* Schema.encodeEffect(Prompt.Prompt)(Prompt.fromMessages([richAssistant])),
      );

      const toolMessage = Prompt.toolMessage({
        content: [
          Prompt.toolResultPart({
            id: "call-2",
            name: "reserve",
            isFailure: false,
            result: { held: true },
            providerExecuted: false,
          }),
        ],
      });

      const extended = Prompt.fromMessages([richAssistant, toolMessage]);

      expect(options.onHistory).toBeDefined();
      if (options.onHistory !== undefined) yield* options.onHistory(extended);
      const snapshot = yield* threads.snapshot(threadId);

      expect(yield* Schema.encodeEffect(Prompt.Prompt)(threadPrompt(snapshot))).toEqual(
        yield* Schema.encodeEffect(Prompt.Prompt)(extended),
      );
    }).pipe(Effect.provide(EphemeralThreadsLive)),
  );

  it.effect("bounds the aggregate number of process-local threads", () =>
    Effect.gen(function* () {
      const threads = yield* EphemeralThreads;

      for (let index = 0; index < 256; index += 1) {
        yield* threads.create(yield* Schema.decodeEffect(ThreadId)(`bounded-${index}`));
      }
      const overflowId = yield* Schema.decodeEffect(ThreadId)("bounded-overflow");
      const exit = yield* threads.create(overflowId).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(EphemeralThreadsLive)),
  );

  it.effect("commits exactly one of two concurrently recorded diverging histories", () =>
    Effect.gen(function* () {
      const threads = yield* EphemeralThreads;

      yield* threads.create(threadId);

      const base = yield* threads.append(
        threadId,
        ThreadAppend.make({ message: textMessage("user", "shared base") }),
      );

      const extend = (content: string) =>
        Prompt.fromMessages([...threadPrompt(base).content, textMessage("assistant", content)]);

      const historyA = extend("suffix A");
      const historyB = extend("suffix B");

      const record = (label: "A" | "B", history: Prompt.Prompt) =>
        threads.recordHistory(threadId, runId, history).pipe(
          Effect.map(() => ({ _tag: "committed" as const, label })),
          Effect.catchTag("ThreadHistoryDiverged", () =>
            Effect.succeed({ _tag: "diverged" as const, label }),
          ),
        );

      const outcomes = yield* Effect.all([record("A", historyA), record("B", historyB)], {
        concurrency: 2,
      });

      const committed = outcomes.filter((outcome) => outcome._tag === "committed");
      const diverged = outcomes.filter((outcome) => outcome._tag === "diverged");

      expect(committed).toHaveLength(1);
      expect(diverged).toHaveLength(1);
      const snapshot = yield* threads.snapshot(threadId);
      const winnerHistory = committed[0]?.label === "A" ? historyA : historyB;

      expect(yield* Schema.encodeEffect(Prompt.Prompt)(threadPrompt(snapshot))).toEqual(
        yield* Schema.encodeEffect(Prompt.Prompt)(winnerHistory),
      );
    }).pipe(Effect.provide(EphemeralThreadsLive)),
  );

  it.effect("commits no suffix message when a recorded history exceeds content bounds", () =>
    Effect.gen(function* () {
      const threads = yield* EphemeralThreads;

      yield* threads.create(threadId);

      const base = yield* threads.append(
        threadId,
        ThreadAppend.make({ message: textMessage("user", "bounded base") }),
      );

      const oversized = Prompt.fromMessages([
        ...threadPrompt(base).content,
        textMessage("assistant", "a".repeat(3 * 1024 * 1024)),
        textMessage("assistant", "b".repeat(3 * 1024 * 1024)),
      ]);

      const error = yield* threads.recordHistory(threadId, runId, oversized).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ThreadLimitExceeded);
      expect(yield* threads.snapshot(threadId)).toEqual(base);
    }).pipe(Effect.provide(EphemeralThreadsLive)),
  );

  it.effect(
    "rejects an engine history that is not an append-only extension of official history",
    () =>
      Effect.gen(function* () {
        const threads = yield* EphemeralThreads;

        yield* threads.create(threadId);

        const official = yield* threads.append(
          threadId,
          ThreadAppend.make({ message: textMessage("user", "official input") }),
        );

        const rewritten = Prompt.fromMessages([
          textMessage("user", "rewritten input"),
          textMessage("assistant", "suffix built on a rewritten base"),
        ]);

        const rewrittenError = yield* threads
          .recordHistory(threadId, runId, rewritten)
          .pipe(Effect.flip);

        const truncatedError = yield* threads
          .recordHistory(threadId, runId, Prompt.empty)
          .pipe(Effect.flip);

        expect(rewrittenError).toBeInstanceOf(ThreadHistoryDiverged);
        expect(truncatedError).toBeInstanceOf(ThreadHistoryDiverged);
        expect(yield* threads.snapshot(threadId)).toEqual(official);
      }).pipe(Effect.provide(EphemeralThreadsLive)),
  );

  it.effect(
    "drains one or all safe-seam commands deterministically and closes with its Scope",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* makeRunCommandQueue(
            runId,
            RunCommandQueueConfig.make({ capacity: 2 }),
          );

          yield* queue.offer(
            SteeringCommand.make({
              id: "steer-1",
              runId,
              threadId,
              author: "traveler",
              content: "Move the trip one day later",
              createdAt: at(1),
            }),
          );
          yield* queue.offer(
            FollowUpCommand.make({
              id: "follow-1",
              runId,
              threadId,
              author: "traveler",
              content: "I prefer a quiet room",
              createdAt: at(2),
            }),
          );

          expect((yield* queue.drain()).map((command) => command.id)).toEqual(["steer-1"]);
          expect((yield* queue.drain("all")).map((command) => command.id)).toEqual(["follow-1"]);
          expect(yield* queue.drain("all")).toEqual([]);
        }),
      ),
  );

  it.effect("rejects command admission after its owning Run Scope closes", () =>
    Effect.gen(function* () {
      const queue = yield* Effect.scoped(
        makeRunCommandQueue(runId, RunCommandQueueConfig.make({ capacity: 1 })),
      );

      const exit = yield* queue
        .offer(
          SteeringCommand.make({
            id: "late-steer",
            runId,
            threadId,
            author: "traveler",
            content: "This run has ended",
            createdAt: at(3),
          }),
        )
        .pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );

  it.effect(
    "structurally redacts decoded approval input and audits both timeout request and decision",
    () =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;

        const request = yield* makeApprovalRequest(approvalDraft("approval-1", at(now - 1)), {
          hotel: "beach resort",
          password: "must-not-appear",
          nested: { apiKey: "also-secret" },
        });

        const decision = yield* requestApproval(request);
        const audit = yield* ApprovalAudit;
        const events = yield* audit.events;

        expect(request.redactedInputPreview).toContain("[REDACTED:string]");
        expect(request.redactedInputPreview).not.toContain("must-not-appear");
        expect(request.redactedInputPreview).not.toContain("also-secret");
        expect(request.redactedInputPreview).not.toContain("beach resort");
        expect(decision).toBeInstanceOf(ApprovalDenied);
        expect(events.map((event) => event._tag)).toEqual([
          "ApprovalRequestRecorded",
          "ApprovalDecisionRecorded",
        ]);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            StructuralRedactorLive,
            ApprovalAuditMemoryLive,
            Layer.succeed(ApprovalResolver)({
              request: () =>
                Effect.fail(ApprovalResolverError.make({ message: "must not be called" })),
            }),
          ),
        ),
      ),
  );

  it.effect("redactedTranscript composes encode + structural redaction for live Run Events", () =>
    Effect.gen(function* () {
      // P7 WP7 friction fix (travel-planner live-profile note): the safe path from live Run
      // Events to a loggable transcript is ONE composed step — no call site hand-assembles
      // the encode → redact pair or accidentally logs the raw event.
      const base = {
        eventVersion: 1 as const,
        runId,
        threadId,
        agentId: Schema.decodeSync(AgentId)("agent-redacted-transcript"),
        sequence: 0,
        timestamp: DateTime.toUtc(DateTime.makeUnsafe(1_000)),
      };

      const lines = yield* redactedTranscript([
        RunStarted.make(base),
        TextDelta.make({ ...base, sequence: 1, text: "secret itinerary sk-live-key" }),
      ]);

      expect(lines).toHaveLength(2);
      for (const line of lines) {
        expect(line).not.toContain("sk-live-key");
        expect(line).not.toContain("secret itinerary");
        expect(line).toContain("[REDACTED:string]");
      }
    }).pipe(Effect.provide(StructuralRedactorLive)),
  );

  it.effect("fails closed when an approval resolver returns another requestId", () =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;

      const request = yield* makeApprovalRequest(
        approvalDraft("approval-correlation", at(now + 1_000)),
        { note: "ordinary-key-secret" },
      );

      const error = yield* requestApproval(request).pipe(Effect.flip);
      const audit = yield* ApprovalAudit;
      const events = yield* audit.events;

      expect(error).toBeInstanceOf(ApprovalDecisionMismatch);
      expect(events).toHaveLength(2);
      const recorded = events[1];

      expect(recorded?._tag).toBe("ApprovalDecisionRecorded");
      if (recorded?._tag === "ApprovalDecisionRecorded") {
        expect(recorded.decision.requestId).toBe(request.requestId);
        expect(recorded.decision._tag).toBe("ApprovalDenied");
      }
      expect(request.redactedInputPreview).not.toContain("ordinary-key-secret");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          StructuralRedactorLive,
          ApprovalAuditMemoryLive,
          Layer.succeed(ApprovalResolver)({
            request: () =>
              Clock.currentTimeMillis.pipe(
                Effect.map((millis) =>
                  ApprovalApproved.make({
                    requestId: "wrong-request",
                    decidedAt: at(millis),
                    resolver: "bad-resolver",
                  }),
                ),
              ),
          }),
        ),
      ),
    ),
  );

  it.effect("audits a synthetic denial and releases the reservation when the resolver fails", () =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;

      const request = yield* makeApprovalRequest(
        approvalDraft("approval-resolver-error", at(now + 1_000)),
        { note: "infrastructure-secret" },
      );

      const error = yield* requestApproval(request).pipe(Effect.flip);
      const audit = yield* ApprovalAudit;
      const events = yield* audit.events;

      expect(error).toBeInstanceOf(ApprovalResolverError);
      expect(events.map((event) => event._tag)).toEqual([
        "ApprovalRequestRecorded",
        "ApprovalDecisionRecorded",
      ]);
      const recorded = events[1];

      expect(recorded?._tag).toBe("ApprovalDecisionRecorded");
      if (recorded?._tag === "ApprovalDecisionRecorded") {
        expect(recorded.decision._tag).toBe("ApprovalDenied");
        expect(recorded.decision.requestId).toBe(request.requestId);
      }
      // The reservation is released: the same request admits a fresh request/decision pair.
      yield* audit.recordRequest(request);
      yield* audit.recordDecision(
        ApprovalDenied.make({
          requestId: request.requestId,
          decidedAt: at(now),
          resolver: "test",
          reason: "explicit denial after recovery",
          timedOut: false,
        }),
      );
      expect(yield* audit.events).toHaveLength(4);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          StructuralRedactorLive,
          ApprovalAuditMemoryLive,
          Layer.succeed(ApprovalResolver)({
            request: () =>
              Effect.fail(ApprovalResolverError.make({ message: "resolver transport failed" })),
          }),
        ),
      ),
    ),
  );

  it.effect("reserves audit capacity atomically for request and decision pairs", () =>
    Effect.gen(function* () {
      const request = yield* makeApprovalRequest(approvalDraft("audit-pair", at(1)), {
        note: "secret",
      });

      const audit = yield* ApprovalAudit;

      const decision = ApprovalDenied.make({
        requestId: request.requestId,
        decidedAt: at(1),
        resolver: "test",
        reason: "test",
        timedOut: false,
      });

      for (let index = 0; index < 1_024; index += 1) {
        yield* audit.recordRequest(request);
        yield* audit.recordDecision(decision);
      }
      const exit = yield* audit.recordRequest(request).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(yield* audit.events).toHaveLength(2_048);
    }).pipe(Effect.provide(Layer.merge(StructuralRedactorLive, ApprovalAuditMemoryLive))),
  );

  it.effect(
    "interrupts an unresolved resolver at the approval deadline and audits the denial",
    () =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;

        const request = yield* makeApprovalRequest(approvalDraft("approval-2", at(now + 1_000)), {
          hotel: "beach resort",
        });

        const fiber = yield* requestApproval(request).pipe(Effect.forkChild);

        yield* TestClock.adjust("1 second");
        const decision = yield* Fiber.join(fiber);
        const audit = yield* ApprovalAudit;

        expect(decision._tag).toBe("ApprovalDenied");
        if (decision._tag === "ApprovalDenied") {
          expect(decision.timedOut).toBe(true);
        }
        expect(yield* audit.events).toHaveLength(2);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            StructuralRedactorLive,
            ApprovalAuditMemoryLive,
            Layer.succeed(ApprovalResolver)({ request: () => Effect.never }),
          ),
        ),
      ),
  );

  it("rejects approval target collections above the public count bound", () => {
    expect(() =>
      approvalDraft(
        "too-many-targets",
        at(1),
        Array.from({ length: 33 }, (_, index) => `target:${index}`),
      ),
    ).toThrow();
  });

  it("rejects approval targets above the aggregate UTF-8 byte bound", () => {
    expect(() =>
      approvalDraft(
        "targets-too-large",
        at(1),
        Array.from({ length: 32 }, () => "💥".repeat(1_000)),
      ),
    ).toThrow();
  });

  it.effect("applies the approval adapter policy Schema before invoking policy callbacks", () => {
    let callbackInvoked = false;

    const hook = toRunApprovalHook({
      expiresInMillis: -1,
      risk: "high",
      denial: "terminal",
      actionSummary: () => {
        callbackInvoked = true;

        return "must not run";
      },
      resourceTargets: () => [],
    });

    return hook
      .request({
        request: Response.toolApprovalRequestPart({
          approvalId: "approval-invalid-policy",
          toolCallId,
        }),
        threadId,
        runId,
        turnId,
        toolCallId,
        toolName: "holdItinerary",
        parameters: { note: "secret" },
      })
      .pipe(
        Effect.exit,
        Effect.tap((exit) =>
          Effect.sync(() => {
            expect(Exit.isFailure(exit)).toBe(true);
            expect(callbackInvoked).toBe(false);
          }),
        ),
        Effect.provide(
          Layer.mergeAll(
            StructuralRedactorLive,
            ApprovalAuditMemoryLive,
            Layer.succeed(ApprovalResolver)({
              request: () => Effect.never,
            }),
          ),
        ),
      );
  });

  it.effect("decodes a valid approval adapter deadline before invoking the resolver", () => {
    const hook = toRunApprovalHook({
      expiresInMillis: 1_000,
      risk: "high",
      denial: "terminal",
      actionSummary: () => "Place a temporary itinerary hold",
      resourceTargets: () => ["quote:quote-sfo-lhr-001"],
    });

    return Effect.gen(function* () {
      const decision = yield* hook.request({
        request: Response.toolApprovalRequestPart({
          approvalId: "approval-valid-policy",
          toolCallId,
        }),
        threadId,
        runId,
        turnId,
        toolCallId,
        toolName: "holdItinerary",
        parameters: { quoteId: "quote-sfo-lhr-001" },
      });

      expect(decision._tag).toBe("approved");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          StructuralRedactorLive,
          ApprovalAuditMemoryLive,
          Layer.succeed(ApprovalResolver)({
            request: (request) =>
              Clock.currentTimeMillis.pipe(
                Effect.map((millis) =>
                  ApprovalApproved.make({
                    requestId: request.requestId,
                    decidedAt: at(millis),
                    resolver: "test-resolver",
                  }),
                ),
              ),
          }),
        ),
      ),
    );
  });

  it.effect("atomically rejects hierarchical consumption across every ancestor", () =>
    Effect.gen(function* () {
      const globalBudget = yield* makeUsageBudgetRoot(
        UsageBudgetNodeConfig.make({
          level: "global",
          id: "all",
          limits: UsageBudgetLimits.make({ maxInputTokens: 4 }),
        }),
      );

      const tenantBudget = yield* globalBudget.child(
        UsageBudgetNodeConfig.make({
          level: "tenant",
          id: "tenant-a",
          limits: UsageBudgetLimits.make({ maxInputTokens: 10 }),
        }),
      );

      yield* tenantBudget.consume(
        UsageDelta.make({
          modelCalls: 1,
          inputTokens: 3,
          outputTokens: 0,
          cacheReadInputTokens: 0,
          cacheWriteInputTokens: 0,
          toolCalls: 0,
          costMicrousd: 0,
        }),
      );

      const exit = yield* tenantBudget
        .consume(
          UsageDelta.make({
            modelCalls: 1,
            inputTokens: 2,
            outputTokens: 0,
            cacheReadInputTokens: 0,
            cacheWriteInputTokens: 0,
            toolCalls: 0,
            costMicrousd: 0,
          }),
        )
        .pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      expect((yield* globalBudget.snapshot).inputTokens).toBe(3);
      expect((yield* tenantBudget.snapshot).inputTokens).toBe(3);
    }),
  );

  it.effect("re-attaches an identical child registration and rejects conflicting limits", () =>
    Effect.gen(function* () {
      const globalBudget = yield* makeUsageBudgetRoot(
        UsageBudgetNodeConfig.make({
          level: "global",
          id: "all",
          limits: UsageBudgetLimits.make({}),
        }),
      );

      const first = yield* globalBudget.child(
        UsageBudgetNodeConfig.make({
          level: "run",
          id: "run-1",
          limits: UsageBudgetLimits.make({ maxToolCalls: 1 }),
        }),
      );

      yield* first.consume(
        UsageDelta.make({
          modelCalls: 0,
          inputTokens: 0,
          outputTokens: 0,
          cacheReadInputTokens: 0,
          cacheWriteInputTokens: 0,
          toolCalls: 1,
          costMicrousd: 0,
        }),
      );

      const reattached = yield* globalBudget.child(
        UsageBudgetNodeConfig.make({
          level: "run",
          id: "run-1",
          limits: UsageBudgetLimits.make({ maxToolCalls: 1 }),
        }),
      );

      expect((yield* reattached.snapshot).toolCalls).toBe(1);

      const exceeded = yield* reattached
        .consume(
          UsageDelta.make({
            modelCalls: 0,
            inputTokens: 0,
            outputTokens: 0,
            cacheReadInputTokens: 0,
            cacheWriteInputTokens: 0,
            toolCalls: 1,
            costMicrousd: 0,
          }),
        )
        .pipe(Effect.flip);

      const conflict = yield* globalBudget
        .child(
          UsageBudgetNodeConfig.make({
            level: "run",
            id: "run-1",
            limits: UsageBudgetLimits.make({ maxToolCalls: 5 }),
          }),
        )
        .pipe(Effect.flip);

      expect(exceeded).toBeInstanceOf(BudgetExceeded);
      expect(conflict).toBeInstanceOf(BudgetNodeConflict);
      if (conflict instanceof BudgetNodeConflict) {
        const decoded = yield* Schema.decodeEffect(BudgetNodeConflict)(
          yield* Schema.encodeEffect(BudgetNodeConflict)(conflict),
        );

        expect(decoded).toBeInstanceOf(BudgetNodeConflict);
        expect(decoded.scopeLevel).toBe("run");
        expect(decoded.scopeId).toBe("run-1");
      }
    }),
  );

  it.effect(
    "retires scoped budget nodes on success, failure, and interruption without refunding ancestors",
    () =>
      Effect.gen(function* () {
        const globalBudget = yield* makeUsageBudgetRoot(
          UsageBudgetNodeConfig.make({
            level: "global",
            id: "lifecycle-root",
            limits: UsageBudgetLimits.make({}),
          }),
        );

        const config = (id: string) =>
          UsageBudgetNodeConfig.make({
            level: "run",
            id,
            limits: UsageBudgetLimits.make({}),
          });

        const delta = UsageDelta.make({
          modelCalls: 1,
          inputTokens: 1,
          outputTokens: 0,
          cacheReadInputTokens: 0,
          cacheWriteInputTokens: 0,
          toolCalls: 0,
          costMicrousd: 0,
        });

        yield* Effect.scoped(
          globalBudget
            .childScoped(config("success"))
            .pipe(Effect.flatMap((node) => node.consume(delta))),
        );
        yield* Effect.scoped(
          globalBudget.childScoped(config("failure")).pipe(
            Effect.flatMap((node) => node.consume(delta)),
            Effect.andThen(Effect.fail("expected failure")),
          ),
        ).pipe(Effect.ignore);

        const started = yield* Deferred.make<void>();

        const interrupted = yield* Effect.scoped(
          globalBudget.childScoped(config("interruption")).pipe(
            Effect.flatMap((node) => node.consume(delta)),
            Effect.andThen(Deferred.succeed(started, undefined)),
            Effect.andThen(Effect.never),
          ),
        ).pipe(Effect.forkChild);

        yield* Deferred.await(started);
        yield* Fiber.interrupt(interrupted);

        expect((yield* globalBudget.snapshot).inputTokens).toBe(3);
        for (const id of ["success", "failure", "interruption"]) {
          const fresh = yield* globalBudget.child(config(id));

          expect((yield* fresh.snapshot).inputTokens).toBe(0);
          yield* fresh.retire;
        }
      }),
  );

  it.effect("linearizes child registration and consumption with handle retirement", () =>
    Effect.gen(function* () {
      const delta = UsageDelta.make({
        modelCalls: 0,
        inputTokens: 0,
        outputTokens: 0,
        cacheReadInputTokens: 0,
        cacheWriteInputTokens: 0,
        toolCalls: 1,
        costMicrousd: 0,
      });

      for (let iteration = 0; iteration < 128; iteration += 1) {
        const root = yield* makeUsageBudgetRoot(
          UsageBudgetNodeConfig.make({
            level: "global",
            id: `retirement-root-${iteration}`,
            limits: UsageBudgetLimits.make({}),
          }),
        );

        const parent = yield* root.child(
          UsageBudgetNodeConfig.make({
            level: "tenant",
            id: "tenant-a",
            limits: UsageBudgetLimits.make({ maxToolCalls: 0 }),
          }),
        );

        const [childExit] = yield* Effect.all(
          [
            parent
              .child(
                UsageBudgetNodeConfig.make({
                  level: "run",
                  id: "run-a",
                  limits: UsageBudgetLimits.make({}),
                }),
              )
              .pipe(Effect.exit),
            parent.retire,
          ],
          { concurrency: "unbounded" },
        );

        if (Exit.isSuccess(childExit)) {
          const consumeExit = yield* childExit.value.consume(delta).pipe(Effect.exit);

          expect(Exit.isFailure(consumeExit)).toBe(true);
          yield* childExit.value.retire;
        }

        const secondParent = yield* root.child(
          UsageBudgetNodeConfig.make({
            level: "tenant",
            id: "tenant-b",
            limits: UsageBudgetLimits.make({ maxToolCalls: 0 }),
          }),
        );

        const [consumeExit] = yield* Effect.all(
          [secondParent.consume(delta).pipe(Effect.exit), secondParent.retire],
          { concurrency: "unbounded" },
        );

        expect(Exit.isFailure(consumeExit)).toBe(true);
        yield* root.retire;
      }
    }),
  );

  it.effect("fails stalled guarded work at the earliest hierarchical deadline", () =>
    Effect.gen(function* () {
      const globalBudget = yield* makeUsageBudgetRoot(
        UsageBudgetNodeConfig.make({
          level: "global",
          id: "all",
          limits: UsageBudgetLimits.make({ maxDurationMillis: 1_000 }),
        }),
      );

      const runBudget = yield* globalBudget.child(
        UsageBudgetNodeConfig.make({
          level: "run",
          id: "run-1",
          limits: UsageBudgetLimits.make({ maxDurationMillis: 5_000 }),
        }),
      );

      const fiber = yield* runBudget.guard(Effect.never).pipe(Effect.forkChild);

      yield* TestClock.adjust("1 second");
      const exit = yield* Fiber.await(fiber);

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );

  it.effect("wires the hierarchical guard through the engine budget adapter", () =>
    Effect.gen(function* () {
      const budget = yield* makeUsageBudgetRoot(
        UsageBudgetNodeConfig.make({
          level: "run",
          id: "adapter-run",
          limits: UsageBudgetLimits.make({ maxDurationMillis: 1_000 }),
        }),
      );

      const hook = toRunBudgetHook(budget);

      const consumed: void = yield* hook.consume({
        modelCalls: 1,
        inputTokens: 3,
        outputTokens: 2,
        totalTokens: 5,
        toolCalls: 1,
        costMicrousd: 7,
        usage: Response.Usage.make({
          inputTokens: { total: 3 },
          outputTokens: { total: 2 },
        }),
      });

      expect(consumed).toBeUndefined();
      expect(yield* budget.snapshot).toMatchObject({
        inputTokens: 3,
        outputTokens: 2,
        toolCalls: 1,
        costMicrousd: 7,
      });
      const fiber = yield* hook.guard(Effect.never).pipe(Effect.forkChild);

      yield* TestClock.adjust("1 second");
      expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
    }),
  );

  it.effect("CAP-017: accumulates cache splits and exposes last-call usage in snapshots", () =>
    Effect.gen(function* () {
      const budget = yield* makeUsageBudgetRoot(
        UsageBudgetNodeConfig.make({
          level: "run",
          id: "run-cache",
          limits: UsageBudgetLimits.make({}),
        }),
      );

      yield* budget.consume(
        UsageDelta.make({
          modelCalls: 1,
          inputTokens: 100,
          outputTokens: 10,
          toolCalls: 0,
          costMicrousd: 0,
          cacheReadInputTokens: 60,
          cacheWriteInputTokens: 30,
        }),
      );
      yield* budget.consume(
        UsageDelta.make({
          modelCalls: 1,
          inputTokens: 150,
          outputTokens: 5,
          toolCalls: 1,
          costMicrousd: 0,
          cacheReadInputTokens: 120,
          cacheWriteInputTokens: 0,
        }),
      );
      yield* budget.consume(
        UsageDelta.make({
          modelCalls: 0,
          inputTokens: 0,
          outputTokens: 0,
          toolCalls: 1,
          costMicrousd: 0,
          cacheReadInputTokens: 0,
          cacheWriteInputTokens: 0,
        }),
      );

      expect(yield* budget.snapshot).toMatchObject({
        inputTokens: 250,
        outputTokens: 15,
        cacheReadInputTokens: 180,
        cacheWriteInputTokens: 30,
        lastInputTokens: 150,
        lastOutputTokens: 5,
        toolCalls: 2,
      });
      yield* TestClock.adjust("1 second");
      expect(yield* budget.snapshot).toMatchObject({
        cacheReadInputTokens: 180,
        cacheWriteInputTokens: 30,
        lastInputTokens: 150,
        lastOutputTokens: 5,
        elapsedMillis: 1_000,
      });
    }),
  );

  it.effect("CAP-017: propagates cache splits through the hierarchy and the engine adapter", () =>
    Effect.gen(function* () {
      const globalBudget = yield* makeUsageBudgetRoot(
        UsageBudgetNodeConfig.make({
          level: "global",
          id: "all",
          limits: UsageBudgetLimits.make({}),
        }),
      );

      const runBudget = yield* globalBudget.child(
        UsageBudgetNodeConfig.make({
          level: "run",
          id: "run-1",
          limits: UsageBudgetLimits.make({}),
        }),
      );

      const hook = toRunBudgetHook(runBudget);

      yield* hook.consume({
        modelCalls: 1,
        inputTokens: 9,
        outputTokens: 3,
        totalTokens: 12,
        toolCalls: 0,
        costMicrousd: 0,
        usage: Response.Usage.make({
          inputTokens: { total: 9, cacheRead: 4, cacheWrite: 2 },
          outputTokens: { total: 3 },
        }),
      });
      yield* hook.consume({
        modelCalls: 0,
        inputTokens: 0,
        outputTokens: 0,
        totalTokens: 0,
        toolCalls: 1,
        costMicrousd: 0,
        usage: Response.Usage.make({ inputTokens: {}, outputTokens: {} }),
      });

      for (const node of [globalBudget, runBudget]) {
        expect(yield* node.snapshot).toMatchObject({
          inputTokens: 9,
          cacheReadInputTokens: 4,
          cacheWriteInputTokens: 2,
          lastInputTokens: 9,
          lastOutputTokens: 3,
        });
      }
    }),
  );

  it.effect(
    "verifies exact compaction digests, preserves a nonzero uncovered prefix, and retains source",
    () =>
      Effect.gen(function* () {
        const threads = yield* EphemeralThreads;

        yield* threads.create(threadId);
        for (const content of ["Original first", "Compact this", "Original last"]) {
          yield* threads.append(
            threadId,
            ThreadAppend.make({ message: textMessage("user", content) }),
          );
        }
        const snapshot = yield* threads.snapshot(threadId);

        const context = yield* prepareModelContext(snapshot, [
          {
            id: "model-view-only",
            version: "1",
            apply: (messages) => Effect.succeed(messages),
          },
        ]);

        const sourceDigest = yield* digestCompactionSource(snapshot, 1, 1);

        const artifact = CompactionArtifact.make({
          version: 1,
          threadId,
          coversFrom: 1,
          coversThrough: 1,
          summary: ModelContextMessage.make({
            role: "system",
            content: "Middle message summary.",
            sourceSequences: [1],
          }),
          retainedFacts: [RetainedFact.make({ fact: "Middle retained", sourceSequences: [1] })],
          tokenEstimate: 4,
          sourceDigest,
          compactorVersion: "test-1",
        });

        const compacted = yield* applyCompaction(context, artifact);

        expect(compacted.source).toEqual(snapshot);
        expect(compacted.source.messages).toHaveLength(3);
        expect(compacted.messages.map((message) => message.content)).toEqual([
          "Original first",
          "Middle message summary.",
          "Original last",
        ]);
      }).pipe(Effect.provide(Layer.mergeAll(EphemeralThreadsLive, NodeCrypto.layer))),
  );

  it.effect("keeps transform-synthesized and partially covered model-view messages visible", () =>
    Effect.gen(function* () {
      const threads = yield* EphemeralThreads;

      yield* threads.create(threadId);
      for (const content of ["first", "second", "third"]) {
        yield* threads.append(
          threadId,
          ThreadAppend.make({ message: textMessage("user", content) }),
        );
      }
      const snapshot = yield* threads.snapshot(threadId);

      const context = yield* prepareModelContext(snapshot, [
        {
          id: "merge-and-synthesize",
          version: "1",
          apply: (messages) =>
            Effect.succeed([
              ModelContextMessage.make({
                role: "system",
                content: "synthesized guidance",
                sourceSequences: [],
              }),
              ModelContextMessage.make({
                role: "user",
                content: "merged first+second",
                sourceSequences: [0, 1],
              }),
              ...messages.filter((message) => message.sourceSequences.includes(2)),
            ]),
        },
      ]);

      const sourceDigest = yield* digestCompactionSource(snapshot, 1, 1);

      const artifact = CompactionArtifact.make({
        version: 1,
        threadId,
        coversFrom: 1,
        coversThrough: 1,
        summary: ModelContextMessage.make({
          role: "system",
          content: "second summarized",
          sourceSequences: [1],
        }),
        retainedFacts: [],
        tokenEstimate: 2,
        sourceDigest,
        compactorVersion: "test-1",
      });

      const compacted = yield* applyCompaction(context, artifact);

      expect(compacted.messages.map((message) => message.content)).toEqual([
        "synthesized guidance",
        "merged first+second",
        "second summarized",
        "third",
      ]);
    }).pipe(Effect.provide(Layer.mergeAll(EphemeralThreadsLive, NodeCrypto.layer))),
  );

  it.effect("rejects a compaction artifact whose exact source digest mismatches", () =>
    Effect.gen(function* () {
      const threads = yield* EphemeralThreads;

      yield* threads.create(threadId);

      const snapshot = yield* threads.append(
        threadId,
        ThreadAppend.make({ message: textMessage("user", "Canonical source") }),
      );

      const context = yield* prepareModelContext(snapshot);

      const artifact = CompactionArtifact.make({
        version: 1,
        threadId,
        coversFrom: 0,
        coversThrough: 0,
        summary: ModelContextMessage.make({
          role: "system",
          content: "Untrusted summary",
          sourceSequences: [0],
        }),
        retainedFacts: [],
        tokenEstimate: 2,
        sourceDigest: `sha256:${"0".repeat(64)}`,
        compactorVersion: "test-1",
      });

      const exit = yield* applyCompaction(context, artifact).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(Layer.mergeAll(EphemeralThreadsLive, NodeCrypto.layer))),
  );

  it.effect("rejects compaction provenance outside the exact covered source range", () =>
    Effect.gen(function* () {
      const threads = yield* EphemeralThreads;

      yield* threads.create(threadId);
      yield* threads.append(threadId, ThreadAppend.make({ message: textMessage("user", "first") }));

      const snapshot = yield* threads.append(
        threadId,
        ThreadAppend.make({ message: textMessage("user", "second") }),
      );

      const sourceDigest = yield* digestCompactionSource(snapshot, 1, 1);

      const artifact = CompactionArtifact.make({
        version: 1,
        threadId,
        coversFrom: 1,
        coversThrough: 1,
        summary: ModelContextMessage.make({
          role: "system",
          content: "invalid provenance",
          sourceSequences: [0],
        }),
        retainedFacts: [],
        tokenEstimate: 2,
        sourceDigest,
        compactorVersion: "test-1",
      });

      const exit = yield* applyCompaction(yield* prepareModelContext(snapshot), artifact).pipe(
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(Layer.merge(EphemeralThreadsLive, NodeCrypto.layer))),
  );

  it.effect(
    "retains authoritative source even when a transform replaces the entire model view",
    () =>
      Effect.gen(function* () {
        const threads = yield* EphemeralThreads;

        yield* threads.create(threadId);

        const snapshot = yield* threads.append(
          threadId,
          ThreadAppend.make({ message: textMessage("user", "Official history") }),
        );

        const context = yield* prepareModelContext(snapshot, [
          {
            id: "empty-view",
            version: "1",
            apply: () => Effect.succeed([]),
          },
        ]);

        expect(context.messages).toEqual([]);
        expect(context.source).toEqual(snapshot);
        expect(context.source.messages[0]?.message).toEqual(
          textMessage("user", "Official history"),
        );
      }).pipe(Effect.provide(EphemeralThreadsLive)),
  );

  it.effect("enforces native MCP discovery count and byte contracts", () =>
    Effect.gen(function* () {
      const request = McpConnectionRequest.make({
        serverId: "travel-mcp",
        maxToolCount: 1,
        maxToolDescriptionBytes: 64,
        maxDiscoveryBytes: 4_096,
        connectTimeoutMillis: 1_000,
      });

      const identity = McpServerIdentity.make({
        serverId: "travel-mcp",
        implementation: McpSchema.Implementation.make({
          name: "travel-tools",
          version: "2.4.0",
        }),
      });

      const tools = ["one", "two"].map((name) =>
        McpSchema.Tool.make({
          name,
          description: "bounded",
          inputSchema: { type: "object" },
        }),
      );

      const exit = yield* validateMcpDiscovery(request, {
        identity,
        capabilities: McpSchema.ServerCapabilities.make({}),
        tools,
        toolkit: Toolkit.empty,
      }).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.effect("binds MCP server identity and resolves escaped named Toolkit schemas", () =>
    Effect.gen(function* () {
      const Search = Tool.make("search", {
        parameters: Schema.Struct({ query: Schema.String }).annotate({
          identifier: "Search/Query~v1",
        }),
        success: Schema.Struct({ answer: Schema.String }).annotate({
          identifier: "Search/Answer~v1",
        }),
      });

      const toolkit = Toolkit.make(Search);

      const request = McpConnectionRequest.make({
        serverId: "travel-mcp",
        maxToolCount: 1,
        maxToolDescriptionBytes: 64,
        maxDiscoveryBytes: 8_192,
        connectTimeoutMillis: 1_000,
      });

      const identity = McpServerIdentity.make({
        serverId: "travel-mcp",
        implementation: McpSchema.Implementation.make({
          name: "travel-tools",
          version: "2.4.0",
        }),
      });

      const matching = McpSchema.Tool.make({
        name: "search",
        description: "bounded",
        inputSchema: {
          type: "object",
          properties: { query: { type: "string" } },
          required: ["query"],
          additionalProperties: false,
        },
        outputSchema: {
          type: "object",
          properties: { answer: { type: "string" } },
          required: ["answer"],
          additionalProperties: false,
        },
      });

      const discovery = yield* validateMcpDiscovery(request, {
        identity,
        capabilities: McpSchema.ServerCapabilities.make({}),
        tools: [matching],
        toolkit,
      });

      expect(discovery.toolkitSchemaDigest).toMatch(/^sha256:[a-f0-9]{64}$/);

      const wrongSchemaExit = yield* validateMcpDiscovery(request, {
        identity,
        capabilities: McpSchema.ServerCapabilities.make({}),
        tools: [
          McpSchema.Tool.make({
            name: matching.name,
            description: matching.description,
            inputSchema: { type: "object", properties: { count: { type: "number" } } },
          }),
        ],
        toolkit,
      }).pipe(Effect.exit);

      expect(Exit.isFailure(wrongSchemaExit)).toBe(true);

      const wrongOutputSchemaExit = yield* validateMcpDiscovery(request, {
        identity,
        capabilities: McpSchema.ServerCapabilities.make({}),
        tools: [
          McpSchema.Tool.make({
            name: matching.name,
            description: matching.description,
            inputSchema: matching.inputSchema,
            outputSchema: {
              type: "object",
              properties: { answer: { type: "number" } },
            },
          }),
        ],
        toolkit,
      }).pipe(Effect.exit);

      expect(Exit.isFailure(wrongOutputSchemaExit)).toBe(true);

      const nonJsonSchemaError = yield* validateMcpDiscovery(request, {
        identity,
        capabilities: McpSchema.ServerCapabilities.make({}),
        tools: [
          // Deliberately non-canonical JSON (an `undefined` value): `Tool.make` now
          // validates `inputSchema` against the MCP JSON-Schema shape and would reject
          // it at construction, so this bypasses construction to reach digestJson's own
          // canonical-JSON check instead.
          {
            name: matching.name,
            description: matching.description,
            inputSchema: { type: "object", unsupported: undefined },
          } as unknown as McpSchema.Tool,
        ],
        toolkit,
      }).pipe(Effect.flip);

      expect(nonJsonSchemaError).toMatchObject({
        _tag: "McpToolkitMismatch",
        message: expect.stringContaining("not canonical JSON"),
        cause: expect.anything(),
      });

      const wrongIdentityExit = yield* validateMcpDiscovery(request, {
        identity: McpServerIdentity.make({
          serverId: "other-mcp",
          implementation: identity.implementation,
        }),
        capabilities: McpSchema.ServerCapabilities.make({}),
        tools: [matching],
        toolkit,
      }).pipe(Effect.exit);

      expect(Exit.isFailure(wrongIdentityExit)).toBe(true);
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.effect("fails a stalled MCP connection under the Effect test Clock", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const request = McpConnectionRequest.make({
          serverId: "stalled-mcp",
          maxToolCount: 1,
          maxToolDescriptionBytes: 64,
          maxDiscoveryBytes: 4_096,
          connectTimeoutMillis: 1_000,
        });

        const finalized = yield* Deferred.make<void>();

        const connector = Layer.succeed(McpConnector)({
          connect: () =>
            Effect.acquireUseRelease(
              Effect.void,
              () => Effect.never,
              () => Deferred.succeed(finalized, undefined),
            ),
        });

        const fiber = yield* connectMcp(request).pipe(
          Effect.provide(Layer.merge(connector, NodeCrypto.layer)),
          Effect.forkChild,
        );

        yield* TestClock.adjust("1 second");
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        expect(yield* Deferred.isDone(finalized)).toBe(true);
      }),
    ),
  );
});
