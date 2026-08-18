import { Duration, Effect, Encoding, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  type BudgetWarning,
  type CompactionPerformed,
  type SubagentCompleted,
  type SubagentFailed,
  type SubagentInterrupted,
  type SubagentJoined,
  type SubagentProgress,
  type SubagentRequested,
  type SubagentStarted,
  type ToolCallFailed,
  AgentId,
  AgentApprovalDenied,
  AgentToolAuthorizationDenied,
  AgentApprovalPending,
  AgentError,
  AgentInputError,
  AgentInterrupted,
  AgentOutputError,
  AgentRunDispositionError,
  AgentPolicy,
  AgentPolicyError,
  applyToolResultBounds,
  unserializableToolResult,
  CompactionPolicy,
  ContextOverflowError,
  ConversationId,
  DelegationId,
  IdGenerator,
  ModelProtocolError,
  ReceiptId,
  RunCompleted,
  RunEvent,
  RunId,
  RunStarted,
  SettlementId,
  SubagentParentLink,
  ToolCallDeclared,
  ToolCallId,
  ToolResultBounds,
  TruncatedToolResult,
  TurnId,
} from "../src/index.ts";

// Core is platform-neutral: no TextEncoder in its lib; hex length halves to UTF-8 bytes.
const utf8Bytes = (value: string): number => Encoding.encodeHex(value).length / 2;

describe("core schemas", () => {
  it("decodes distinct non-empty branded identifiers", () => {
    expect(Schema.decodeSync(AgentId)("travel-planner")).toBe("travel-planner");
    expect(Schema.decodeSync(DelegationId)("delegate-research")).toBe("delegate-research");
    expect(() => Schema.decodeSync(RunId)("")).toThrow();
    expect(() => Schema.decodeSync(DelegationId)("")).toThrow();
  });

  it("round-trips the durable Receipt and Settlement identities and rejects empty values", () => {
    const receiptId = Schema.decodeSync(ReceiptId)("receipt-1");
    const settlementId = Schema.decodeSync(SettlementId)("settlement:submission-1");

    expect(Schema.encodeSync(ReceiptId)(receiptId)).toBe("receipt-1");
    expect(Schema.encodeSync(SettlementId)(settlementId)).toBe("settlement:submission-1");
    expect(Schema.decodeUnknownExit(ReceiptId)("")._tag).toBe("Failure");
    expect(Schema.decodeUnknownExit(SettlementId)("")._tag).toBe("Failure");
    expect(Schema.decodeUnknownExit(ReceiptId)(7)._tag).toBe("Failure");
    expect(Schema.decodeUnknownExit(SettlementId)(null)._tag).toBe("Failure");
  });

  it("constructs finite policies and rejects invalid bounds", () => {
    const policy = AgentPolicy.make({
      maxTurns: 2,
      maxToolCalls: 1,
      maxDuration: "30 seconds",
      toolConcurrency: 1,
      tokenBudget: 1_000,
      costBudgetMicrousd: 10_000,
    });

    expect(policy.maxDuration).toEqual(Duration.seconds(30));
    expect(policy.repeatedFailureLimit).toBe(3);
    expect(policy.tokenBudget).toBe(1_000);
    expect(policy.costBudgetMicrousd).toBe(10_000);
    expect(() =>
      AgentPolicy.make({
        maxTurns: 0,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    ).toThrow();
    expect(() =>
      AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: Duration.infinity,
        toolConcurrency: 1,
      }),
    ).toThrow();
    expect(() =>
      AgentPolicy.make({
        maxTurns: 2.5,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    ).toThrow();
    expect(() =>
      AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
        repeatedFailureLimit: -1,
      }),
    ).toThrow();
  });

  it("round-trips every framework-owned expected failure through the public union", () => {
    const failures = [
      AgentInputError.make({ message: "invalid trip input" }),
      AgentOutputError.make({ message: "invalid itinerary output" }),
      AgentRunDispositionError.make({
        cause: new Error("invalid application run disposition"),
        message: "invalid application run disposition",
      }),
      AgentPolicyError.make({ limit: "turns", message: "turn limit reached" }),
      AgentPolicyError.make({
        limit: "repeated-failures",
        message: "3 consecutive Tool Call failures",
      }),
      AgentApprovalDenied.make({
        toolCallId: "hold-1",
        toolName: "hold",
        message: "denied",
      }),
      AgentToolAuthorizationDenied.make({
        toolCallId: Schema.decodeSync(ToolCallId)("hold-1"),
        toolName: "hold",
        message: "current execution authority was denied",
      }),
      AgentApprovalPending.make({
        approvalId: "approval-1",
        toolCallId: "hold-1",
        toolName: "hold",
        message: "pending",
      }),
      ModelProtocolError.make({ message: "response completed twice" }),
      AgentInterrupted.make({ message: "consumer disconnected" }),
      ContextOverflowError.make({ message: "prompt is too long", retried: true }),
    ] as const;

    for (const failure of failures) {
      const encoded = Schema.encodeSync(AgentError)(failure);
      expect(Schema.decodeSync(AgentError)(encoded)).toEqual(failure);
    }
  });

  it("decodes semantic run events through the public union", () => {
    const encodedEvent = {
      _tag: "RunStarted",
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 0,
      timestamp: "2026-07-29T12:00:00.000Z",
    } satisfies typeof RunStarted.Encoded;
    const event = Schema.decodeSync(RunStarted)(encodedEvent);

    expect(Schema.decodeSync(RunEvent)(encodedEvent)).toEqual(event);
    const invalidEncodedEvent: unknown = {
      ...encodedEvent,
      eventVersion: 2,
    };
    expect(() => Schema.decodeUnknownSync(RunEvent)(invalidEncodedEvent)).toThrow();
  });

  it("round-trips a Schema-encoded application run disposition on ordinary completion", () => {
    const encodedEvent = {
      _tag: "RunCompleted",
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 5,
      timestamp: "2026-07-29T12:00:00.000Z",
      output: { itinerary: "Kyoto" },
      turns: 1,
      finishReason: "model-stop",
      runDisposition: "application-complete",
    } satisfies typeof RunCompleted.Encoded;

    const event = Schema.decodeSync(RunEvent)(encodedEvent);
    expect(event).toMatchObject({
      _tag: "RunCompleted",
      runDisposition: "application-complete",
    });
    expect(Schema.encodeSync(RunEvent)(event)).toEqual(encodedEvent);
  });

  it("preserves Tool Call parameters and provider execution provenance", () => {
    const encodedEvent = {
      _tag: "ToolCallDeclared",
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 3,
      timestamp: "2026-07-29T12:00:00.000Z",
      turnId: "turn-1",
      toolCallId: "search-1",
      toolName: "OpenAiWebSearch",
      parameters: { query: "current London travel context" },
      providerExecuted: true,
    } satisfies typeof ToolCallDeclared.Encoded;

    const event = Schema.decodeSync(ToolCallDeclared)(encodedEvent);

    expect(Schema.decodeSync(RunEvent)(encodedEvent)).toEqual(event);
    expect(event.providerExecuted).toBe(true);
    expect(event.parameters).toEqual({ query: "current London travel context" });
  });

  it("rejects malformed terminal Tool Call and Run Event payloads", () => {
    const event = {
      _tag: "ToolCallFailed",
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 4,
      timestamp: "2026-07-29T12:00:00.000Z",
      turnId: "turn-1",
      toolCallId: "search-1",
      toolName: "search_availability",
      errorTag: "AvailabilityFailure",
      message: "catalog unavailable",
      providerExecuted: false,
    } satisfies typeof ToolCallFailed.Encoded;

    expect(Schema.decodeSync(RunEvent)(event)).toMatchObject({
      _tag: "ToolCallFailed",
      toolCallId: "search-1",
    });
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({
        ...event,
        sequence: -1,
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({
        ...event,
        toolCallId: "",
      }),
    ).toThrow();
  });

  it("round-trips the Subagent Parent Link and rejects invalid lineage", () => {
    const encodedLink = {
      delegationId: "delegate-research",
      parentAgentId: "travel-planner",
      parentConversationId: "conversation-1",
      parentRunId: "run-1",
      parentToolCallId: "delegate-1",
      depth: 1,
    } satisfies typeof SubagentParentLink.Encoded;
    const link = Schema.decodeSync(SubagentParentLink)(encodedLink);

    expect(Schema.encodeSync(SubagentParentLink)(link)).toEqual(encodedLink);
    expect(() =>
      Schema.decodeUnknownSync(SubagentParentLink)({ ...encodedLink, depth: 0 }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(SubagentParentLink)({ ...encodedLink, depth: 1.5 }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(SubagentParentLink)({ ...encodedLink, delegationId: "" }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(SubagentParentLink)({ ...encodedLink, parentRunId: "" }),
    ).toThrow();
  });

  it("round-trips every Subagent lifecycle event through the public union", () => {
    const base = {
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 5,
      timestamp: "2026-07-29T12:00:00.000Z",
      turnId: "turn-1",
      toolCallId: "delegate-1",
      delegationId: "delegate-research",
      childConversationId: "conversation-2",
      childRunId: "run-2",
      targetAgentId: "research-specialist",
      depth: 1,
    } as const;
    const encodedEvents = [
      { _tag: "SubagentRequested", ...base } satisfies typeof SubagentRequested.Encoded,
      { _tag: "SubagentStarted", ...base } satisfies typeof SubagentStarted.Encoded,
      {
        _tag: "SubagentProgress",
        ...base,
        summary: "comparing rail connections",
      } satisfies typeof SubagentProgress.Encoded,
      {
        _tag: "SubagentCompleted",
        ...base,
        turns: 2,
        finishReason: "completed",
      } satisfies typeof SubagentCompleted.Encoded,
      {
        _tag: "SubagentFailed",
        ...base,
        errorTag: "ChildDomainFailure",
        message: "no availability",
      } satisfies typeof SubagentFailed.Encoded,
      {
        _tag: "SubagentInterrupted",
        ...base,
        reason: "parent run interrupted",
      } satisfies typeof SubagentInterrupted.Encoded,
      { _tag: "SubagentJoined", ...base } satisfies typeof SubagentJoined.Encoded,
    ] as const;

    for (const encodedEvent of encodedEvents) {
      const event = Schema.decodeSync(RunEvent)(encodedEvent);
      expect(event._tag).toBe(encodedEvent._tag);
      expect(Schema.encodeSync(RunEvent)(event)).toEqual(encodedEvent);
    }
  });

  it("rejects malformed Subagent lifecycle payloads", () => {
    const base = {
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 5,
      timestamp: "2026-07-29T12:00:00.000Z",
      turnId: "turn-1",
      toolCallId: "delegate-1",
      delegationId: "delegate-research",
      childConversationId: "conversation-2",
      childRunId: "run-2",
      targetAgentId: "research-specialist",
      depth: 1,
    } as const;
    const progress = {
      _tag: "SubagentProgress",
      ...base,
      summary: "comparing rail connections",
    } satisfies typeof SubagentProgress.Encoded;
    const { delegationId: _delegationId, ...withoutDelegationId } = progress;

    expect(() => Schema.decodeUnknownSync(RunEvent)({ ...progress, eventVersion: 2 })).toThrow();
    expect(() => Schema.decodeUnknownSync(RunEvent)({ ...progress, depth: 0 })).toThrow();
    expect(() => Schema.decodeUnknownSync(RunEvent)({ ...progress, childRunId: "" })).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({ ...progress, childConversationId: "" }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({ ...progress, summary: "x".repeat(4 * 1024 + 1) }),
    ).toThrow();
    expect(() => Schema.decodeUnknownSync(RunEvent)(withoutDelegationId)).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({
        _tag: "SubagentCompleted",
        ...base,
        turns: 0,
        finishReason: "completed",
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({
        _tag: "SubagentCompleted",
        ...base,
        turns: 2,
        finishReason: "tool-calls",
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({
        _tag: "SubagentFailed",
        ...base,
        errorTag: "",
        message: "no availability",
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({
        _tag: "SubagentInterrupted",
        ...base,
        reason: "x".repeat(4 * 1024 + 1),
      }),
    ).toThrow();
  });

  it("mints valid, distinct branded identities from the default IdGenerator Layer", () => {
    const program = Effect.gen(function* () {
      const ids = yield* IdGenerator;
      return {
        firstConversation: yield* ids.nextConversationId,
        secondConversation: yield* ids.nextConversationId,
        run: yield* ids.nextRunId,
        turn: yield* ids.nextTurnId,
      };
    }).pipe(Effect.provide(IdGenerator.layer));
    const { firstConversation, secondConversation, run, turn } = Effect.runSync(program);

    expect(Schema.decodeSync(ConversationId)(firstConversation)).toBe(firstConversation);
    expect(Schema.decodeSync(RunId)(run)).toBe(run);
    expect(Schema.decodeSync(TurnId)(turn)).toBe(turn);
    expect(firstConversation).not.toBe(secondConversation);
    expect(firstConversation.startsWith("conversation-")).toBe(true);
    expect(run.startsWith("run-")).toBe(true);
    expect(turn.startsWith("turn-")).toBe(true);
  });
});

describe("context-economics policy", () => {
  it("fills context-economics defaults and accepts explicit overrides", () => {
    const policy = AgentPolicy.make({
      maxTurns: 2,
      maxToolCalls: 1,
      maxDuration: "30 seconds",
      toolConcurrency: 1,
    });

    expect(policy.toolResultBounds.maxBytes).toBe(50 * 1024);
    expect(policy.onExhaustion).toBe("final-answer");
    expect(policy.runStatus).toBe("appended");
    expect(policy.compaction.keepRecentTokens).toBe(20_000);
    expect(policy.compaction.mode).toBe("prune-then-summarize");
    expect(policy.contextTokenLimit).toBeUndefined();

    const custom = AgentPolicy.make({
      maxTurns: 2,
      maxToolCalls: 1,
      maxDuration: "30 seconds",
      toolConcurrency: 1,
      contextTokenLimit: 30_000,
      toolResultBounds: ToolResultBounds.make({ maxBytes: 1_024 }),
      onExhaustion: "fail",
      runStatus: "off",
      compaction: CompactionPolicy.make({ keepRecentTokens: 5_000, mode: "prune" }),
    });

    expect(custom.contextTokenLimit).toBe(30_000);
    expect(custom.toolResultBounds.maxBytes).toBe(1_024);
    expect(custom.onExhaustion).toBe("fail");
    expect(custom.runStatus).toBe("off");
    expect(custom.compaction.keepRecentTokens).toBe(5_000);
    expect(custom.compaction.mode).toBe("prune");
  });

  it("rejects non-positive context-economics bounds", () => {
    expect(() =>
      AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
        contextTokenLimit: 0,
      }),
    ).toThrow();
    expect(() => ToolResultBounds.make({ maxBytes: 0 })).toThrow();
    expect(() => CompactionPolicy.make({ keepRecentTokens: 0 })).toThrow();
    expect(() => CompactionPolicy.make({ keepRecentTokens: 2.5 })).toThrow();
  });
});

describe("tool result bounds", () => {
  it("RUN-022: returns a within-bounds encoded result unchanged", () => {
    const encoded = JSON.stringify({ stdout: "ok", code: 0 });

    expect(applyToolResultBounds(encoded, ToolResultBounds.make({ maxBytes: 1_024 }))).toBe(
      encoded,
    );
  });

  it("RUN-022: truncates an oversized result into the canonical envelope within maxBytes", () => {
    const encoded = JSON.stringify({ stdout: "line\n".repeat(4_000), code: 0 });
    const bounds = ToolResultBounds.make({ maxBytes: 1_024 });
    const output = applyToolResultBounds(encoded, bounds);
    const envelope = Schema.decodeUnknownSync(TruncatedToolResult)(JSON.parse(output));

    expect(utf8Bytes(output)).toBeLessThanOrEqual(1_024);
    expect(envelope.truncatedToolResult).toBe(true);
    expect(envelope.originalBytes).toBe(utf8Bytes(encoded));
    expect(envelope.head.length).toBeGreaterThan(0);
    expect(envelope.tail.length).toBeGreaterThan(0);
    expect(encoded.startsWith(envelope.head)).toBe(true);
    expect(encoded.endsWith(envelope.tail)).toBe(true);
    expect(applyToolResultBounds(encoded, bounds)).toBe(output);
  });

  it("RUN-022: never splits multibyte characters and always emits valid JSON", () => {
    const encoded = JSON.stringify({ text: `${"🦀".repeat(2_000)}${"端到端".repeat(2_000)}` });
    const output = applyToolResultBounds(encoded, ToolResultBounds.make({ maxBytes: 512 }));
    const envelope = Schema.decodeUnknownSync(TruncatedToolResult)(JSON.parse(output));

    expect(utf8Bytes(output)).toBeLessThanOrEqual(512);
    expect(encoded.startsWith(envelope.head)).toBe(true);
    expect(encoded.endsWith(envelope.tail)).toBe(true);
    for (const slice of [envelope.head, envelope.tail]) {
      for (const unit of [slice.charCodeAt(0), slice.charCodeAt(slice.length - 1)]) {
        expect(unit >= 0xdc00 && unit <= 0xdfff && slice.length === 1).toBe(false);
      }
    }
  });

  it("RUN-022: rejects bounds below the guaranteed envelope floor at construction", () => {
    expect(() => ToolResultBounds.make({ maxBytes: 255 })).toThrow();
    expect(() => ToolResultBounds.make({ maxBytes: 8 })).toThrow();
    expect(() => Schema.decodeUnknownSync(ToolResultBounds)({ maxBytes: 128 })).toThrow();
    expect(ToolResultBounds.make({ maxBytes: 256 }).maxBytes).toBe(256);
  });

  it("RUN-022: the envelope fits within the bound at the exact 256-byte floor", () => {
    const encoded = JSON.stringify({ stdout: "x".repeat(500_000) });
    const output = applyToolResultBounds(encoded, ToolResultBounds.make({ maxBytes: 256 }));
    const envelope = Schema.decodeUnknownSync(TruncatedToolResult)(JSON.parse(output));

    expect(utf8Bytes(output)).toBeLessThanOrEqual(256);
    expect(envelope.originalBytes).toBe(utf8Bytes(encoded));
    expect(encoded.startsWith(envelope.head)).toBe(true);
    expect(encoded.endsWith(envelope.tail)).toBe(true);
  });

  it("RUN-022: the unserializable sentinel is total under hostile causes", () => {
    const hostileToString = {
      toString() {
        throw new Error("toString trap");
      },
    };
    class HostileMessage extends Error {
      override get message(): string {
        throw new Error("message trap");
      }
    }
    for (const cause of [hostileToString, new HostileMessage()]) {
      const sentinel = unserializableToolResult(cause);
      expect(sentinel.unserializableToolResult).toBe(true);
      expect(typeof sentinel.reason).toBe("string");
    }
  });

  it("RUN-022: the encoded sentinel never exceeds the 256-byte policy floor", () => {
    for (const cause of [
      new Error("💩".repeat(300)),
      "🜁".repeat(400),
      new Error('\\"'.repeat(400)),
    ]) {
      const sentinel = unserializableToolResult(cause);
      expect(utf8Bytes(JSON.stringify(sentinel))).toBeLessThanOrEqual(256);
    }
  });
});

describe("context-economics errors and events", () => {
  it("round-trips ContextOverflowError as an expected framework failure", () => {
    const error = ContextOverflowError.make({
      message: "prompt exceeds the model context window",
      retried: true,
    });
    const encoded = Schema.encodeSync(ContextOverflowError)(error);

    expect(error._tag).toBe("ContextOverflowError");
    expect(Schema.decodeSync(ContextOverflowError)(encoded)).toEqual(error);
  });

  it("round-trips BudgetWarning and CompactionPerformed through the public union", () => {
    const base = {
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 6,
      timestamp: "2026-08-15T12:00:00.000Z",
    } as const;
    const warning = {
      _tag: "BudgetWarning",
      ...base,
      limit: "tokens",
      consumed: 160_000,
      limitValue: 200_000,
    } satisfies typeof BudgetWarning.Encoded;
    const compaction = {
      _tag: "CompactionPerformed",
      ...base,
      turn: 3,
      kind: "summarize",
      tokensBeforeEstimate: 180_000,
      tokensAfterEstimate: 24_000,
    } satisfies typeof CompactionPerformed.Encoded;

    for (const encodedEvent of [warning, compaction] as const) {
      const event = Schema.decodeSync(RunEvent)(encodedEvent);
      expect(event._tag).toBe(encodedEvent._tag);
      expect(Schema.encodeSync(RunEvent)(event)).toEqual(encodedEvent);
    }
    expect(() => Schema.decodeUnknownSync(RunEvent)({ ...warning, limit: "vibes" })).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({ ...compaction, kind: "delete-history" }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({ ...compaction, tokensBeforeEstimate: -1 }),
    ).toThrow();
  });

  it("decodes terminal completion events with and without the exhausted marker", () => {
    const encodedRun = {
      _tag: "RunCompleted",
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 9,
      timestamp: "2026-08-15T12:00:00.000Z",
      output: { summary: "partial findings" },
      turns: 3,
      finishReason: "completed",
    } satisfies typeof RunCompleted.Encoded;

    expect(Schema.decodeSync(RunCompleted)(encodedRun).exhausted).toBeUndefined();
    expect(
      Schema.decodeUnknownSync(RunEvent)({ ...encodedRun, exhausted: "tokens" }),
    ).toMatchObject({ _tag: "RunCompleted", exhausted: "tokens" });
    expect(() =>
      Schema.decodeUnknownSync(RunEvent)({ ...encodedRun, exhausted: "duration" }),
    ).toThrow();

    const encodedChild = {
      _tag: "SubagentCompleted",
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "travel-planner",
      sequence: 10,
      timestamp: "2026-08-15T12:00:00.000Z",
      turnId: "turn-1",
      toolCallId: "delegate-1",
      delegationId: "delegate-research",
      childConversationId: "conversation-2",
      childRunId: "run-2",
      targetAgentId: "research-specialist",
      depth: 1,
      turns: 2,
      finishReason: "completed",
      exhausted: "turns",
    } satisfies typeof SubagentCompleted.Encoded;

    expect(Schema.decodeUnknownSync(RunEvent)(encodedChild)).toMatchObject({
      _tag: "SubagentCompleted",
      exhausted: "turns",
    });
  });
});
