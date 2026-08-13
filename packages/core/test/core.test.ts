import { describe, expect, it } from "vite-plus/test";

import { Duration, Effect, Schema } from "effect";

import {
  AgentId,
  AgentApprovalDenied,
  AgentApprovalPending,
  AgentError,
  AgentInputError,
  AgentInterrupted,
  AgentOutputError,
  AgentPolicy,
  AgentPolicyError,
  ConversationId,
  DelegationId,
  IdGenerator,
  ModelProtocolError,
  RunEvent,
  RunId,
  RunStarted,
  SubagentCompleted,
  SubagentFailed,
  SubagentInterrupted,
  SubagentJoined,
  SubagentParentLink,
  SubagentProgress,
  SubagentRequested,
  SubagentStarted,
  ToolCallFailed,
  ToolCallDeclared,
  TurnId,
} from "../src/index.ts";

describe("core schemas", () => {
  it("decodes distinct non-empty branded identifiers", () => {
    expect(Schema.decodeSync(AgentId)("travel-planner")).toBe("travel-planner");
    expect(Schema.decodeSync(DelegationId)("delegate-research")).toBe("delegate-research");
    expect(() => Schema.decodeSync(RunId)("")).toThrow();
    expect(() => Schema.decodeSync(DelegationId)("")).toThrow();
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
      AgentApprovalPending.make({
        approvalId: "approval-1",
        toolCallId: "hold-1",
        toolName: "hold",
        message: "pending",
      }),
      ModelProtocolError.make({ message: "response completed twice" }),
      AgentInterrupted.make({ message: "consumer disconnected" }),
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
