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
  IdGenerator,
  ModelProtocolError,
  RunEvent,
  RunId,
  RunStarted,
  ToolCallFailed,
  ToolCallDeclared,
  TurnId,
} from "../src/index.ts";

describe("core schemas", () => {
  it("decodes distinct non-empty branded identifiers", () => {
    expect(Schema.decodeSync(AgentId)("travel-planner")).toBe("travel-planner");
    expect(() => Schema.decodeSync(RunId)("")).toThrow();
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
