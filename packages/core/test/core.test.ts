import { describe, expect, it } from "vite-plus/test";

import { Duration, Schema } from "effect";

import {
  AgentId,
  AgentError,
  AgentInputError,
  AgentInterrupted,
  AgentOutputError,
  AgentPolicy,
  AgentPolicyError,
  ModelProtocolError,
  RunEvent,
  RunId,
  RunStarted,
  ToolCallFailed,
  ToolCallDeclared,
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
    });

    expect(policy.maxDuration).toEqual(Duration.seconds(30));
    expect(policy.repeatedFailureLimit).toBe(3);
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
      new AgentInputError({ message: "invalid trip input" }),
      new AgentOutputError({ message: "invalid itinerary output" }),
      new AgentPolicyError({ limit: "turns", message: "turn limit reached" }),
      new ModelProtocolError({ message: "response completed twice" }),
      new AgentInterrupted({ message: "consumer disconnected" }),
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
      ...event,
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
});
