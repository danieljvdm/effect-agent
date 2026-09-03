import { RunEvent } from "@effect-agent/core/RunEvent";
import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { projectToolTraces } from "./tool-trace";

const event = (input: typeof RunEvent.Encoded): RunEvent => Schema.decodeSync(RunEvent)(input);

const base = {
  eventVersion: 1 as const,
  runId: "run-1",
  threadId: "thread-1",
  agentId: "general-chat-openai",
  timestamp: "2026-07-29T12:00:00.000Z",
  turnId: "turn-1",
};

describe("demo Tool trace projection", () => {
  it("distinguishes application and provider-hosted Tool Calls", () => {
    const traces = projectToolTraces([
      event({
        ...base,
        _tag: "ToolCallDeclared",
        sequence: 0,
        toolCallId: "fixture-search-1",
        toolName: "search_fixture_knowledge",
        parameters: { query: "Europe in August" },
        providerExecuted: false,
      }),
      event({
        ...base,
        _tag: "ToolCallStarted",
        sequence: 1,
        toolCallId: "fixture-search-1",
        toolName: "search_fixture_knowledge",
      }),
      event({
        ...base,
        _tag: "ToolCallSucceeded",
        sequence: 2,
        toolCallId: "fixture-search-1",
        toolName: "search_fixture_knowledge",
        result: { fixture: true },
        providerExecuted: false,
      }),
      event({
        ...base,
        _tag: "ToolCallDeclared",
        sequence: 3,
        toolCallId: "web-1",
        toolName: "OpenAiWebSearch",
        parameters: { action: { query: "London travel news" } },
        providerExecuted: true,
      }),
      event({
        ...base,
        _tag: "ToolCallSucceeded",
        sequence: 4,
        toolCallId: "web-1",
        toolName: "OpenAiWebSearch",
        result: { status: "completed" },
        providerExecuted: true,
      }),
    ]);

    expect(traces).toEqual([
      {
        toolCallId: "fixture-search-1",
        toolName: "search_fixture_knowledge",
        parameters: { query: "Europe in August" },
        providerExecuted: false,
        state: "output-available",
        result: { fixture: true },
      },
      {
        toolCallId: "web-1",
        toolName: "OpenAiWebSearch",
        parameters: { action: { query: "London travel news" } },
        providerExecuted: true,
        state: "output-available",
        result: { status: "completed" },
      },
    ]);
  });
});
