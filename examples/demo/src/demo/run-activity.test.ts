import { RunEvent } from "@effect-agent/core";
import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { projectRunActivity } from "./run-activity";

const event = (input: typeof RunEvent.Encoded): RunEvent => Schema.decodeSync(RunEvent)(input);

const base = {
  eventVersion: 1 as const,
  runId: "run-1",
  conversationId: "conversation-1",
  agentId: "general-chat-openai",
  timestamp: "2026-07-29T12:00:00.000Z",
};

describe("run activity projection", () => {
  it("moves a direct response from startup through thinking to writing", () => {
    expect(projectRunActivity([], "openai")).toEqual({
      phase: "starting",
      label: "Starting agent…",
      detail: "Live profile",
    });

    const events = [
      event({ ...base, _tag: "RunStarted", sequence: 0 }),
      event({
        ...base,
        _tag: "ModelStarted",
        sequence: 1,
        turnId: "turn-1",
        turn: 1,
      }),
    ];

    expect(projectRunActivity(events, "openai")).toEqual({
      phase: "thinking",
      label: "Thinking…",
      detail: "Model turn 1",
    });

    expect(
      projectRunActivity(
        [
          ...events,
          event({
            ...base,
            _tag: "TextDelta",
            sequence: 2,
            turnId: "turn-1",
            text: "Hello",
          }),
        ],
        "openai",
      ),
    ).toEqual({
      phase: "composing",
      label: "Writing the response…",
      detail: "Assistant text stream",
    });
  });

  it("distinguishes waiting, running, and composing for application Tools", () => {
    const declared = event({
      ...base,
      _tag: "ToolCallDeclared",
      sequence: 0,
      turnId: "turn-1",
      toolCallId: "calculate-1",
      toolName: "calculate",
      parameters: { operation: "multiply", left: 137, right: 19 },
      providerExecuted: false,
    });

    expect(projectRunActivity([declared], "deterministic")).toMatchObject({
      phase: "waiting",
      label: "Waiting to calculate…",
    });

    const started = event({
      ...base,
      _tag: "ToolCallStarted",
      sequence: 1,
      turnId: "turn-1",
      toolCallId: "calculate-1",
      toolName: "calculate",
    });
    expect(projectRunActivity([declared, started], "deterministic")).toMatchObject({
      phase: "tool",
      label: "Calculating…",
    });

    const succeeded = event({
      ...base,
      _tag: "ToolCallSucceeded",
      sequence: 2,
      turnId: "turn-1",
      toolCallId: "calculate-1",
      toolName: "calculate",
      result: { value: 2603 },
      providerExecuted: false,
    });
    expect(projectRunActivity([declared, started, succeeded], "deterministic")).toMatchObject({
      phase: "composing",
      label: "Checking the calculation…",
    });

    expect(
      projectRunActivity(
        [
          declared,
          started,
          succeeded,
          event({
            ...base,
            _tag: "ModelStarted",
            sequence: 3,
            turnId: "turn-2",
            turn: 2,
          }),
        ],
        "deterministic",
      ),
    ).toMatchObject({
      phase: "composing",
      label: "Composing from tool results…",
    });
  });

  it("treats a provider-hosted Tool as running immediately", () => {
    const declared = event({
      ...base,
      _tag: "ToolCallDeclared",
      sequence: 0,
      turnId: "turn-1",
      toolCallId: "web-1",
      toolName: "OpenAiWebSearch",
      parameters: { action: { query: "current Effect release" } },
      providerExecuted: true,
    });

    expect(projectRunActivity([declared], "openai")).toEqual({
      phase: "tool",
      label: "Searching the web…",
      detail: "OpenAiWebSearch · OpenAI hosted",
    });
  });

  it("keeps reporting an active Tool when another call completes", () => {
    const first = event({
      ...base,
      _tag: "ToolCallDeclared",
      sequence: 0,
      turnId: "turn-1",
      toolCallId: "calculate-1",
      toolName: "calculate",
      parameters: { operation: "add", left: 1, right: 2 },
      providerExecuted: false,
    });
    const second = event({
      ...base,
      _tag: "ToolCallDeclared",
      sequence: 1,
      turnId: "turn-1",
      toolCallId: "fixture-1",
      toolName: "search_fixture_knowledge",
      parameters: { query: "Effect" },
      providerExecuted: false,
    });
    const firstSucceeded = event({
      ...base,
      _tag: "ToolCallSucceeded",
      sequence: 2,
      turnId: "turn-1",
      toolCallId: "calculate-1",
      toolName: "calculate",
      result: { value: 3 },
      providerExecuted: false,
    });

    expect(projectRunActivity([first, second, firstSucceeded], "deterministic")).toMatchObject({
      phase: "waiting",
      label: "Waiting to search offline knowledge…",
    });
  });

  it("projects terminal and suspended events without transport-specific guesses", () => {
    expect(
      projectRunActivity(
        [
          event({
            ...base,
            _tag: "RunSuspended",
            sequence: 0,
            reason: "Approval required",
          }),
        ],
        "openai",
      ),
    ).toEqual({
      phase: "waiting",
      label: "Waiting for external input…",
      detail: "Approval required",
    });

    expect(
      projectRunActivity(
        [
          event({
            ...base,
            _tag: "RunCompleted",
            sequence: 0,
            output: { answer: "Done" },
            turns: 2,
            finishReason: "completed",
          }),
        ],
        "openai",
      ),
    ).toEqual({
      phase: "complete",
      label: "Response complete",
      detail: "2 model turns",
    });

    expect(
      projectRunActivity(
        [
          event({
            ...base,
            _tag: "RunFailed",
            sequence: 0,
            errorTag: "ModelUnavailable",
            message: "The model is unavailable.",
          }),
        ],
        "openai",
      ),
    ).toEqual({
      phase: "failed",
      label: "Run failed",
      detail: "The model is unavailable.",
    });

    expect(
      projectRunActivity(
        [
          event({
            ...base,
            _tag: "RunInterrupted",
            sequence: 0,
            message: "Stopped by the user.",
          }),
        ],
        "openai",
      ),
    ).toEqual({
      phase: "stopped",
      label: "Run stopped",
      detail: "Stopped by the user.",
    });
  });
});
