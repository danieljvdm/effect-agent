import { describe, expect, it } from "@effect/vitest";
import { Effect, Logger } from "effect";

import { compactReviewLogger, formatCompactLogLine } from "../src/index.ts";

const DATE = new Date("2026-08-15T06:21:06.284Z");

describe("formatCompactLogLine", () => {
  it("renders engine tool telemetry as one short line without the annotation dump", () => {
    const line = formatCompactLogLine({
      date: DATE,
      logLevel: "Info",
      message: "agent tool execution completed",
      annotations: {
        "gen_ai.operation.name": "execute_tool",
        "gen_ai.tool.name": "read_file_diff",
        "effect_agent.tool.outcome": "success",
        agentId: "pr-file-reviewer",
        conversationId: "conversation-1",
        runId: "run-1",
        turnId: "turn-1",
        toolName: "read_file_diff",
        toolExecutionClass: "readonly",
        toolOutcome: "success",
      },
    });
    expect(line).toBe("[06:21:06] ✓ tool read_file_diff");
  });

  it("renders a failed tool execution at its warning level", () => {
    const line = formatCompactLogLine({
      date: DATE,
      logLevel: "Warn",
      message: "agent tool execution failed",
      annotations: { toolName: "read_file_diff" },
    });
    expect(line).toBe("[06:21:06] WARN ✗ tool read_file_diff failed");
  });

  it("keeps unmapped info messages plain and appends annotations only for warnings", () => {
    expect(
      formatCompactLogLine({
        date: DATE,
        logLevel: "Info",
        message: "review retirement finished",
        annotations: { reviewsRetired: 2 },
      }),
    ).toBe("[06:21:06] review retirement finished");
    expect(
      formatCompactLogLine({
        date: DATE,
        logLevel: "Warn",
        message: "review progress comment update failed",
        annotations: {
          operation: "createProgressComment",
          "effect_agent.tool.outcome": "duplicate-otel-key",
        },
      }),
    ).toBe(
      "[06:21:06] WARN review progress comment update failed · operation=createProgressComment",
    );
  });

  it("appends the pretty cause on its own line", () => {
    const line = formatCompactLogLine({
      date: DATE,
      logLevel: "Error",
      message: "review run failed",
      annotations: {},
      cause: "Error: boom",
    });
    expect(line).toBe("[06:21:06] ERROR review run failed\nError: boom");
  });
});

describe("compactReviewLogger", () => {
  it.effect("reads annotations from the emitting fiber", () =>
    Effect.gen(function* () {
      const lines: Array<string> = [];
      const collector = Logger.make((options) => {
        lines.push(compactReviewLogger.log(options));
      });
      yield* Effect.logInfo("agent tool execution completed").pipe(
        Effect.annotateLogs({
          toolName: "read_file_diff",
          "gen_ai.tool.name": "read_file_diff",
          runId: "run-1",
        }),
        Effect.provide(Logger.layer([collector])),
      );
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatch(/^\[\d{2}:\d{2}:\d{2}\] ✓ tool read_file_diff$/);
    }),
  );
});
