import { type RunEvent } from "@effect-agent/core/RunEvent";

import type { DemoMode } from "./contracts";

export type RunActivityPhase =
  | "starting"
  | "thinking"
  | "tool"
  | "composing"
  | "waiting"
  | "complete"
  | "failed"
  | "stopped";

export interface RunActivity {
  readonly phase: RunActivityPhase;
  readonly label: string;
  readonly detail?: string;
}

interface ToolActivity {
  readonly name: string;
  readonly providerExecuted: boolean;
  readonly state: "waiting" | "running" | "succeeded" | "failed";
}

const humanizeToolName = (name: string): string =>
  name
    .replaceAll("_", " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());

const toolDetail = (tool: ToolActivity): string =>
  `${tool.name} · ${tool.providerExecuted ? "OpenAI hosted" : "framework tool"}`;

const activeToolLabel = (tool: ToolActivity): string => {
  switch (tool.name) {
    case "OpenAiWebSearch":
      return "Searching the web…";
    case "search_fixture_knowledge":
      return tool.state === "waiting"
        ? "Waiting to search offline knowledge…"
        : "Searching offline knowledge…";
    case "calculate":
      return tool.state === "waiting" ? "Waiting to calculate…" : "Calculating…";
    default:
      return `${tool.state === "waiting" ? "Waiting to run" : "Running"} ${humanizeToolName(
        tool.name,
      )}…`;
  }
};

const activeToolActivity = (tool: ToolActivity): RunActivity => ({
  phase: tool.state === "waiting" ? "waiting" : "tool",
  label: activeToolLabel(tool),
  detail: toolDetail(tool),
});

const latestActiveTool = (tools: ReadonlyMap<string, ToolActivity>): ToolActivity | undefined => {
  let latest: ToolActivity | undefined;

  for (const tool of tools.values()) {
    if (tool.state === "waiting" || tool.state === "running") {
      latest = tool;
    }
  }

  return latest;
};

const completedToolLabel = (tool: ToolActivity): string => {
  switch (tool.name) {
    case "OpenAiWebSearch":
      return "Reviewing web results…";
    case "search_fixture_knowledge":
      return "Reviewing fixture results…";
    case "calculate":
      return "Checking the calculation…";
    default:
      return `Reviewing results from ${humanizeToolName(tool.name)}…`;
  }
};

/** Derives concise, user-facing activity from the stable semantic event stream. */
export const projectRunActivity = (
  events: ReadonlyArray<RunEvent>,
  mode: DemoMode,
): RunActivity => {
  let activity: RunActivity = {
    phase: "starting",
    label: "Starting agent…",
    detail: mode === "openai" ? "Live profile" : "Offline fixture profile",
  };

  let hasToolResult = false;
  const tools = new Map<string, ToolActivity>();

  for (const event of events) {
    switch (event._tag) {
      case "RunStarted": {
        activity = {
          phase: "starting",
          label: "Preparing the run…",
          detail: "Run created",
        };
        break;
      }
      case "TurnStarted":
      case "ModelStarted": {
        activity = hasToolResult
          ? {
              phase: "composing",
              label: "Composing from tool results…",
              detail: `Model turn ${event.turn}`,
            }
          : {
              phase: "thinking",
              label: "Thinking…",
              detail: `Model turn ${event.turn}`,
            };
        break;
      }
      case "ReasoningDelta": {
        activity = {
          phase: "thinking",
          label: hasToolResult ? "Reasoning over tool results…" : "Reasoning…",
          detail: "Provider reasoning stream",
        };
        break;
      }
      case "TextDelta": {
        activity = {
          phase: "composing",
          label: "Writing the response…",
          detail: "Assistant text stream",
        };
        break;
      }
      case "ToolCallDeclared": {
        const tool: ToolActivity = {
          name: event.toolName,
          providerExecuted: event.providerExecuted,
          state: event.providerExecuted ? "running" : "waiting",
        };

        tools.set(event.toolCallId, tool);
        activity = activeToolActivity(tool);
        break;
      }
      case "ToolCallStarted":
      case "ToolProgress": {
        const declared = tools.get(event.toolCallId);

        const tool: ToolActivity = {
          name: event.toolName,
          providerExecuted:
            event._tag === "ToolProgress"
              ? event.providerExecuted
              : (declared?.providerExecuted ?? false),
          state: "running",
        };

        tools.set(event.toolCallId, tool);
        activity = activeToolActivity(tool);
        break;
      }
      case "ToolCallSucceeded": {
        const tool: ToolActivity = {
          name: event.toolName,
          providerExecuted: event.providerExecuted,
          state: "succeeded",
        };

        tools.set(event.toolCallId, tool);
        hasToolResult = true;
        const activeTool = latestActiveTool(tools);

        activity =
          activeTool === undefined
            ? {
                phase: "composing",
                label: completedToolLabel(tool),
                detail: toolDetail(tool),
              }
            : activeToolActivity(activeTool);
        break;
      }
      case "ToolCallFailed": {
        const tool: ToolActivity = {
          name: event.toolName,
          providerExecuted: event.providerExecuted,
          state: "failed",
        };

        tools.set(event.toolCallId, tool);
        hasToolResult = true;
        const activeTool = latestActiveTool(tools);

        activity =
          activeTool === undefined
            ? {
                phase: "thinking",
                label: "Recovering from a tool failure…",
                detail: toolDetail(tool),
              }
            : activeToolActivity(activeTool);
        break;
      }
      case "ApprovalRequested": {
        const declared = tools.get(event.toolCallId);

        activity = {
          phase: "waiting",
          label: `Waiting for approval to run ${humanizeToolName(event.toolName)}…`,
          detail:
            declared === undefined ? event.toolName : toolDetail({ ...declared, state: "waiting" }),
        };
        break;
      }
      case "TurnCompleted": {
        activity =
          event.finishReason === "tool-calls"
            ? {
                phase: "composing",
                label: "Continuing with tool results…",
                detail: `Model turn ${event.turn} complete`,
              }
            : {
                phase: "composing",
                label: "Finalizing the response…",
                detail: `Model turn ${event.turn} complete`,
              };
        break;
      }
      case "RunCompleted": {
        activity = {
          phase: "complete",
          label: "Response complete",
          detail: `${event.turns} model ${event.turns === 1 ? "turn" : "turns"}`,
        };
        break;
      }
      case "RunFailed": {
        activity = {
          phase: "failed",
          label: "Run failed",
          detail: event.message,
        };
        break;
      }
      case "RunInterrupted": {
        activity = {
          phase: "stopped",
          label: "Run stopped",
          detail: event.message,
        };
        break;
      }
      case "RunSuspended": {
        activity = {
          phase: "waiting",
          label: "Waiting for external input…",
          detail: event.reason,
        };
        break;
      }
    }
  }

  return activity;
};
