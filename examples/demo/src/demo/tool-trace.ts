import type { RunEvent } from "@effect-agent/core";
import type { DynamicToolUIPart } from "ai";

export interface ToolTrace {
  readonly toolCallId: string;
  readonly toolName: string;
  readonly parameters: unknown;
  readonly providerExecuted: boolean;
  readonly state: DynamicToolUIPart["state"];
  readonly result?: unknown;
  readonly failure?: {
    readonly errorTag: string;
    readonly message: string;
  };
}

/** Projects semantic Tool events into one current UI row per Tool Call. */
export const projectToolTraces = (events: ReadonlyArray<RunEvent>): ReadonlyArray<ToolTrace> => {
  const traces = new Map<string, ToolTrace>();

  for (const event of events) {
    switch (event._tag) {
      case "ToolCallDeclared": {
        traces.set(event.toolCallId, {
          toolCallId: event.toolCallId,
          toolName: event.toolName,
          parameters: event.parameters,
          providerExecuted: event.providerExecuted,
          state: event.providerExecuted ? "input-available" : "input-streaming",
        });
        break;
      }
      case "ToolCallStarted": {
        const current = traces.get(event.toolCallId);
        if (current !== undefined) {
          traces.set(event.toolCallId, { ...current, state: "input-available" });
        }
        break;
      }
      case "ToolProgress": {
        const current = traces.get(event.toolCallId);
        if (current !== undefined) {
          traces.set(event.toolCallId, {
            ...current,
            result: event.result,
            state: "input-available",
          });
        }
        break;
      }
      case "ToolCallSucceeded": {
        const current = traces.get(event.toolCallId);
        if (current !== undefined) {
          traces.set(event.toolCallId, {
            ...current,
            result: event.result,
            state: "output-available",
          });
        }
        break;
      }
      case "ToolCallFailed": {
        const current = traces.get(event.toolCallId);
        if (current !== undefined) {
          traces.set(event.toolCallId, {
            ...current,
            failure: {
              errorTag: event.errorTag,
              message: event.message,
            },
            state: "output-error",
          });
        }
        break;
      }
    }
  }

  return Array.from(traces.values());
};
