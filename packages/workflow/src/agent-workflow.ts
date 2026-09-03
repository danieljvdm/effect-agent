import { AgentInputError, AgentOutputError } from "@effect-agent/core";
import {
  AdmissionConflict,
  AppendConflict,
  BindingUnavailable,
  DigestError,
  DurableRuntimeFailpointError,
  FenceRejected,
  LedgerError,
  OperationDenied,
  SettlementConflict,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "@effect-agent/thread";
import { Effect, Schema } from "effect";

import { WorkflowDispatchError } from "./dispatch.ts";
import {
  WorkflowExecutionFailure,
  type WorkflowAgent,
  type WorkflowExecuteOptions,
} from "./execution.ts";
import { WorkflowAdmissionClosed, WorkflowAgentHost } from "./host.ts";

/** Schema for the exact typed failure channel of execute, suitable for Workflow.make({ error }). */
export const Error = Schema.Union([
  AgentInputError,
  AgentOutputError,
  AdmissionConflict,
  AppendConflict,
  BindingUnavailable,
  DigestError,
  DurableRuntimeFailpointError,
  FenceRejected,
  LedgerError,
  OperationDenied,
  SettlementConflict,
  ThreadNotMaterialized,
  ThreadStoreError,
  WorkflowDispatchError,
  WorkflowAdmissionClosed,
  WorkflowExecutionFailure,
]);

export type Error = typeof Error.Type;

/**
 * Run a registered agent inside a native Workflow.toLayer handler. One stable name identifies
 * one submission in that parent execution. Replays verify admission identity and decode the
 * canonical output again. Pending work suspends through Effect DurableDeferred; neither
 * suspension nor parent interruption aborts accepted agent work. Use the host's authorized abort.
 * Provide WorkflowAgentHost at the application boundary, sharing the parent's WorkflowEngine.
 */
export const execute = Effect.fn("AgentWorkflow.execute")(function* <
  Input extends Schema.Top,
  Output extends Schema.Top,
>(agent: WorkflowAgent<Input, Output>, input: Input["Type"], options: WorkflowExecuteOptions) {
  const host = yield* WorkflowAgentHost;

  return yield* host.execute(agent, input, options);
});
