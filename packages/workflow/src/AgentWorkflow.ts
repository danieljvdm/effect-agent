import { AgentInputError, AgentOutputError } from "@effect-agent/core/AgentError";
import { BindingUnavailable } from "@effect-agent/thread/AgentRegistration";
import { DigestError } from "@effect-agent/thread/Digest";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import { OperationDenied } from "@effect-agent/thread/OperationAuthorizer";
import {
  AdmissionConflict,
  LedgerError,
  SettlementConflict,
} from "@effect-agent/thread/SubmissionLedger";
import {
  AppendConflict,
  FenceRejected,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "@effect-agent/thread/ThreadStore";
import { Effect, Schema } from "effect";

import { WorkflowAdmissionClosed, WorkflowAgentHost } from "./WorkflowAgentHost.ts";
import { WorkflowDispatchError } from "./WorkflowDispatch.ts";
import {
  WorkflowExecutionFailure,
  type WorkflowAgent,
  type WorkflowExecuteOptions,
} from "./WorkflowExecution.ts";

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
 * Pass the exact Agent Definition instance used in runtime registration; a same-ID copy fails
 * with BindingUnavailable before input encoding or admission.
 */
export const execute = Effect.fn("AgentWorkflow.execute")(function* <
  Input extends Schema.Top,
  Output extends Schema.Top,
>(agent: WorkflowAgent<Input, Output>, input: Input["Type"], options: WorkflowExecuteOptions) {
  const host = yield* WorkflowAgentHost;

  return yield* host.execute(agent, input, options);
});
