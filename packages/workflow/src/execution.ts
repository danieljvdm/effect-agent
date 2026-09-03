import type { AgentId } from "@effect-agent/core";
import { Receipt, SettlementFailureDiagnostic } from "@effect-agent/thread";
import { Schema } from "effect";

/** The definition supplies codecs; executable model and tool services belong to registration. */
export interface WorkflowAgent<Input extends Schema.Top, Output extends Schema.Top> {
  readonly id: typeof AgentId.Type;
  readonly input: Input;
  readonly output: Output;
}

export interface WorkflowExecuteOptions {
  /** Stable, nonempty step name, unique within one parent execution. Use item IDs in loops. */
  readonly name: string;
}

/** A terminal agent outcome or invalid invocation, distinct from retriable infrastructure errors. */
export class WorkflowExecutionFailure extends Schema.TaggedError<WorkflowExecutionFailure>()(
  "WorkflowExecutionFailure",
  {
    reason: Schema.Literals([
      "invalid-name",
      "engine-mismatch",
      "failed",
      "aborted",
      "missing-output",
    ]),
    message: Schema.String,
    receipt: Schema.optionalKey(Receipt),
    failure: Schema.optionalKey(SettlementFailureDiagnostic),
  },
) {}
