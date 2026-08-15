import type {
  AgentId,
  ConversationId,
  DelegationDepth,
  DelegationId,
  RunId,
  ToolCallId,
} from "@effect-agent/core";
import { Context, Schema } from "effect";
import type { Effect } from "effect";

/**
 * Shared identity carried by every pre-base Subagent event payload. The
 * emitting Tool handler supplies delegation lineage and its own Tool Call
 * identity; the engine stamps the remaining fields it is authoritative for
 * (`eventVersion`, `runId`, `conversationId`, `agentId`, `sequence`,
 * `timestamp`, and the emitting batch's `turnId`) when the payload is woven
 * into the parent stream.
 */
export interface SubagentEventBasePayload {
  readonly toolCallId: ToolCallId;
  readonly delegationId: DelegationId;
  readonly childConversationId: ConversationId;
  readonly childRunId: RunId;
  readonly targetAgentId: AgentId;
  readonly depth: DelegationDepth;
}

/** Pre-base payload for the core `SubagentRequested` event. */
export interface SubagentRequestedPayload extends SubagentEventBasePayload {
  readonly _tag: "SubagentRequested";
}

/** Pre-base payload for the core `SubagentStarted` event. */
export interface SubagentStartedPayload extends SubagentEventBasePayload {
  readonly _tag: "SubagentStarted";
}

/** Pre-base payload for the core `SubagentProgress` event; `summary` is at most 4096 characters. */
export interface SubagentProgressPayload extends SubagentEventBasePayload {
  readonly _tag: "SubagentProgress";
  readonly summary: string;
}

/** Pre-base payload for the core `SubagentCompleted` event; `turns` is a positive integer. */
export interface SubagentCompletedPayload extends SubagentEventBasePayload {
  readonly _tag: "SubagentCompleted";
  readonly turns: number;
  readonly finishReason: "completed" | "model-stop" | "budget-exhausted";
}

/** Pre-base payload for the core `SubagentFailed` event; `message` is at most 4096 characters. */
export interface SubagentFailedPayload extends SubagentEventBasePayload {
  readonly _tag: "SubagentFailed";
  readonly errorTag: string;
  readonly message: string;
}

/** Pre-base payload for the core `SubagentInterrupted` event; `reason` is at most 4096 characters. */
export interface SubagentInterruptedPayload extends SubagentEventBasePayload {
  readonly _tag: "SubagentInterrupted";
  readonly reason: string;
}

/** Pre-base payload for the core `SubagentJoined` event. */
export interface SubagentJoinedPayload extends SubagentEventBasePayload {
  readonly _tag: "SubagentJoined";
}

/**
 * Discriminated union of pre-base Subagent lifecycle event payloads accepted
 * by `RunEventSink.emit`. Each variant is the corresponding core `RunEvent`
 * class minus the base fields the engine stamps. The stamped event is
 * constructed through the core Schema class, so core field checks (bounded
 * text, positive turns) still apply; violating them is a defect of the
 * emitting handler, not an expected failure.
 */
export type SubagentEventPayload =
  | SubagentRequestedPayload
  | SubagentStartedPayload
  | SubagentProgressPayload
  | SubagentCompletedPayload
  | SubagentFailedPayload
  | SubagentInterruptedPayload
  | SubagentJoinedPayload;

/** A handler emitted a Subagent event after its Tool batch had already settled. */
export class RunEventSinkClosedError extends Schema.TaggedError<RunEventSinkClosedError>()(
  "RunEventSinkClosedError",
  {
    message: Schema.String,
  },
) {}

/**
 * Service value accepted by Tool handlers to emit first-class Subagent
 * lifecycle events into the parent Run's semantic stream.
 *
 * Emission never blocks the emitting handler: the sink is backed by an
 * unbounded queue drained by the Run's own event stream, matching the Run's
 * existing buffering model (the Run stream is the only consumer, so no
 * external observer can backpressure the Tool batch). Emitting after the
 * owning Tool batch has settled — or outside any Tool batch — fails closed
 * with `RunEventSinkClosedError`.
 */
export interface RunEventSinkService {
  readonly emit: (payload: SubagentEventPayload) => Effect.Effect<void, RunEventSinkClosedError>;
}

/**
 * Engine-owned seam through which Tool handlers emit Subagent lifecycle
 * events onto the parent Run stream.
 *
 * The engine provides this service locally to every Tool batch, bound to that
 * batch's Run identity and Turn; it is never satisfied from an application
 * Layer and is excluded from the runtime's public requirements.
 */
export class RunEventSink extends Context.Service<RunEventSink, RunEventSinkService>()(
  "@effect-agent/engine/RunEventSink",
) {}
