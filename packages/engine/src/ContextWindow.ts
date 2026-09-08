import { RunId, ThreadId } from "@effect-agent/core/Identifiers";
import { Context, type Effect, Encoding, Schema } from "effect";

/** Model-authored continuation state; fuller notes belong in an application-owned memory store. */
export const ContextHandoff = Schema.NonEmptyString.check(
  Schema.isMaxLength(20_000),
  Schema.isPattern(/\S/),
  Schema.makeFilter((text) => Encoding.encodeHex(JSON.stringify(text)).length / 2 <= 32_768, {
    expected: "at most 32768 JSON-encoded UTF-8 bytes",
  }),
);

/** Successful output of an explicitly designated context-window Tool. */
export const ContextRolloverRequest = Schema.Struct({
  handoff: Schema.optionalKey(ContextHandoff),
});

export type ContextRolloverRequest = typeof ContextRolloverRequest.Type;

/**
 * Host-selected exclusive source-message boundary, applied at the next safe model seam.
 * Omit through to reset the prefix before the protected current instructions/input. The
 * engine resolves that prefix after preparation; an empty prior prefix needs no new window.
 */
export const ContextRolloverSelection = Schema.Struct({
  ...ContextRolloverRequest.fields,
  through: Schema.optionalKey(Schema.Natural),
});

export type ContextRolloverSelection = typeof ContextRolloverSelection.Type;

/**
 * Bounds resolved alongside the native model for one Turn. Output reserve must match the
 * selected provider configuration. Uncounted overhead includes only provider framing or image
 * token costs absent from the prompt estimate; prompt text, output contracts, and native Tool
 * schemas are counted by the engine. These are live-context bounds, not cumulative Run budgets.
 */
export class ModelCallContext extends Schema.Class<ModelCallContext>("ModelCallContext")({
  contextCapacity: Schema.Int.check(Schema.isGreaterThan(0)),
  maxInputTokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  outputReserveTokens: Schema.Natural,
  uncountedOverheadTokens: Schema.Natural,
}) {}

/**
 * Definition-owned control annotation. A successful singleton application Tool carrying this
 * annotation returns ContextRolloverRequest. The engine applies it at the next Turn seam,
 * including after durable Tool-result replay. A Tool name alone never grants this authority.
 */
export const ContextRolloverTool = Context.Reference<boolean>(
  "@effect-agent/engine/ContextRolloverTool",
  { defaultValue: () => false },
);

/** Best available live-context estimate, independent of cumulative Run budgets. */
export class ContextWindowStatus extends Schema.Class<ContextWindowStatus>("ContextWindowStatus")({
  threadId: ThreadId,
  runId: RunId,
  windowId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  estimatedTokens: Schema.Natural,
  contextTokenLimit: Schema.NullOr(Schema.Natural),
  remainingTokens: Schema.NullOr(Schema.Natural),
}) {}

/** Engine-owned identity and context accounting for the current Run. */
export class ContextWindow extends Context.Service<
  ContextWindow,
  {
    readonly status: Effect.Effect<ContextWindowStatus>;
  }
>()("@effect-agent/engine/ContextWindow") {}
