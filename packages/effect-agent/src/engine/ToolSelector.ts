import { Context, type Effect } from "effect";
import type { AiError, Prompt } from "effect/unstable/ai";

import type { ModelProtocolError } from "../core/AgentError.ts";
import type { RunId, ThreadId, TurnId } from "../core/Identifiers.ts";
import type { Descriptor, Selection } from "../core/ToolExposure.ts";

/** One fresh model Turn. No hidden Tool metadata or handler capabilities are included. */
export interface Request {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly turn: number;
  /** Application input; decode with the application's Schema before inspecting it. */
  readonly input: unknown;
  /** Prepared prompt before compaction. Project only the state needed by an external evaluator. */
  readonly source: Prompt.Prompt;
  readonly catalogue: ReadonlyArray<Descriptor>;
  readonly selection: Selection | undefined;
}

/**
 * Provider-neutral ranking. Return unique catalogue IDs in preference order, or undefined
 * to retain the current selection. An empty array deliberately clears non-pinned exposure.
 * Every ID is validated before limits are applied; aliases select their owning native Tool.
 * Errors propagate unchanged. Use Effect recovery explicitly for an application fallback.
 */
export interface Hook<Error = never, Requirements = never> {
  readonly select: (
    request: Request,
  ) => Effect.Effect<ReadonlyArray<string> | undefined, Error, Requirements>;
  /** Maximum distinct selected native Tools, excluding additional engine pins; default 8, maximum 64. */
  readonly maxTools?: number | undefined;
  /** Refuse oversized catalogues before invoking select; default 1024, maximum 1024 entries. */
  readonly maxCandidates?: number | undefined;
  /** Complete UTF-8 JSON metadata budget; default 256 KiB, maximum 1 MiB. */
  readonly maxCatalogueBytes?: number | undefined;
}

/**
 * Optional host selector captured by durable runtimes, including absence. Capture provider
 * services when constructing its Layer. Per-run toolSelector overrides preserve their own E/R.
 * Selection is advisory exposure, never action authorization. Evaluation usage is separate from
 * the Run's language-model accounting; hosts own evaluator billing and request deadlines.
 */
export const RunToolSelector = Context.Reference<
  Hook<AiError.AiError | ModelProtocolError> | undefined
>("@effect-agent/engine/RunToolSelector", { defaultValue: () => undefined });
