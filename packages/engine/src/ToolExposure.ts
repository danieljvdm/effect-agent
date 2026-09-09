import type { RunId, ThreadId } from "@effect-agent/core/Identifiers";
import { Context, type Effect } from "effect";
import type { Tool } from "effect/unstable/ai";

export type CatalogEntry =
  | {
      readonly kind: "native";
      readonly tool: Tool.Any;
      readonly namespace?: string | undefined;
      readonly nativeToolName: string;
    }
  | {
      readonly kind: "code-mode";
      readonly tool: Tool.Any;
      readonly namespace: string;
      readonly method: string;
      readonly nativeToolName: string;
    };

/** Engine-owned per-Turn snapshot. All candidates passed host visibility and inherited grants. */
export class CurrentToolCatalog extends Context.Service<
  CurrentToolCatalog,
  {
    readonly entries: ReadonlyArray<CatalogEntry>;
  }
>()("@effect-agent/engine/ToolExposure/CurrentToolCatalog") {}

/** Host visibility is checked before a discovery Handler may inspect candidate documentation. */
export interface VisibilityRequest {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turn: number;
  readonly input: unknown;
  /** Includes registered native names and explicitly advertised programmatic names. */
  readonly toolNames: ReadonlyArray<string>;
}

export interface VisibilityHook<E = never, R = never> {
  readonly visible: (request: VisibilityRequest) => Effect.Effect<ReadonlyArray<string>, E, R>;
}

/** Optional host policy captured by durable runtimes; action authorization remains independent. */
export class RunToolVisibility extends Context.Service<RunToolVisibility, VisibilityHook>()(
  "@effect-agent/engine/ToolExposure/RunToolVisibility",
) {}
