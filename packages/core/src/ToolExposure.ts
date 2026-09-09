import { Context, Schema } from "effect";
import type { Tool } from "effect/unstable/ai";

/** Registered native names only. Empty selection deliberately clears non-pinned Tools. */
export const ToolNames = Schema.Array(
  Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
).check(
  Schema.isMaxLength(1_024),
  Schema.makeFilter((names) => new Set(names).size === names.length, {
    title: "Tool names are unique",
  }),
);

/** One run-scoped replacement, independent of search and of model-visible result text. */
export class Selection extends Schema.Class<Selection>("@effect-agent/core/ToolExposure/Selection")(
  {
    toolNames: ToolNames,
  },
) {}

/** Exact declarations used by one model request, with the selection that produced them. */
export class Snapshot extends Schema.Class<Snapshot>("@effect-agent/core/ToolExposure/Snapshot")({
  exposedToolNames: ToolNames,
  selection: Schema.optionalKey(Selection),
}) {}

export const Limits = Schema.Struct({
  maxTools: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
  maxSchemaBytes: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(4_194_304)),
});

/** Pure projection from a successful ordinary readonly Tool result; exceptions fail the Run. */
export interface FromTool<Parameters = unknown, Result = unknown> {
  readonly tool: string;
  readonly project: (input: {
    readonly parameters: Parameters;
    readonly result: Result;
  }) => ReadonlyArray<string>;
}

/** Omission preserves eager exposure. Selection changes take effect only after a complete batch. */
export interface Configuration {
  readonly initialToolNames?: ReadonlyArray<string> | undefined;
  readonly maxTools?: number | undefined;
  readonly maxSchemaBytes?: number | undefined;
  readonly fromTools?: ReadonlyArray<FromTool> | undefined;
}

/** Trusted grouping metadata; never inferred from a Tool's name. */
export const ToolNamespace = Context.Reference<string | undefined>(
  "@effect-agent/core/ToolExposure/ToolNamespace",
  { defaultValue: () => undefined },
);

/** An ordinary readonly Tool whose successful result has a `toolNames` field of native names. */
export const DiscoveryTool = Context.Reference<boolean>(
  "@effect-agent/core/ToolExposure/DiscoveryTool",
  { defaultValue: () => false },
);

/** Remains exposed under selection, subject to host visibility and inherited grants. */
export const PinnedTool = Context.Reference<boolean>("@effect-agent/core/ToolExposure/PinnedTool", {
  defaultValue: () => false,
});

/** Extra documentation candidates executed through their owning native Tool, such as Code Mode. */
export interface AdditionalCatalogEntry {
  readonly tool: Tool.Any;
  readonly namespace: string;
  readonly method: string;
}

export const AdditionalToolCatalog = Context.Reference<ReadonlyArray<AdditionalCatalogEntry>>(
  "@effect-agent/core/ToolExposure/AdditionalToolCatalog",
  { defaultValue: () => [] },
);

/** The owning Tool description embeds its complete additional catalogue. */
export const IncludesCatalogDocumentation = Context.Reference<boolean>(
  "@effect-agent/core/ToolExposure/IncludesCatalogDocumentation",
  { defaultValue: () => false },
);
