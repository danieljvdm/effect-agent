import type { AnyDefinition } from "@effect-agent/core/Agent";
import { ModelProtocolError } from "@effect-agent/core/AgentError";
import {
  getToolExecutionKind,
  isSubagentToolAllowed,
  type SubagentGrant,
} from "@effect-agent/core/SubagentContract";
import {
  AdditionalToolCatalog,
  IncludesCatalogDocumentation,
  DiscoveryTool,
  Limits,
  PinnedTool,
  Selection,
  Snapshot,
  ToolNames,
  ToolNamespace,
} from "@effect-agent/core/ToolExposure";
import { Context, Effect, Schema } from "effect";
import { Tool, type LanguageModel } from "effect/unstable/ai";

import { ContextRolloverTool } from "../ContextWindow.ts";
import { getToolExecutionClass } from "../DurableStep.ts";
import { RunToolVisibility, type CatalogEntry, type VisibilityRequest } from "../ToolExposure.ts";
import { utf8ByteLength } from "./bounded-value.ts";

const invalid = (message: string) => ModelProtocolError.make({ message });

export const decodeSelection = (value: unknown) =>
  Schema.decodeUnknownEffect(Selection)(value).pipe(
    Effect.mapError(() => invalid("Invalid Tool exposure selection")),
  );

export const decodeSnapshot = (value: unknown) =>
  Schema.decodeUnknownEffect(Snapshot)(value).pipe(
    Effect.mapError(() => invalid("Invalid recorded Tool exposure snapshot")),
    Effect.map((snapshot) =>
      Object.freeze(
        Snapshot.make({
          exposedToolNames: Object.freeze([...snapshot.exposedToolNames]),
          ...(snapshot.selection === undefined
            ? {}
            : {
                selection: Object.freeze(
                  Selection.make({ toolNames: Object.freeze([...snapshot.selection.toolNames]) }),
                ),
              }),
        }),
      ),
    ),
  );

export const eligibleCatalog = Effect.fn("ToolExposure.eligibleCatalog")(function* (
  definition: AnyDefinition,
  request: Omit<VisibilityRequest, "toolNames">,
  grant: SubagentGrant | undefined,
  depth: number,
) {
  const visibility = yield* RunToolVisibility;
  const entries: Array<CatalogEntry> = [];

  for (const tool of Object.values(definition.toolkit.tools)) {
    if (
      Context.get(tool.annotations, DiscoveryTool) &&
      (getToolExecutionKind(tool.annotations) !== "ordinary" ||
        getToolExecutionClass(tool) !== "readonly")
    )
      return yield* invalid("Discovery requires ordinary readonly Tools");
    const namespace = Context.get(tool.annotations, ToolNamespace);

    entries.push({
      kind: "native",
      tool,
      nativeToolName: tool.name,
      ...(namespace === undefined ? {} : { namespace }),
    });
    for (const extra of Context.get(tool.annotations, AdditionalToolCatalog)) {
      entries.push({ kind: "code-mode", ...extra, nativeToolName: tool.name });
    }
  }
  const names = [...new Set(entries.map((entry) => entry.tool.name))];

  const visible =
    visibility === undefined ? names : yield* visibility.visible({ ...request, toolNames: names });

  const checked =
    visibility === undefined
      ? visible
      : yield* Schema.decodeUnknownEffect(ToolNames)(visible).pipe(
          Effect.mapError(() => invalid("Host Tool visibility returned invalid names")),
        );

  if (checked.some((name) => !names.includes(name)))
    return yield* invalid("Host Tool visibility returned an unregistered name");
  const allowed = new Set(checked);

  const permits = (tool: Tool.Any) =>
    allowed.has(tool.name) && isSubagentToolAllowed(grant, depth, tool.name, tool.annotations);

  for (const outer of Object.values(definition.toolkit.tools)) {
    if (
      permits(outer) &&
      Context.get(outer.annotations, IncludesCatalogDocumentation) &&
      Context.get(outer.annotations, AdditionalToolCatalog).some((entry) => !permits(entry.tool))
    ) {
      return yield* invalid(
        "A Tool description includes hidden catalogue declarations; configure includeDeclarations: false",
      );
    }
  }

  return Object.freeze(
    entries
      .filter((entry) => {
        const outer = definition.toolkit.tools[entry.nativeToolName];

        return outer !== undefined && permits(outer) && permits(entry.tool);
      })
      .map((entry) => Object.freeze(entry)),
  );
});

export const validateSelection = Effect.fn("ToolExposure.validateSelection")(function* (
  value: unknown,
  definition: AnyDefinition,
  entries: ReadonlyArray<CatalogEntry>,
  requireEligible = true,
) {
  const selection = yield* decodeSelection(value);

  const eligible = new Set(
    entries.filter((entry) => entry.kind === "native").map((entry) => entry.nativeToolName),
  );

  if (
    selection.toolNames.some(
      (name) =>
        !Object.hasOwn(definition.toolkit.tools, name) || (requireEligible && !eligible.has(name)),
    )
  ) {
    return yield* invalid("Tool selection names a Tool outside the eligible registered toolkit");
  }

  return Selection.make({ toolNames: Object.freeze([...selection.toolNames]) });
});

export const exposureSnapshot = Effect.fn("ToolExposure.exposureSnapshot")(function* (
  definition: AnyDefinition,
  selection: Selection | undefined,
  entries: ReadonlyArray<CatalogEntry>,
  transformer?: LanguageModel.CodecTransformer,
  only?: ReadonlyArray<string>,
) {
  const native = entries.filter((entry) => entry.kind === "native").map((entry) => entry.tool);
  const progressive = definition.toolExposure !== undefined || selection !== undefined;

  const pinned = Object.values(definition.toolkit.tools).filter(
    (tool) =>
      Context.get(tool.annotations, PinnedTool) ||
      Context.get(tool.annotations, DiscoveryTool) ||
      Context.get(tool.annotations, ContextRolloverTool) ||
      (definition.completion?.required === true && definition.completion.tool === tool.name),
  );

  if (
    progressive &&
    pinned.some((tool) => !native.some((candidate) => candidate.name === tool.name))
  ) {
    return yield* invalid("A pinned Tool is excluded by host visibility or the inherited grant");
  }
  if (selection !== undefined) yield* validateSelection(selection, definition, entries, false);
  const selected = selection === undefined ? undefined : new Set(selection.toolNames);

  const names = native
    .filter((tool) => selected === undefined || selected.has(tool.name) || pinned.includes(tool))
    .map((tool) => tool.name)
    .filter((name) => only === undefined || only.includes(name));

  if (progressive) {
    const limits = yield* Schema.decodeUnknownEffect(Limits)({
      maxTools: definition.toolExposure?.maxTools ?? 64,
      maxSchemaBytes: definition.toolExposure?.maxSchemaBytes ?? 262_144,
    }).pipe(Effect.mapError(() => invalid("Invalid Tool exposure bounds")));

    if (names.length > limits.maxTools)
      return yield* invalid("Tool exposure exceeds the configured Tool count bound");

    const bytes = yield* Effect.try({
      try: () =>
        utf8ByteLength(
          JSON.stringify(
            names.map((name) => {
              const tool = definition.toolkit.tools[name];

              if (tool === undefined) throw new Error("Selected Tool is unavailable");

              return Tool.isProviderDefined(tool)
                ? { type: tool.providerName, args: tool.args }
                : {
                    name,
                    description: Tool.getDescription(tool),
                    parameters: Tool.getJsonSchema(tool, { transformer }),
                  };
            }),
          ),
        ),
      catch: () => invalid("A selected Tool schema cannot be rendered"),
    });

    if (bytes > limits.maxSchemaBytes)
      return yield* invalid("Tool exposure exceeds the configured schema byte bound");
  }

  return yield* decodeSnapshot({
    exposedToolNames: names,
    ...(selection === undefined ? {} : { selection }),
  });
});
