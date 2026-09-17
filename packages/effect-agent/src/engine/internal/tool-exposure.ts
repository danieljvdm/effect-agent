import { Context, Effect, Schema } from "effect";
import { Tool, type LanguageModel } from "effect/unstable/ai";

import type { AnyDefinition } from "../../core/Agent.ts";
import { ModelProtocolError } from "../../core/AgentError.ts";
import { utf8ByteLength } from "../../core/internal/utf8.ts";
import {
  getToolExecutionKind,
  isSubagentToolAllowed,
  type SubagentGrant,
} from "../../core/SubagentContract.ts";
import {
  AdditionalToolCatalog,
  Descriptor,
  IncludesCatalogDocumentation,
  DiscoveryTool,
  Limits,
  PinnedTool,
  Selection,
  Snapshot,
  ToolNames,
  ToolNamespace,
} from "../../core/ToolExposure.ts";
import { ContextRolloverTool } from "../ContextWindow.ts";
import { getToolExecutionClass } from "../DurableStep.ts";
import { RunToolVisibility, type CatalogEntry, type VisibilityRequest } from "../ToolExposure.ts";
import type * as ToolSelector from "../ToolSelector.ts";

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
      : yield* Schema.decodeEffect(ToolNames)(visible).pipe(
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

  const mandatory = Object.values(definition.toolkit.tools).filter(
    (tool) =>
      Context.get(tool.annotations, DiscoveryTool) ||
      Context.get(tool.annotations, ContextRolloverTool) ||
      (definition.completion?.required === true && definition.completion.tool === tool.name),
  );

  if (
    progressive &&
    mandatory.some((tool) => !native.some((candidate) => candidate.name === tool.name))
  ) {
    return yield* invalid("A mandatory Tool is excluded by host visibility or the inherited grant");
  }

  // Explicit pins affect selection, never eligibility. Hosts may disable a common action
  // without disabling the Run; protocol-required Tools still fail closed above.
  const pinned = native.filter(
    (tool) => Context.get(tool.annotations, PinnedTool) || mandatory.includes(tool),
  );

  if (selection !== undefined) yield* validateSelection(selection, definition, entries, false);
  const selected = selection === undefined ? undefined : new Set(selection.toolNames);

  const names = native
    .filter((tool) => selected === undefined || selected.has(tool.name) || pinned.includes(tool))
    .map((tool) => tool.name)
    .filter((name) => only === undefined || only.includes(name));

  if (progressive) {
    const limits = yield* Schema.decodeEffect(Limits)({
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

/** One stable identity for native Tools and each programmatic alias. */
export const catalogEntryId = (entry: CatalogEntry): string =>
  entry.kind === "native"
    ? `native:${entry.tool.name}`
    : `code-mode:${entry.nativeToolName}:${entry.namespace}.${entry.method}`;

export const describeCatalog = Effect.fnUntraced(function* (
  entries: ReadonlyArray<CatalogEntry>,
  namespaceDescriptions: Readonly<Record<string, string>> = {},
) {
  const seen = new Set<string>();
  const descriptors: Array<Descriptor> = [];

  for (const entry of entries) {
    const id = catalogEntryId(entry);

    if (seen.has(id))
      return yield* invalid("The eligible Tool catalogue contains duplicate identities");
    seen.add(id);
    const description = Tool.getDescription(entry.tool);

    const namespaceDescription =
      entry.namespace !== undefined && Object.hasOwn(namespaceDescriptions, entry.namespace)
        ? namespaceDescriptions[entry.namespace]
        : undefined;

    const descriptor = yield* Schema.decodeEffect(Descriptor)({
      id,
      kind: entry.kind,
      name: entry.tool.name,
      nativeToolName: entry.nativeToolName,
      ...(entry.namespace === undefined ? {} : { namespace: entry.namespace }),
      ...(entry.kind === "code-mode" ? { method: entry.method } : {}),
      ...(description === undefined
        ? {}
        : {
            description:
              description.length > 2_048 ? `${description.slice(0, 2_047)}…` : description,
          }),
      ...(namespaceDescription === undefined ? {} : { namespaceDescription }),
    }).pipe(
      Effect.mapError(() => invalid("The eligible Tool catalogue contains invalid metadata")),
    );

    descriptors.push(Object.freeze(descriptor));
  }

  return Object.freeze(descriptors.toSorted((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)));
});

const SelectorLimits = Schema.Struct({
  maxTools: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })),
  maxCandidates: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_024 })),
  maxCatalogueBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_048_576 })),
});

const SelectedIds = Schema.Array(Descriptor.fields.id).check(
  Schema.isMaxLength(1_024),
  Schema.isUnique(),
);

export const selectTools = Effect.fn("ToolSelector.select")(function* <E, R>(
  selector: ToolSelector.Hook<E, R>,
  request: Omit<ToolSelector.Request, "catalogue">,
  entries: ReadonlyArray<CatalogEntry>,
) {
  const limits = yield* Schema.decodeEffect(SelectorLimits)({
    maxTools: selector.maxTools ?? 8,
    maxCandidates: selector.maxCandidates ?? 1_024,
    maxCatalogueBytes: selector.maxCatalogueBytes ?? 262_144,
  }).pipe(Effect.mapError(() => invalid("Invalid Tool selector bounds")));

  if (entries.length > limits.maxCandidates)
    return yield* invalid("Tool selector catalogue exceeds its candidate bound");
  const catalogue = yield* describeCatalog(entries);

  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Descriptor)))(
    catalogue,
  ).pipe(Effect.mapError(() => invalid("Tool selector catalogue cannot be encoded")));

  if (utf8ByteLength(encoded) > limits.maxCatalogueBytes)
    return yield* invalid("Tool selector catalogue exceeds its byte bound");
  const ids = yield* Effect.scoped(selector.select(Object.freeze({ ...request, catalogue })));

  if (ids === undefined) return undefined;

  const checked = yield* Schema.decodeEffect(SelectedIds)(ids).pipe(
    Effect.mapError(() => invalid("Tool selector must return unique catalogue IDs")),
  );

  const byId = new Map(catalogue.map((entry) => [entry.id, entry.nativeToolName]));
  const names = new Set<string>();

  for (const id of checked) {
    const name = byId.get(id);

    if (name === undefined)
      return yield* invalid("Tool selector returned an ID outside the eligible catalogue");
    names.add(name);
  }

  return Selection.make({ toolNames: [...names].slice(0, limits.maxTools) });
});
