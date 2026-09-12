import { DiscoveryTool, PinnedTool } from "@effect-agent/core/ToolExposure";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { CurrentToolCatalog, type CatalogEntry } from "@effect-agent/engine/ToolExposure";
import { Effect, Schema, type Scope } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { utf8ByteLength } from "./internal/utf8.ts";

const Name = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const Namespace = Schema.NonEmptyString.check(Schema.isMaxLength(128));
const Description = Schema.String.check(Schema.isMaxLength(2_048));
const NamespaceDescription = Schema.String.check(Schema.isMaxLength(512));
const EntryId = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));

/** Search text is bounded and literal; namespace selects one exact host-declared group. */
export const Parameters = Schema.Struct({
  query: Schema.NonEmptyString.check(Schema.isMaxLength(512), Schema.isPattern(/\S/)),
  namespace: Schema.optionalKey(Namespace),
});

/** Metadata given to custom search only after host visibility and inherited grants are applied. */
export class Descriptor extends Schema.Class<Descriptor>(
  "@effect-agent/capabilities/ToolDiscovery/Descriptor",
)({
  id: EntryId,
  kind: Schema.Literals(["native", "code-mode"]),
  name: Name,
  nativeToolName: Name,
  namespace: Schema.optionalKey(Namespace),
  method: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(128))),
  description: Schema.optionalKey(Description),
  namespaceDescription: Schema.optionalKey(NamespaceDescription),
}) {}

/**
 * Selected metadata and canonical encoded Tool Schemas. Provider-specific model schema
 * transformations do not change the wire values documented for application handlers.
 */
export class Match extends Schema.Class<Match>("@effect-agent/capabilities/ToolDiscovery/Match")({
  ...Descriptor.fields,
  parameters: Schema.Json,
  success: Schema.Json,
}) {}

/** Only nativeToolName values activate schemas; Code Mode aliases activate their owning Tool. */
export const Result = Schema.Struct({
  toolNames: Schema.Array(Name).check(Schema.isMaxLength(64), Schema.isUnique()),
  matches: Schema.Array(Match).check(Schema.isMaxLength(64)),
});

/** Invalid custom results, unsupported documentation, and bounded output failures remain typed. */
export class ToolDiscoveryError extends Schema.TaggedError<ToolDiscoveryError>()(
  "ToolDiscoveryError",
  {
    reason: Schema.Literals([
      "invalid-catalogue",
      "invalid-matches",
      "unknown-match",
      "invalid-schema",
      "limit-exceeded",
    ]),
    message: Schema.String.check(Schema.isMaxLength(1_024)),
  },
) {}

const MatchIds = Schema.Array(EntryId).check(Schema.isMaxLength(1_024), Schema.isUnique());

const Bounds = Schema.Struct({
  maxResults: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })),
  maxResultBytes: Schema.Int.check(Schema.isBetween({ minimum: 256, maximum: 256 * 1024 })),
  namespaceDescriptions: Schema.Record(Namespace, NamespaceDescription),
});

export interface Options<Failure extends Schema.Top = typeof Schema.Never, Requirements = never> {
  /** Optional generic instructions. Namespace names and hints are never appended here. */
  readonly description?: string | undefined;
  /** Maximum returned catalogue entries, default 8 and at most 64. */
  readonly maxResults?: number | undefined;
  /** Complete JSON-encoded result budget, default 32768 and at most 262144 UTF-8 bytes. */
  readonly maxResultBytes?: number | undefined;
  /** Short category hints returned only for namespaces present in the visible catalogue. */
  readonly namespaceDescriptions?: Readonly<Record<string, string>> | undefined;
  /** Schema of the custom search's expected failures; defaults to Schema.Never. */
  readonly failure?: Failure | undefined;
  /**
   * Rank unique Descriptor.id values, never tool names. Native tools and each Code Mode alias
   * have distinct identities. Every returned id is checked before the result limit is applied.
   * The catalogue is already filtered by the optional exact namespace and current authority.
   * Services are captured with the handler Layer; temporary resources close after each search.
   */
  readonly search?:
    | ((
        request: typeof Parameters.Type,
        catalogue: ReadonlyArray<Descriptor>,
      ) => Effect.Effect<ReadonlyArray<string>, Failure["Type"], Requirements>)
    | undefined;
}

const entryId = (entry: CatalogEntry): string =>
  entry.kind === "native"
    ? `native:${entry.tool.name}`
    : `code-mode:${entry.nativeToolName}:${entry.namespace}.${entry.method}`;

const compare = (left: string, right: string): number => (left < right ? -1 : left > right ? 1 : 0);

const defaultSearch = (
  request: typeof Parameters.Type,
  catalogue: ReadonlyArray<Descriptor>,
  entries: ReadonlyMap<string, CatalogEntry>,
): ReadonlyArray<string> => {
  const terms = request.query.trim().toLowerCase().split(/\s+/);

  return catalogue
    .filter((entry) => {
      const original = entries.get(entry.id);

      if (original === undefined) return false;

      const text = [
        entry.name,
        entry.namespace ?? "",
        entry.method ?? "",
        Tool.getDescription(original.tool) ?? "",
        entry.namespaceDescription ?? "",
      ]
        .join(" ")
        .toLowerCase();

      return terms.every((term) => text.includes(term));
    })
    .map((entry) => entry.id);
};

/**
 * Build one native discover_tools Tool and its singleton Toolkit/handler Layer. Include the
 * Tool beside the application's existing native Tools. The engine owns catalogue authority
 * and applies successful native selections after a complete Tool batch; the handler mutates
 * no exposure state. Default search matches all case-insensitive whitespace-separated terms
 * against names, descriptions, methods and namespace hints, ordered by catalogue id.
 */
export const make = <Failure extends Schema.Top = typeof Schema.Never, Requirements = never>(
  options: Options<Failure, Requirements> = {},
) => {
  const bounds = Schema.decodeSync(Bounds)({
    maxResults: options.maxResults ?? 8,
    maxResultBytes: options.maxResultBytes ?? 32 * 1024,
    namespaceDescriptions: options.namespaceDescriptions ?? {},
  });

  const search = options.search;

  const declaredFailure: Schema.Codec<
    Failure["Type"],
    Failure["Encoded"],
    Failure["DecodingServices"],
    Failure["EncodingServices"]
  > = options.failure ?? Schema.Never;

  const tool = Tool.make("discover_tools", {
    description:
      options.description ??
      "Find available tools using short search terms and an optional exact namespace. All terms must match a name, description or namespace hint, ignoring case. Returned native tools become available next turn. Code Mode matches document namespace methods and select their owning execution tool.",
    parameters: Parameters,
    success: Result,
    failure: Schema.Union([declaredFailure, ToolDiscoveryError]),
    failureMode: "error",
    dependencies: [CurrentToolCatalog],
  })
    .annotate(DiscoveryTool, true)
    .annotate(PinnedTool, true)
    .annotate(Tool.Readonly, true)
    .annotate(ToolExecutionClass, "readonly");

  // Restore the known singleton name; native ToolsByName leaves its conditional deferred
  // over the generic failure Schema. This assertion changes no Schema or runtime value.
  const toolkit = Toolkit.make(tool) as unknown as Toolkit.Toolkit<{
    readonly discover_tools: typeof tool;
  }>;

  const handlers = toolkit.toLayer(
    Effect.gen(function* () {
      const searchServices = yield* Effect.context<Exclude<Requirements, Scope.Scope>>();

      return toolkit.of({
        discover_tools: Effect.fn("ToolDiscovery.discover_tools")(function* (request) {
          const snapshot = yield* CurrentToolCatalog;

          const entries = snapshot.entries
            .filter(
              (entry) => request.namespace === undefined || entry.namespace === request.namespace,
            )
            .toSorted((left, right) => compare(entryId(left), entryId(right)));

          const catalogue: Array<Descriptor> = [];
          const byId = new Map<string, CatalogEntry>();

          for (const entry of entries) {
            const id = entryId(entry);

            if (byId.has(id)) {
              return yield* ToolDiscoveryError.make({
                reason: "invalid-catalogue",
                message: "The visible catalogue contains duplicate entry identities",
              });
            }
            const description = Tool.getDescription(entry.tool);

            const namespaceDescription =
              entry.namespace === undefined ||
              !Object.hasOwn(bounds.namespaceDescriptions, entry.namespace)
                ? undefined
                : bounds.namespaceDescriptions[entry.namespace];

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
              Effect.mapError(() =>
                ToolDiscoveryError.make({
                  reason: "invalid-catalogue",
                  message: "The visible catalogue contains invalid metadata",
                }),
              ),
            );

            catalogue.push(Object.freeze(descriptor));
            byId.set(id, entry);
          }

          const ids =
            search === undefined
              ? defaultSearch(request, catalogue, byId)
              : yield* Effect.scoped(search(request, Object.freeze(catalogue))).pipe(
                  Effect.provideContext(searchServices),
                );

          const checkedIds = yield* Schema.decodeEffect(MatchIds)(ids).pipe(
            Effect.mapError(() =>
              ToolDiscoveryError.make({
                reason: "invalid-matches",
                message: "Search must return at most 1024 unique catalogue entry ids",
              }),
            ),
          );

          if (checkedIds.some((id) => !byId.has(id))) {
            return yield* ToolDiscoveryError.make({
              reason: "unknown-match",
              message: "Search returned an entry outside the visible catalogue",
            });
          }

          const descriptors = new Map(catalogue.map((descriptor) => [descriptor.id, descriptor]));
          const matches: Array<Match> = [];

          for (const id of checkedIds.slice(0, bounds.maxResults)) {
            const entry = byId.get(id);
            const descriptor = descriptors.get(id);

            if (entry === undefined || descriptor === undefined) {
              return yield* ToolDiscoveryError.make({
                reason: "unknown-match",
                message: "Search returned an entry outside the visible catalogue",
              });
            }
            if (Tool.isProviderDefined(entry.tool)) {
              return yield* ToolDiscoveryError.make({
                reason: "invalid-schema",
                message: "Provider-defined Tools do not expose application handler documentation",
              });
            }

            const schemas = yield* Effect.try({
              try: () => ({
                parameters: Tool.getJsonSchema(entry.tool),
                success: Tool.getJsonSchemaFromSchema(entry.tool.successSchema),
              }),
              catch: () =>
                ToolDiscoveryError.make({
                  reason: "invalid-schema",
                  message: "A selected Tool cannot be documented as JSON Schema",
                }),
            });

            const match = yield* Schema.decodeUnknownEffect(Match)({
              ...descriptor,
              ...schemas,
            }).pipe(
              Effect.mapError(() =>
                ToolDiscoveryError.make({
                  reason: "invalid-schema",
                  message: "A selected Tool's documentation is outside the JSON surface",
                }),
              ),
            );

            matches.push(match);
          }

          const result = {
            toolNames: [...new Set(matches.map((match) => match.nativeToolName))],
            matches,
          };

          const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Result))(result).pipe(
            Effect.mapError(() =>
              ToolDiscoveryError.make({
                reason: "invalid-schema",
                message: "Discovery documentation could not be encoded",
              }),
            ),
          );

          if (utf8ByteLength(encoded) > bounds.maxResultBytes) {
            return yield* ToolDiscoveryError.make({
              reason: "limit-exceeded",
              message: "Selected documentation exceeds the result byte limit; narrow the search",
            });
          }

          return result;
        }),
      });
    }),
  );

  return Object.freeze({ tool, toolkit, handlers });
};
