import type { DecisionModel, DecisionSchema } from "@effect-agent/ai-decision";
import { Effect, Schema, type Scope } from "effect";
import { AiError, Tool, Toolkit } from "effect/unstable/ai";

import { utf8ByteLength } from "../core/internal/utf8.ts";
import { Descriptor, DiscoveryTool, PinnedTool } from "../core/ToolExposure.ts";
import { ToolExecutionClass } from "../engine/DurableStep.ts";
import { catalogEntryId as entryId, describeCatalog } from "../engine/internal/tool-exposure.ts";
import { CurrentToolCatalog, type CatalogEntry } from "../engine/ToolExposure.ts";
import { readDecisionConfig } from "./DecisionToolSelectorConfig.ts";
import { rankToolRelevance } from "./internal/tool-relevance.ts";

export { DecisionConfig, defaultDecisionConfig } from "./DecisionToolSelectorConfig.ts";

const Name = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const Namespace = Schema.NonEmptyString.check(Schema.isMaxLength(128));
const NamespaceDescription = Schema.String.check(Schema.isMaxLength(512));
const EntryId = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));

/** Search text is bounded; the search policy defines matching. Namespace selects one exact group. */
export const Parameters = Schema.Struct({
  query: Schema.NonEmptyString.check(Schema.isMaxLength(512), Schema.isPattern(/\S/)),
  namespace: Schema.optionalKey(Namespace),
});

export { Descriptor } from "../core/ToolExposure.ts";

/**
 * Selected metadata and canonical encoded Tool Schemas. Provider-specific model schema
 * transformations do not change the wire values documented for application handlers.
 */
export class Match extends Schema.Class<Match>("@effect-agent/capabilities/ToolDiscovery/Match")({
  ...Descriptor.fields,
  parameters: Schema.Json,
  success: Schema.Json,
}) {}

/**
 * Only documented nativeToolName values activate schemas; Code Mode aliases activate their
 * owning Tool. A byte-limited result is successful, including when no matches fit: an empty
 * toolNames array clears the non-pinned selection under the engine's replacement contract.
 */
export const Result = Schema.Struct({
  toolNames: Schema.Array(Name).check(Schema.isMaxLength(64), Schema.isUnique()),
  matches: Schema.Array(Match).check(Schema.isMaxLength(64)),
  /** Present when the byte budget omits complete matches, with guidance for continuing. */
  notice: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(192))),
});

/** Invalid custom results and unsupported documentation remain typed failures. */
export class ToolDiscoveryError extends Schema.TaggedError<ToolDiscoveryError>()(
  "ToolDiscoveryError",
  {
    reason: Schema.Literals([
      "invalid-catalogue",
      "invalid-matches",
      "unknown-match",
      "invalid-schema",
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

const resultByteLength = Effect.fnUntraced(function* (result: typeof Result.Type) {
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Result))(result).pipe(
    Effect.mapError(() =>
      ToolDiscoveryError.make({
        reason: "invalid-schema",
        message: "Discovery documentation could not be encoded",
      }),
    ),
  );

  return utf8ByteLength(encoded);
});

export interface Options<Failure extends Schema.Top = typeof Schema.Never, Requirements = never> {
  /** Optional generic instructions. Namespace names and hints are never appended here. */
  readonly description?: string | undefined;
  /** Maximum returned catalogue entries, default 8 and at most 64. */
  readonly maxResults?: number | undefined;
  /**
   * Complete JSON-encoded result budget, including any notice: default 32768, minimum 256,
   * maximum 262144 UTF-8 bytes. Within the first maxResults candidates, retain whole matches
   * in rank order when they fit; skip oversized matches and try later candidates. Overflow
   * returns a successful result with an actionable notice, never partial schemas.
   */
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

          const catalogue = yield* describeCatalog(entries, bounds.namespaceDescriptions).pipe(
            Effect.mapError(() =>
              ToolDiscoveryError.make({
                reason: "invalid-catalogue",
                message: "The visible catalogue contains invalid metadata or duplicate identities",
              }),
            ),
          );

          const byId = new Map(entries.map((entry) => [entryId(entry), entry]));

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

          if ((yield* resultByteLength(result)) <= bounds.maxResultBytes) return result;

          // Validate every selected schema before packing. A byte limit must not mask an
          // invalid catalogue/schema. The empty notice envelope fits the minimum 256 bytes.
          let bounded: typeof Result.Type = {
            toolNames: [],
            matches: [],
            notice:
              "Result byte limit reached. Narrow the search or namespace; if one tool still cannot fit, ask the host to increase maxResultBytes.",
          };

          for (const match of matches) {
            const candidates = [...bounded.matches, match];

            const candidate = {
              ...bounded,
              toolNames: [...new Set(candidates.map((entry) => entry.nativeToolName))],
              matches: candidates,
            };

            if ((yield* resultByteLength(candidate)) <= bounds.maxResultBytes) bounded = candidate;
          }

          return bounded;
        }),
      });
    }),
  );

  return Object.freeze({ tool, toolkit, handlers });
};

/** Options for semantic discovery. The failure Schema describes onEvaluation failures. */
export interface DecisionOptions<
  Failure extends Schema.Top = typeof Schema.Never,
  Requirements = never,
> extends Omit<Options<Failure, Requirements>, "search"> {
  /** Separate evaluator usage, without query or answers. Failures stop discovery; no implicit fallback. */
  readonly onEvaluation?:
    | ((
        result: Pick<DecisionSchema.EvaluateResponse, "provider" | "model" | "usage">,
      ) => Effect.Effect<void, Failure["Type"], Requirements>)
    | undefined;
}

/**
 * Read the shared DecisionConfig service and construct semantic discover_tools. Provide
 * configuration to this construction Effect; the resulting Tool captures the validated settings.
 * The configured prompt and criteria accompany the bounded discovery query, optional namespace,
 * and eligible metadata; no conversation prompt or Thread history is implicitly projected.
 * The discovery Tool excludes itself from evaluation.
 * One batch asks an independent probability question per candidate, with stable ID tie-breaking.
 * Empty catalogues skip I/O. Result/schema budgets, pins and activation use make's normal contract.
 *
 * Provide DecisionModel and observer services to handlers at Layer construction. AiError and the
 * declared observer failures remain typed; defects, timeout and interruption propagate normally.
 * Hosts own provider deadlines and billing. Evaluation grants no additional Tool authority.
 */
export const fromDecisionModel = Effect.fnUntraced(function* <
  Failure extends Schema.Top = typeof Schema.Never,
  Requirements = never,
>(options: DecisionOptions<Failure, Requirements> = {}) {
  const bounds = yield* readDecisionConfig;

  const observerFailure: Schema.Codec<
    Failure["Type"],
    Failure["Encoded"],
    Failure["DecodingServices"],
    Failure["EncodingServices"]
  > = options.failure ?? Schema.Never;

  const invalid = (description: string) =>
    AiError.AiError.make({
      module: "ToolDiscovery",
      method: "search",
      reason: AiError.InvalidRequestError.make({ description }),
    });

  const failure = Schema.Union([AiError.AiError, observerFailure]);

  return make<typeof failure, Requirements | DecisionModel.DecisionModel>({
    description:
      options.description ??
      "Find available tools by describing the capability you need, with an optional exact namespace. Semantic relevance determines matches; exact keywords are not required. Returned native tools become available next turn. Code Mode matches document namespace methods and select their owning execution tool.",
    maxResults: options.maxResults,
    maxResultBytes: options.maxResultBytes,
    namespaceDescriptions: options.namespaceDescriptions,
    failure,
    search: Effect.fn("ToolDiscovery.decisions")(function* (request, eligible) {
      const catalogue = eligible.filter(
        (candidate) => candidate.nativeToolName !== "discover_tools",
      );

      if (catalogue.length === 0) return [];
      if (catalogue.length > bounds.maxCandidates)
        return yield* invalid("Tool discovery catalogue exceeds its candidate bound");

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Descriptor)))(
        catalogue,
      ).pipe(Effect.mapError(() => invalid("Invalid Tool discovery catalogue")));

      if (utf8ByteLength(encoded) > bounds.maxCatalogueBytes)
        return yield* invalid("Tool discovery catalogue exceeds its byte bound");

      const result = yield* rankToolRelevance({
        prompt: bounds.prompt,
        criteria: bounds.criteria,
        state: {
          query: request.query,
          ...(request.namespace === undefined ? {} : { namespace: request.namespace }),
        },
        catalogue,
        minimumRelevance: bounds.minimumRelevance,
        maxStateBytes: bounds.maxStateBytes,
        module: "ToolDiscovery",
      });

      if (options.onEvaluation !== undefined) yield* options.onEvaluation(result.evaluation);

      return result.ids;
    }),
  });
});
