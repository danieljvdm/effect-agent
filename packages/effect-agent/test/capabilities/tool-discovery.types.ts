import * as ToolDiscovery from "@yielded/agent/tool-discovery";
import type { CurrentToolCatalog } from "@yielded/agent/tool-exposure";
import type { Layer } from "effect";
import { Context, Effect, Schema } from "effect";
import type { Tool } from "effect/ai";
import type { expectTypeOf as ExpectTypeOf } from "vite-plus/test";

class SearchError extends Schema.TaggedError<SearchError>()("SearchError", {
  reason: Schema.String,
}) {}
class SearchIndex extends Context.Service<SearchIndex, string>()(
  "tool-discovery-test/SearchIndex",
) {}

const typed = ToolDiscovery.make({
  failure: SearchError,
  search: (_request, catalogue) =>
    Effect.gen(function* () {
      yield* SearchIndex;
      yield* Effect.addFinalizer(() => Effect.void);
      if (catalogue.length === 0) return yield* SearchError.make({ reason: "empty" });

      return catalogue.map((entry) => entry.id);
    }),
});

export const verifyRetainsSearchErrorsAndLayerRequirementsWithoutCapturingTheEngineCatalogue =
  () => {
    expectTypeOf<Layer.Services<typeof typed.handlers>>().toEqualTypeOf<SearchIndex>();
    expectTypeOf<
      Extract<Tool.HandlerError<typeof typed.tool>, SearchError>
    >().toEqualTypeOf<SearchError>();
    expectTypeOf<Tool.HandlerServices<typeof typed.tool>>().toEqualTypeOf<CurrentToolCatalog>();
  };

declare const expectTypeOf: typeof ExpectTypeOf;
