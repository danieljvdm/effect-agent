import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { it } from "@effect/vitest";
import type { Layer } from "effect";
import { Context, Effect, Schema, Stream } from "effect";
import type { AiError } from "effect/unstable/ai";
import { Tool, Toolkit } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";
import { expect, expectTypeOf } from "vite-plus/test";

import {
  DeliverResponse,
  DisplayToolsLive,
  ResponseToolsLive,
  ShowTravelOptions,
} from "../src/agent.ts";
import { PreviousReadTravelPage, PreviousReadTravelPageResult } from "../src/research.ts";
import {
  planner,
  previousBudgetPlanner,
  previousResearchPlanner,
  previousEditorPlanner,
  previousAppPlanner,
  previousContinuingPlanner,
  previousCardPlanner,
  previousResponsePlanner,
  previousPlanner,
  legacyPlanner,
} from "../src/server/planner.ts";
import type { TravelContent } from "../src/travel-content.ts";

const content: TravelContent = {
  title: "Sourced travel options",
  items: [
    {
      kind: "stay",
      name: "A source property",
      location: "Lake Tahoe",
      url: "https://www.airbnb.com/rooms/24912220",
      photos: [],
      highlights: ["Review the linked source for its amenities"],
      price: null,
      note: "Dates and availability are unverified.",
    },
  ],
};

const show = Effect.fn("test.showTravelOptions")(function* (value: TravelContent) {
  const toolkit = yield* Toolkit.make(ShowTravelOptions);

  return yield* Stream.runCollect(yield* toolkit.handle("show_travel_options", value));
});

it.effect(
  "echoes validated travel cards through a native read-only tool without dependencies",
  () =>
    Effect.gen(function* () {
      const result = yield* show(content).pipe(Effect.provide(DisplayToolsLive));

      expect(result).toHaveLength(1);
      expect(result[0]).toMatchObject({
        isFailure: false,
        result: content,
        encodedResult: content,
      });
      expect(Context.get(ShowTravelOptions.annotations, Tool.Readonly)).toBe(true);
      expect(Context.get(ShowTravelOptions.annotations, ToolExecutionClass)).toBe("readonly");
      expect(Tool.getJsonSchema(ShowTravelOptions, { transformer: toCodecOpenAI })).toMatchObject({
        type: "object",
        additionalProperties: false,
      });
      expectTypeOf<Layer.Services<typeof DisplayToolsLive>>().toEqualTypeOf<never>();
      expectTypeOf<Effect.Error<ReturnType<typeof show>>>().toEqualTypeOf<AiError.AiError>();
      expectTypeOf<Effect.Services<ReturnType<typeof show>>>().toEqualTypeOf<
        Tool.Handler<"show_travel_options">
      >();
    }),
);

it.effect("rejects unsafe nested URLs and oversize display payloads before returning cards", () =>
  Effect.gen(function* () {
    const first = content.items[0];

    if (first === undefined) return yield* Effect.die("Missing fixture item");
    for (const invalid of [
      { ...content, items: [{ ...first, url: "javascript:alert(1)" }] },
      {
        ...content,
        items: [
          {
            ...first,
            photos: [
              { url: "https://user:secret@images.example.com/image.jpg", caption: "Unsafe" },
            ],
          },
        ],
      },
      { ...content, items: Array.from({ length: 7 }, () => first) },
    ]) {
      const error = yield* show(invalid).pipe(Effect.provide(DisplayToolsLive), Effect.flip);

      expect(error.reason._tag).toBe("ToolParameterValidationError");
    }
  }),
);

it.effect(
  "requires response delivery while retaining the accepted v10/v9/v8/v7/v6/v5/v4/v3/v2 contracts",
  () =>
    Effect.gen(function* () {
      expect(planner.id).toBe("travel-planner-v11");
      expect(previousBudgetPlanner.id).toBe("travel-planner-v10");
      expect(previousResearchPlanner.id).toBe("travel-planner-v9");
      expect(previousEditorPlanner.id).toBe("travel-planner-v8");
      expect(previousEditorPlanner.toolkit.tools).not.toHaveProperty("research_scout_start");
      expect(planner.toolkit.tools).toHaveProperty("research_scout_start");
      expect(planner.toolkit.tools).toHaveProperty("research_scout_follow_up");
      expect(previousContinuingPlanner.id).toBe("travel-planner-v7");
      expect(previousAppPlanner.id).toBe("travel-planner-v6");
      expect(previousResponsePlanner.id).toBe("travel-planner-v5");
      expect(previousResponsePlanner.toolkit.tools).not.toHaveProperty("create_trip_app");
      expect(previousContinuingPlanner.toolkit.tools).toHaveProperty("create_trip_app");
      expect(planner.toolkit.tools).not.toHaveProperty("edit_trip_app");
      expect(planner.toolkit.tools).not.toHaveProperty("create_trip_app");
      expect(planner.toolkit.tools).toHaveProperty("app_editor_start");
      expect(planner.toolkit.tools).toHaveProperty("app_editor_follow_up");
      expect(planner.completion).toMatchObject({ tool: "deliver_response", required: true });
      expect(previousCardPlanner.id).toBe("travel-planner-v4");
      expect(previousCardPlanner.completion).toBeUndefined();
      expect(previousCardPlanner.toolkit.tools).not.toHaveProperty("deliver_response");
      expect(previousPlanner.id).toBe("travel-planner-v3");
      expect(legacyPlanner.id).toBe("travel-planner");

      const oldNames = [
        "list_trips",
        "get_trip",
        "save_trip",
        "publish_trip_site",
        "read_travel_page",
        "OpenAiWebSearch",
      ];

      for (const prior of [previousPlanner, legacyPlanner]) {
        expect(Object.keys(prior.toolkit.tools)).toEqual(oldNames);
        expect(prior.toolkit.tools.read_travel_page).toBe(PreviousReadTravelPage);
        expect(prior.toolkit.tools.read_travel_page.successSchema).toBe(
          PreviousReadTravelPageResult,
        );
        expect(yield* prior.instructions()).not.toContain("show_travel_options");
      }
      expect(planner.toolkit.tools).toHaveProperty("show_travel_options");
      expect(yield* planner.instructions()).toContain("Never invent or guess image URLs");
      expect(Schema.is(previousPlanner.output)("Plain answer")).toBe(true);
      expect(Schema.is(legacyPlanner.output)({ message: "Historical answer" })).toBe(true);
      expect(Schema.is(legacyPlanner.output)("Plain answer")).toBe(false);
    }),
);

it.effect(
  "delivers schema-validated cards or an ordinary greeting through the same completion tool",
  () =>
    Effect.gen(function* () {
      const toolkit = yield* Toolkit.make(DeliverResponse);

      for (const response of [
        { message: "Here are your options.", content },
        { message: "Where would you like to go?", content: null },
      ]) {
        const results = yield* Stream.runCollect(
          yield* toolkit.handle("deliver_response", response),
        );

        expect(results[0]).toMatchObject({ isFailure: false, result: response });
        expect(planner.completion?.project({ parameters: response, result: response })).toBe(
          response.message,
        );
      }
      expectTypeOf<Layer.Services<typeof ResponseToolsLive>>().toEqualTypeOf<never>();
    }).pipe(Effect.provide(ResponseToolsLive)),
);
