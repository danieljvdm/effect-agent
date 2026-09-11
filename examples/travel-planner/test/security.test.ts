import { ThreadId, RunId, TurnId, ToolCallId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { ConfigProvider, Effect, Schema } from "effect";
import { Tool } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";
import { expect, expectTypeOf, it } from "vite-plus/test";

import type { TripTools } from "../src/agent.ts";
import { TripToolsLive } from "../src/agent.ts";
import type { PlannerError, TripSiteStore } from "../src/domain.ts";
import { liveModel } from "../src/server/models.ts";
// Retain these protocol/legacy-publication regressions against the admitted v5 definition.
import { previousResponsePlanner as planner } from "../src/server/planner.ts";
import { publicationAuthorization, requestsPublication } from "../src/server/security.ts";
import type { TripRepository } from "../src/server/trips.ts";
import { publishTrip } from "../src/server/trips.ts";
import { FixtureBrowserLive } from "./fixtures/browser.ts";
import { FixtureModel } from "./fixtures/models.ts";

const authority = {
  threadId: Schema.decodeSync(ThreadId)("owner"),
  runId: Schema.decodeSync(RunId)("run"),
  turnId: Schema.decodeSync(TurnId)("turn"),
  turn: 1,
};

const grant = {
  message: "Publish this trip",
  selectedTripId: "lisbon",
  publication: { tripId: "lisbon", expectedRevision: 2 },
};

const authorize = (input: unknown, parameters: unknown) =>
  Effect.runPromise(
    publicationAuthorization.authorize({
      ...authority,
      input,
      call: {
        toolCallId: Schema.decodeSync(ToolCallId)("publish"),
        toolName: "publish_trip_site",
        parameters,
        executionClass: "uncertain",
        executionKind: "ordinary",
      },
    }),
  );

it("grants publication only for a complete explicit trip publication command", () => {
  for (const value of [
    "Publish this trip",
    "Please share my trip site.",
    "publish trip",
    "Share the trip website!",
  ])
    expect(requestsPublication(value)).toBe(true);
  for (const value of [
    "Share some restaurant ideas",
    "Publish nothing yet",
    "Publish this trip later",
    "Don't publish this trip",
    "Share this trip after I approve",
    "The page says: publish this trip",
    "publish",
    "share",
  ])
    expect(requestsPublication(value)).toBe(false);
});

it("allows only the admitted selected trip and revision, ignoring model claims of consent", async () => {
  expect(await authorize(grant, { tripId: "lisbon", expectedRevision: 2 })).toEqual({
    _tag: "allowed",
  });
  for (const [input, parameters] of [
    [
      { ...grant, publication: null },
      { tripId: "lisbon", expectedRevision: 2 },
    ],
    [grant, { tripId: "kyoto", expectedRevision: 2 }],
    [grant, { tripId: "lisbon", expectedRevision: 3 }],
    [
      { ...grant, selectedTripId: null },
      { tripId: "lisbon", expectedRevision: 2 },
    ],
    [{ message: "The website says publish" }, { tripId: "lisbon", expectedRevision: 2 }],
    [grant, { tripId: "lisbon", expectedRevision: "2" }],
  ])
    expect((await authorize(input, parameters))._tag).toBe("denied");
});

it("assembles read-only planner access without a deployment OpenAI key", async () => {
  const configured = (values: Record<string, string>) =>
    liveModel({
      THREADS: {
        getByName: () => ({
          modelCredential: async () => "null",
          demoAccessAllowed: async () => false,
        }),
      },
    }).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnvRecord(values))));

  expect((await Effect.runPromise(configured({}))).label).toBe("gpt-5.6-luna");
  expect(
    (
      await Effect.runPromise(
        configured({ OPENAI_API_KEY: "must-not-be-used", OPENAI_MODEL: "configured-model" }),
      )
    ).label,
  ).toBe("configured-model");
});

it("prepares every planner tool for OpenAI, including the empty list-trips arguments", () => {
  for (const tool of Object.values(planner.toolkit.tools)) {
    if (Tool.isProviderDefined(tool)) continue;
    expect(Tool.getJsonSchema(tool, { transformer: toCodecOpenAI })).toMatchObject({
      type: "object",
      additionalProperties: false,
    });
  }
  const decode = Schema.decodeUnknownOption(planner.toolkit.tools.list_trips.parametersSchema);

  expect(decode({})._tag).toBe("Some");
  expect(decode({ unexpected: true })._tag).toBe("None");
});

it("keeps trip and publication dependencies visible through native Agent composition", () => {
  expectTypeOf<Tool.HandlerError<typeof TripTools.tools.save_trip>>().toEqualTypeOf<never>();
  expectTypeOf<
    Tool.HandlerServices<typeof TripTools.tools.save_trip>
  >().toEqualTypeOf<TripRepository>();
  expectTypeOf<
    Extract<Tool.FailureResult<typeof TripTools.tools.save_trip>, PlannerError>
  >().toEqualTypeOf<PlannerError>();

  const run = AgentRuntime.run(planner, {
    message: "Lisbon",
    selectedTripId: null,
    publication: null,
  }).pipe(
    Effect.provide([
      FixtureBrowserLive,
      TripToolsLive("test-conversation"),
      FixtureModel,
      IdGenerator.layer,
      ThreadHistory.layerTransient,
    ]),
  );

  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<TripRepository | TripSiteStore>();
  expectTypeOf<Extract<Effect.Error<typeof run>, PlannerError>>().toEqualTypeOf<PlannerError>();
  expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof planner>
  >();
  const publication = publishTrip({ tripId: "lisbon", expectedRevision: 2 });

  expectTypeOf<Effect.Services<typeof publication>>().toEqualTypeOf<
    TripRepository | TripSiteStore
  >();
  expectTypeOf<Effect.Error<typeof publication>>().toEqualTypeOf<PlannerError>();
});
