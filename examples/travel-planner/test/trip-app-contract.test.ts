import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import type { WorkerReportPreparationFailure } from "@effect-agent/engine/SubagentHost";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import type { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import type { SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import { type Crypto, Effect, Schema } from "effect";
import { Tool, type Toolkit } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";
import { expect, expectTypeOf, it } from "vite-plus/test";

import { TripToolsLive } from "../src/agent.ts";
import { safeMessageUrl } from "../src/components/message-text.tsx";
import { AppFilePath, type PlannerError, type TripSiteStore } from "../src/domain.ts";
import type { researchScoutReport } from "../src/research/runtime.ts";
import { researchScout } from "../src/research/scout.ts";
import {
  previousContinuingPlanner as planner,
  planner as currentPlanner,
} from "../src/server/planner.ts";
import type { TripRepository } from "../src/server/trips.ts";
import type { AppBuildBucket } from "../src/trip-app/bucket.ts";
import { appEditor } from "../src/trip-app/editor.ts";
import type { AppRepository } from "../src/trip-app/repository.ts";
import type { AppSourceStore } from "../src/trip-app/source.ts";
import { AppTools } from "../src/trip-app/tools.ts";
import { FixtureBrowserLive } from "./fixtures/browser.ts";
import { FixtureModel } from "./fixtures/models.ts";

it("keeps app tools, dependencies, and typed failures in the v7 composition", () => {
  const run = AgentRuntime.run(planner, {
    message: "Make a trip website",
    selectedTripId: "trip",
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

  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
    | TripRepository
    | TripSiteStore
    | AppRepository
    | AppSourceStore
    | AppBuildBucket
    | ThreadObjectIdentity
    | Tool.HandlersFor<Toolkit.Tools<typeof AppTools>>
  >();
  expectTypeOf<Extract<Effect.Error<typeof run>, PlannerError>>().toEqualTypeOf<PlannerError>();
  expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof planner>
  >();
  expect(planner.id).toBe("travel-planner-v7");
  for (const tool of Object.values(AppTools.tools)) {
    const schema = Tool.getJsonSchema(tool, { transformer: toCodecOpenAI });

    expect(schema.type).toBe("object");
    expect(schema.additionalProperties).toBe(false);
  }
});

it("keeps every model tool schema free of unsupported regex lookaround", () => {
  for (const tool of [
    ...Object.values(currentPlanner.toolkit.tools),
    ...Object.values(appEditor.toolkit.tools),
    ...Object.values(researchScout.toolkit.tools),
  ]) {
    if (Tool.isProviderDefined(tool)) continue;
    const schema = Tool.getJsonSchema(tool, { transformer: toCodecOpenAI });

    expect(JSON.stringify(schema), tool.name).not.toMatch(/\(\?(?:[=!]|<[=!])/);
  }
});

it("accepts source paths while rejecting traversal independently of model schema enforcement", () => {
  for (const path of [
    "package.json",
    ".gitignore",
    "packages/web/src/app.tsx",
    "@trip/contracts.ts",
  ])
    expect(Schema.is(AppFilePath)(path), path).toBe(true);
  for (const path of [
    "",
    ".",
    "..",
    "../package.json",
    "packages/../package.json",
    "packages/./index.ts",
    "packages/..",
    "/package.json",
    "packages//index.ts",
    "packages/",
    "packages\\index.ts",
    "packages/%2e%2e/index.ts",
    "a".repeat(241),
  ])
    expect(Schema.is(AppFilePath)(path), path).toBe(false);
});

it("allows exact legacy trip links without enabling arbitrary local actions", () => {
  expect(safeMessageUrl("/trips/tahoe-123/2")).toBe("/trips/tahoe-123/2");
  for (const path of [
    "/cdn-cgi/access/logout",
    "//outside.example",
    "/trips/id/2?redirect=x",
    "/trips/../2",
    "javascript:alert(1)",
  ])
    expect(safeMessageUrl(path)).toBeUndefined();
});

it("preserves the v9 coordinator's native worker requirements and typed failures", () => {
  const run = AgentRuntime.run(currentPlanner, {
    message: "Restyle the trip app and continue camping research",
    selectedTripId: "trip",
    publication: null,
  }).pipe(Effect.provide(FixtureModel));

  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
    | ThreadHistory
    | IdGenerator
    | TripRepository
    | TripSiteStore
    | AppRepository
    | AppSourceStore
    | AppBuildBucket
    | ThreadObjectIdentity
    | Crypto.Crypto
    | Tool.HandlersFor<typeof currentPlanner.toolkit.tools>
  >();
  expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof currentPlanner>
  >();
  expectTypeOf<Extract<Effect.Error<typeof run>, PlannerError>>().toEqualTypeOf<PlannerError>();
});

it("keeps research reports dependent on canonical storage and scouts free of mutation services", () => {
  expectTypeOf<
    Effect.Services<ReturnType<typeof researchScoutReport.prepare>>
  >().toEqualTypeOf<SubmissionLedger>();
  expectTypeOf<Effect.Error<ReturnType<typeof researchScoutReport.prepare>>>().toEqualTypeOf<
    PlannerError | WorkerReportPreparationFailure
  >();
  const run = AgentRuntime.runUnknown(researchScout, {}).pipe(Effect.provide(FixtureModel));

  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
    ThreadHistory | IdGenerator | Tool.HandlersFor<typeof researchScout.toolkit.tools>
  >();
  expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof researchScout>
  >();
  expect(Object.keys(researchScout.toolkit.tools)).toEqual([
    "finish_research",
    "read_travel_page",
    "OpenAiWebSearch",
  ]);
});

it("preserves the editor's scoped app services without requiring a child host", () => {
  const run = AgentRuntime.runUnknown(appEditor, {}).pipe(Effect.provide(FixtureModel));

  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
    | ThreadHistory
    | IdGenerator
    | TripRepository
    | AppRepository
    | AppSourceStore
    | AppBuildBucket
    | ThreadObjectIdentity
    | Tool.HandlersFor<typeof appEditor.toolkit.tools>
  >();
  expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof appEditor>
  >();
  expectTypeOf<Extract<Effect.Error<typeof run>, PlannerError>>().toEqualTypeOf<PlannerError>();
});
