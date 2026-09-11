import type * as Agent from "@effect-agent/core/Agent";
import { ThreadMaintenance } from "@effect-agent/platform-cloudflare/Alarm";
import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import {
  CloudflareBrowser,
  type CloudflareBrowserOptions,
} from "@effect-agent/platform-cloudflare/CloudflareBrowser";
import * as ThreadObject from "@effect-agent/platform-cloudflare/ThreadObject";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { SubmissionLedger, SubmissionLookupById } from "@effect-agent/thread/SubmissionLedger";
import { Effect, Layer, Option, Schema } from "effect";
import { DurableObject, WorkerEnvironment } from "effect-cf";
import type { Tool } from "effect/unstable/ai";
import { FetchHttpClient, HttpRouter } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";

import { TripToolsLive } from "../agent.ts";
import type { TripSiteStore } from "../domain.ts";
import {
  PlannerError,
  PlannerRpcs,
  PlannerSnapshot,
  PlannerProgress,
  defaultPlannerSettings,
} from "../domain.ts";
import { ReadTravelPageLive } from "../research.ts";
import { LiveConversationInput } from "../research/contracts.ts";
import {
  ResearchAuthorizationLive,
  researchScoutReport,
  conversationScoutReport,
  scoutAttemptLayer,
  liveScoutReport,
  recoverableScoutReport,
  editorReport,
  ScoutMessagingLive,
} from "../research/runtime.ts";
import {
  ExpandedResearchScoutBackground,
  ResearchScoutBackground,
  researchScout,
  progressResearchScout,
  recoverableResearchScout,
  RecoverableResearchScoutBackground,
  PreviousProgressResearchScoutBackground,
  ProgressResearchScoutBackground,
  PreviousResearchScoutBackground,
} from "../research/scout.ts";
import { AppBuildBucketLive } from "../trip-app/bindings.ts";
import { EditorHostLive, editorAttemptLayer } from "../trip-app/editor-runtime.ts";
import { AppEditorBackground, appEditor } from "../trip-app/editor.ts";
import { OwnerAppRepositoryLive, serveAppRepository } from "../trip-app/remote.ts";
import {
  AppSourceLive,
  createTripApp,
  restoreTripApp,
  retryTripAppBuild,
} from "../trip-app/service.ts";
import { AppToolsLive } from "../trip-app/tools-live.ts";
import { PlannerModel, plannerSnapshot, sendMessage, voiceWork } from "./application.ts";
import {
  CredentialSource,
  CredentialStore,
  credentialStoreLayer,
  credentialSourceLayer,
  encodeStoredCredential,
  validateOpenAiKey,
} from "./credentials.ts";
import {
  DiagnosticContext,
  DiagnosticObserverLive,
  FailureDiagnosticsLive,
  readDiagnostics,
  RecordedDiagnostics,
} from "./diagnostics.ts";
import { liveModel } from "./models.ts";
import {
  planner,
  previousVoicePlanner,
  previousProgressPlanner,
  previousDelegatingPlanner,
  previousTextPlanner,
  previousBudgetPlanner,
  previousResearchPlanner,
  previousEditorPlanner,
  previousContinuingPlanner,
  previousAppPlanner,
  previousResponsePlanner,
  previousCardPlanner,
  previousPlanner,
  legacyPlanner,
} from "./planner.ts";
import { PlannerAttempt, ProgressStore } from "./progress.ts";
import { PlannerSettingsStore, PlannerSettingsStoreLive } from "./settings.ts";
import { ownerOfThread, privateConversation, publicSnapshot } from "./tenancy.ts";
import { OwnerTripRepositoryLive, serveTripRepository } from "./trip-rpc.ts";
import { publishTrip, TripRepository } from "./trips.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      BROWSER?: CloudflareBrowserOptions["browser"];
    }
  }
}

/** The HTTP edge bounds defect diagnostics; engine defects retain their canonical meaning. */
const safeRpc = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchDefect(
      () =>
        new PlannerError({
          code: "unavailable",
          message: "The request failed unexpectedly. Refresh before retrying.",
        }),
    ),
  );

const CredentialSourceLive: Layer.Layer<CredentialSource, never, WorkerEnvironment> = Layer.unwrap(
  Effect.map(WorkerEnvironment, credentialSourceLayer),
);

const effectiveConnection = Effect.gen(function* () {
  const connection = yield* Effect.flatMap(CredentialStore, (store) => store.status);

  if (connection.connected) return connection;
  const identity = yield* ThreadObjectIdentity;
  const source = yield* CredentialSource;

  return (yield* source.funded(ownerOfThread(identity.threadId)))
    ? { ...connection, connected: true, serverFunded: true }
    : connection;
});

export const plannerHandlers = PlannerRpcs.toLayer({
  GetOpenAiConnection: () => safeRpc(effectiveConnection),
  ConnectOpenAi: ({ apiKey }) =>
    safeRpc(
      Effect.gen(function* () {
        const store = yield* CredentialStore;

        const verified = yield* validateOpenAiKey(apiKey).pipe(
          Effect.provide(FetchHttpClient.layer),
        );

        return yield* store.save(verified);
      }),
    ),
  DisconnectOpenAi: () =>
    safeRpc(
      Effect.flatMap(CredentialStore, (store) => store.remove).pipe(
        Effect.andThen(effectiveConnection),
      ),
    ),
  CreateTripApp: ({ tripId }) => safeRpc(createTripApp(tripId)),
  RetryTripAppBuild: ({ tripId }) => safeRpc(retryTripAppBuild(tripId)),
  RestoreTripApp: ({ tripId, commitId }) => safeRpc(restoreTripApp(tripId, commitId)),
  GetPlannerSettings: () =>
    safeRpc(
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;

        if (identity.threadId !== ownerOfThread(identity.threadId))
          return yield* new PlannerError({
            code: "invalid",
            message: "Model preferences belong to the signed-in account.",
          });

        return yield* Effect.flatMap(PlannerSettingsStore, (settings) => settings.get);
      }),
    ),
  SavePlannerSettings: (request) =>
    safeRpc(
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;

        if (identity.threadId !== ownerOfThread(identity.threadId))
          return yield* new PlannerError({
            code: "invalid",
            message: "Model preferences belong to the signed-in account.",
          });

        return yield* Effect.flatMap(PlannerSettingsStore, (settings) => settings.save(request));
      }),
    ),
  GetPlanner: ({ conversationId }) =>
    safeRpc(
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;

        if (conversationId === null)
          return publicSnapshot(identity.threadId, yield* plannerSnapshot(null));
        const privateId = yield* privateConversation(identity.threadId, conversationId);
        const env = yield* WorkerEnvironment;

        const reply = yield* Effect.tryPromise({
          try: () => env.ACCOUNT_THREADS.getByName(privateId).plannerState(),
          catch: () =>
            new PlannerError({
              code: "unavailable",
              message: "This conversation is temporarily unavailable.",
            }),
        });

        const snapshot = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(PlannerSnapshot))(
          reply,
        ).pipe(
          Effect.mapError(
            () =>
              new PlannerError({
                code: "unavailable",
                message: "The conversation response could not be read.",
              }),
          ),
        );

        return publicSnapshot(identity.threadId, snapshot);
      }),
    ),
  GetVoiceWork: (request) =>
    safeRpc(
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;

        const conversationId = yield* privateConversation(
          identity.threadId,
          request.conversationId,
        );

        return yield* voiceWork({ ...request, conversationId });
      }),
    ),
  SendMessage: (request) =>
    Effect.gen(function* () {
      const identity = yield* ThreadObjectIdentity;
      const conversationId = yield* privateConversation(identity.threadId, request.conversationId);
      const maintenance = yield* ThreadMaintenance;

      const settings =
        request.settings ??
        (yield* Effect.flatMap(PlannerSettingsStore, (preferences) => preferences.get));

      return yield* maintenance.withMutation(sendMessage({ ...request, settings, conversationId }));
    }).pipe(
      Effect.mapError(
        () =>
          new PlannerError({
            code: "unavailable",
            message: "The request could not be accepted. Refresh before retrying.",
          }),
      ),
      safeRpc,
    ),
  SaveTrip: ({ conversationId, ...request }) =>
    safeRpc(
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const privateId = yield* privateConversation(identity.threadId, conversationId);

        return yield* Effect.flatMap(TripRepository, (trips) => trips.save(request, privateId));
      }),
    ),
  PublishTrip: (request) => safeRpc(publishTrip(request)),
});

const RpcHttp = RpcServer.layerHttp({
  group: PlannerRpcs,
  path: "/api/rpc",
  protocol: "http",
  concurrency: 4,
}).pipe(Layer.provide(plannerHandlers), Layer.provide(RpcSerialization.layerNdjson));

export const plannerApplication = <E, R>(
  model: Layer.Layer<Agent.ModelServices, never, PlannerAttempt>,
  modelVersion: string,
  modelLabel: string,
  browser: Layer.Layer<Tool.Handler<"read_travel_page">, E, R>,
  selectedModel?: Layer.Layer<Agent.ModelServices, never, PlannerAttempt>,
  sourceLayer = AppSourceLive,
) => {
  const attemptLayer = (
    context: {
      readonly threadId: string;
      readonly submissionId: SubmissionLookupById["submissionId"];
      readonly attemptId: string;
    },
    expandedResearch: boolean | "progress" | "recoverable" = false,
  ) =>
    Layer.mergeAll(
      TripToolsLive(context.threadId),
      AppToolsLive,
      AppEditorBackground.layer,
      PreviousResearchScoutBackground.layer,
      PreviousProgressResearchScoutBackground.layer,
      expandedResearch === "recoverable"
        ? RecoverableResearchScoutBackground.layer
        : expandedResearch === "progress"
          ? ProgressResearchScoutBackground.layer
          : expandedResearch
            ? ExpandedResearchScoutBackground.layer
            : ResearchScoutBackground.layer,
    ).pipe(
      Layer.provideMerge(
        Layer.effect(
          PlannerAttempt,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const progress = yield* ProgressStore;

            const unavailable = () =>
              new PlannerError({
                code: "unavailable",
                message: "The admitted model settings could not be read.",
              });

            const settings = yield* Effect.cached(
              ledger.lookup(SubmissionLookupById.make({ submissionId: context.submissionId })).pipe(
                Effect.mapError(unavailable),
                Effect.flatMap((found) =>
                  Option.isNone(found) || found.value.threadId !== context.threadId
                    ? Effect.fail(unavailable())
                    : Schema.decodeUnknownEffect(LiveConversationInput)(
                        found.value.inputPayload,
                      ).pipe(
                        Effect.map((input) => input.settings ?? defaultPlannerSettings),
                        Effect.mapError(unavailable),
                      ),
                ),
              ),
            );

            const writer = yield* Effect.acquireRelease(
              progress.begin(context.submissionId, context.attemptId),
              (writer) => writer.finish,
            );

            return {
              settings,
              progress: writer,
              billingOwner: Effect.succeed(ownerOfThread(context.threadId)),
            };
          }),
        ),
      ),
      Layer.provideMerge(
        Layer.succeed(DiagnosticContext, {
          submissionId: context.submissionId,
          attemptId: context.attemptId,
        }),
      ),
    );

  const registered = DurableAgentRuntime.layerRegistered([
    {
      agent: planner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: planner.id, version: "travel-planner-v15" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(planner.toolkit.tools),
      }),
      reporting: [recoverableScoutReport, editorReport],
      attemptLayer: (context) => attemptLayer(context, "recoverable"),
    },
    {
      agent: previousDelegatingPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousDelegatingPlanner.id, version: "travel-planner-v14" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousDelegatingPlanner.toolkit.tools),
      }),
      reporting: [liveScoutReport, editorReport],
      attemptLayer: (context) => attemptLayer(context, "progress"),
    },
    {
      agent: previousProgressPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousProgressPlanner.id, version: "travel-planner-v13" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousProgressPlanner.toolkit.tools),
      }),
      reporting: [liveScoutReport, editorReport],
      attemptLayer: (context) => attemptLayer(context, "progress"),
    },
    {
      agent: previousVoicePlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousVoicePlanner.id, version: "travel-planner-v12" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousVoicePlanner.toolkit.tools),
      }),
      reporting: [conversationScoutReport],
      attemptLayer: (context) => attemptLayer(context, true),
    },
    {
      agent: previousTextPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousTextPlanner.id, version: "travel-planner-v11" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousTextPlanner.toolkit.tools),
      }),
      reporting: [researchScoutReport],
      attemptLayer: (context) => attemptLayer(context, true),
    },
    {
      agent: previousBudgetPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousBudgetPlanner.id, version: "travel-planner-v10" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousBudgetPlanner.toolkit.tools),
      }),
      reporting: [researchScoutReport],
      attemptLayer,
    },
    {
      agent: previousResearchPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousResearchPlanner.id, version: "travel-planner-v9" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousResearchPlanner.toolkit.tools),
      }),
      reporting: [researchScoutReport],
      attemptLayer,
    },
    {
      agent: previousEditorPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousEditorPlanner.id, version: "travel-planner-v8" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousEditorPlanner.toolkit.tools),
      }),
      attemptLayer,
    },
    {
      agent: recoverableResearchScout,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: recoverableResearchScout.id, version: "travel-research-scout-v3" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(recoverableResearchScout.toolkit.tools),
      }),
      attemptLayer: (context) =>
        scoutAttemptLayer(context, true).pipe(
          Layer.provideMerge(
            Layer.succeed(DiagnosticContext, {
              submissionId: context.submissionId,
              attemptId: context.attemptId,
            }),
          ),
        ),
    },
    {
      agent: progressResearchScout,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: progressResearchScout.id, version: "travel-research-scout-v2" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(progressResearchScout.toolkit.tools),
      }),
      attemptLayer: (context) =>
        scoutAttemptLayer(context).pipe(
          Layer.provideMerge(
            Layer.succeed(DiagnosticContext, {
              submissionId: context.submissionId,
              attemptId: context.attemptId,
            }),
          ),
        ),
    },
    {
      agent: researchScout,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: researchScout.id, version: "travel-research-scout-v1" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(researchScout.toolkit.tools),
      }),
      attemptLayer: (context) =>
        scoutAttemptLayer(context).pipe(
          Layer.provideMerge(
            Layer.succeed(DiagnosticContext, {
              submissionId: context.submissionId,
              attemptId: context.attemptId,
            }),
          ),
        ),
    },
    {
      agent: appEditor,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: appEditor.id, version: "trip-app-editor-v1" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(appEditor.toolkit.tools),
      }),
      attemptLayer: (context) =>
        editorAttemptLayer(context).pipe(
          Layer.provideMerge(
            Layer.succeed(DiagnosticContext, {
              submissionId: context.submissionId,
              attemptId: context.attemptId,
            }),
          ),
        ),
    },
    {
      agent: previousContinuingPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousContinuingPlanner.id, version: "travel-planner-v7" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousContinuingPlanner.toolkit.tools),
      }),
      attemptLayer,
    },
    {
      agent: previousAppPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousAppPlanner.id, version: "travel-planner-v6" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousAppPlanner.toolkit.tools),
      }),
      attemptLayer,
    },
    {
      agent: previousResponsePlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousResponsePlanner.id, version: "travel-planner-v5" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousResponsePlanner.toolkit.tools),
      }),
      attemptLayer,
    },
    {
      agent: previousCardPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousCardPlanner.id, version: "travel-planner-v4" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousCardPlanner.toolkit.tools),
      }),
      attemptLayer,
    },
    {
      agent: previousPlanner,
      model: selectedModel ?? model,
      definitions: DefinitionDigestInput.make({
        agent: { id: previousPlanner.id, version: "travel-planner-v3" },
        model: selectedModel === undefined ? modelVersion : "openai-selectable-v1",
        tools: Object.keys(previousPlanner.toolkit.tools),
      }),
      attemptLayer,
    },
    {
      agent: legacyPlanner,
      model,
      definitions: DefinitionDigestInput.make({
        agent: { id: legacyPlanner.id, version: "travel-planner-v2" },
        model: modelVersion,
        tools: Object.keys(legacyPlanner.toolkit.tools),
      }),
      attemptLayer,
    },
  ]).pipe(
    Layer.provide(browser),
    Layer.provide(EditorHostLive),
    Layer.provide(ResearchAuthorizationLive),
    Layer.provide(DiagnosticObserverLive),
    Layer.provide(ScoutMessagingLive),
  );

  // Acquire the owner's SQL once, then capture the repository in the registered tools.
  // Rebuild maintenance against that runtime so alarms execute the same registrations.
  const local = Layer.mergeAll(
    OwnerTripRepositoryLive,
    OwnerAppRepositoryLive,
    AppBuildBucketLive,
    PlannerSettingsStoreLive,
    Layer.unwrap(
      Effect.gen(function* () {
        const env = yield* WorkerEnvironment;
        const identity = yield* ThreadObjectIdentity;

        return credentialStoreLayer(env, identity.threadId);
      }),
    ),
    CredentialSourceLive,
    FailureDiagnosticsLive,
    sourceLayer,
  ).pipe(Layer.provideMerge(ThreadObject.layer([])));

  return Layer.fresh(ThreadMaintenance.layer).pipe(
    Layer.provideMerge(registered),
    Layer.provideMerge(local),
    Layer.provideMerge(ProgressStore.layer),
    Layer.provideMerge(Layer.succeed(PlannerModel, { model: modelLabel })),
  );
};

const PlannerLive = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;

    if (env.BROWSER === undefined)
      return yield* new PlannerError({
        code: "unavailable",
        message: "The planner requires a Browser Run binding.",
      });

    const model: Effect.Success<typeof liveModel> = yield* liveModel;
    const credentials: Layer.Layer<CredentialSource> = credentialSourceLayer(env);

    return plannerApplication(
      model.model.pipe(Layer.provide(credentials)),
      model.identity,
      model.label,
      CloudflareBrowser.layer({ handlers: ReadTravelPageLive }, { browser: env.BROWSER }),
      model.selectable.pipe(Layer.provide(credentials)),
    );
  }),
);

/** Real durable engine with alarm recovery. Tests substitute the application Layer. */
export const makeTravelPlannerThread = <E>(
  sites: Layer.Layer<TripSiteStore, E, WorkerEnvironment>,
  application = PlannerLive,
) => {
  return class extends ThreadObject.make(application.pipe(Layer.provideMerge(sites)), {
    namespaceBinding: "ACCOUNT_THREADS",
    deploymentId: "travel-planner-v1",
    producerPrefix: "travel-planner",
    wakeScanInterval: 250,
    settlementPollInterval: 100,
    maxQueueDepthPerLane: 8,
    maxInputBytes: 16 * 1024,
  }) {
    /** Host-only lookup; no HTTP route exposes ciphertext or decrypted model credentials. */
    modelCredential(): Promise<string> {
      return this[DurableObject.RunSymbol](
        Effect.flatMap(CredentialStore, (store) => store.sealed).pipe(
          Effect.flatMap(encodeStoredCredential),
        ),
      );
    }

    /** Finite native RPC; UI observation never owns or interrupts durable execution. */
    plannerProgress(): Promise<string> {
      return this[DurableObject.RunSymbol](
        Effect.flatMap(ProgressStore, (progress) => progress.read).pipe(
          Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(PlannerProgress))),
        ),
      );
    }

    /** Private namespace RPC; callers verify worker lineage before reading its diagnostics. */
    plannerDiagnostics(): Promise<string> {
      return this[DurableObject.RunSymbol](
        readDiagnostics.pipe(
          Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(RecordedDiagnostics))),
        ),
      );
    }

    /** Private namespace RPC; callers cannot access it through the public HTTP API. */
    tripRepository(request: string): Promise<string> {
      return this[DurableObject.RunSymbol](serveTripRepository(request));
    }

    tripApp(request: string): Promise<string> {
      return this[DurableObject.RunSymbol](serveAppRepository(request));
    }

    plannerState(): Promise<string> {
      return this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const identity = yield* ThreadObjectIdentity;
          const snapshot = yield* plannerSnapshot(identity.threadId);

          return yield* Schema.encodeEffect(Schema.fromJsonString(PlannerSnapshot))(snapshot);
        }),
      );
    }

    /** Native RPC is private to the authenticated Worker ingress. */
    plannerFetch(request: Request): Promise<Response> {
      return this[DurableObject.RunSymbol](
        Effect.scoped(
          Effect.gen(function* () {
            const context =
              yield* Effect.context<
                Exclude<Layer.Services<typeof RpcHttp>, HttpRouter.HttpRouter>
              >();

            const web = yield* Effect.acquireRelease(
              Effect.sync(() =>
                HttpRouter.toWebHandler(
                  RpcHttp.pipe(Layer.provide(Layer.succeedContext(context))),
                  { disableLogger: true },
                ),
              ),
              (handler) => Effect.promise(() => handler.dispose()),
            );

            const response = yield* Effect.promise(() => web.handler(request));
            // Finite RPC responses are consumed before their request-owned runtime is finalized.
            const body = yield* Effect.promise(() => response.arrayBuffer());

            return new Response(body, { status: response.status, headers: response.headers });
          }),
        ),
      );
    }
  };
};
