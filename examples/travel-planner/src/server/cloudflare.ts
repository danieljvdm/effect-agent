import type { Agent } from "@yielded/agent";
import { ThreadMaintenance } from "@yielded/agent-platform-alchemy-cloudflare/alarm";
import { ThreadObjectIdentity } from "@yielded/agent-platform-alchemy-cloudflare/cloudflare-bindings";
import {
  CloudflareBrowser,
  type CloudflareBrowserOptions,
} from "@yielded/agent-platform-alchemy-cloudflare/cloudflare-browser";
import * as ThreadObject from "@yielded/agent-platform-alchemy-cloudflare/thread-object";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { DefinitionDigestInput } from "@yielded/agent/records";
import { SubmissionLedger, SubmissionLookupById } from "@yielded/agent/submission-ledger";
import { makeDurableObjectBridge } from "alchemy/Cloudflare/Bridge";
import { DurableObject as AlchemyDurableObject } from "alchemy/Cloudflare/Workers/DurableObject";
import { Worker } from "alchemy/Cloudflare/Workers/Worker";
import type { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, Option, Schema } from "effect";
import type { Tool } from "effect/ai";
import { FetchHttpClient, HttpRouter, HttpServerResponse } from "effect/http";
import { RpcSerialization, RpcServer } from "effect/rpc";

import { TripToolsLive } from "../agent.ts";
import type { TripSiteStore } from "../domain.ts";
import {
  PlannerError,
  PlannerInput,
  PlannerRpcs,
  PlannerSnapshot,
  PlannerWorkerDetail,
  PlannerProgress,
  defaultPlannerSettings,
} from "../domain.ts";
import { ReadTravelPageLive } from "../research.ts";
import { ResearchAuthorizationLive, scoutAttemptLayer } from "../research/runtime.ts";
import {
  updatingResearchScout,
  UpdatingResearchScoutBackground,
  UpdatingResearchScoutActions,
} from "../research/scout.ts";
import { AppBuildBucketLive } from "../trip-app/bindings.ts";
import { EditorHostLive, editorAttemptLayer } from "../trip-app/editor-runtime.ts";
import {
  ReportingAppEditorBackground,
  ReportingAppEditorActions,
  appEditor,
} from "../trip-app/editor.ts";
import { OwnerAppRepositoryLive, serveAppRepository } from "../trip-app/remote.ts";
import {
  AppSourceLive,
  createTripApp,
  restoreTripApp,
  retryTripAppBuild,
} from "../trip-app/service.ts";
import { AppToolsLive } from "../trip-app/tools-live.ts";
import { plannerEnvironment, runtimeStack } from "./alchemy.ts";
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
import { planner } from "./planner.ts";
import { PlannerAttempt, ProgressStore } from "./progress.ts";
import { PlannerSettingsStore, PlannerSettingsStoreLive } from "./settings.ts";
import { ownerOfThread, privateConversation, publicSnapshot } from "./tenancy.ts";
import { OwnerTripRepositoryLive, serveTripRepository } from "./trip-rpc.ts";
import { publishTrip, TripRepository } from "./trips.ts";
import { plannerWorker, workerStatus, WorkerLocator, WorkerStatusRequest } from "./worker-state.ts";

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
  Effect.map(plannerEnvironment, credentialSourceLayer),
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
  GetPlannerWorker: ({ conversationId, ...locator }) =>
    safeRpc(
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const privateId = yield* privateConversation(identity.threadId, conversationId);
        const env = yield* plannerEnvironment;

        const request = yield* Schema.encodeEffect(Schema.fromJsonString(WorkerLocator))(
          locator,
        ).pipe(
          Effect.mapError(() => new PlannerError({ code: "invalid", message: "Invalid worker." })),
        );

        const reply = yield* Effect.tryPromise({
          try: () => env.ACCOUNT_THREADS.getByName(privateId).plannerWorker(request),
          catch: () =>
            new PlannerError({
              code: "unavailable",
              message: "Worker updates are temporarily unavailable.",
            }),
        });

        return yield* Schema.decodeEffect(Schema.fromJsonString(PlannerWorkerDetail))(reply).pipe(
          Effect.mapError(
            () =>
              new PlannerError({
                code: "unavailable",
                message: "Worker updates could not be read.",
              }),
          ),
        );
      }),
    ),
  GetPlanner: ({ conversationId }) =>
    safeRpc(
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;

        if (conversationId === null)
          return publicSnapshot(identity.threadId, yield* plannerSnapshot(null));
        const privateId = yield* privateConversation(identity.threadId, conversationId);
        const env = yield* plannerEnvironment;

        const reply = yield* Effect.tryPromise({
          try: () => env.ACCOUNT_THREADS.getByName(privateId).plannerState(),
          catch: () =>
            new PlannerError({
              code: "unavailable",
              message: "This conversation is temporarily unavailable.",
            }),
        });

        const snapshot = yield* Schema.decodeEffect(Schema.fromJsonString(PlannerSnapshot))(
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
) => {
  const attemptLayer = (context: {
    readonly threadId: string;
    readonly submissionId: SubmissionLookupById["submissionId"];
    readonly attemptId: string;
  }) =>
    Layer.mergeAll(
      TripToolsLive(context.threadId),
      AppToolsLive,
      ReportingAppEditorActions.layer,
      UpdatingResearchScoutActions.layer,
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
                    : Schema.decodeUnknownEffect(PlannerInput)(found.value.inputPayload).pipe(
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
      model,
      definitions: DefinitionDigestInput.make({
        agent: { id: planner.id, version: "travel-planner-v17" },
        model: modelVersion,
        tools: Object.keys(planner.toolkit.tools),
      }),
      attemptLayer,
    },
    {
      agent: updatingResearchScout,
      model,
      definitions: DefinitionDigestInput.make({
        agent: { id: updatingResearchScout.id, version: "travel-research-scout-v5" },
        model: modelVersion,
        tools: Object.keys(updatingResearchScout.toolkit.tools),
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
      model,
      definitions: DefinitionDigestInput.make({
        agent: { id: appEditor.id, version: "trip-app-editor-v1" },
        model: modelVersion,
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
  ]).pipe(
    // Reports have no parent attempt. Keep their projection services in the registration context.
    Layer.provide(UpdatingResearchScoutBackground.layer),
    Layer.provide(ReportingAppEditorBackground.layer),
    Layer.provide(browser),
    Layer.provide(EditorHostLive),
    Layer.provide(ResearchAuthorizationLive),
    Layer.provide(DiagnosticObserverLive),
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
        const env = yield* plannerEnvironment;
        const identity = yield* ThreadObjectIdentity;

        return credentialStoreLayer(env, identity.threadId);
      }),
    ),
    CredentialSourceLive,
    FailureDiagnosticsLive,
    AppSourceLive,
  ).pipe(Layer.provideMerge(ThreadObject.layer([])));

  return Layer.fresh(ThreadMaintenance.layer).pipe(
    Layer.provideMerge(registered),
    // Read RPCs use the same authorization policy as the registered worker host.
    Layer.provideMerge(EditorHostLive),
    Layer.provideMerge(local),
    Layer.provideMerge(ProgressStore.layer),
    Layer.provideMerge(Layer.succeed(PlannerModel, { model: modelLabel })),
  );
};

const PlannerLive = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* plannerEnvironment;

    if (env.BROWSER === undefined)
      return yield* new PlannerError({
        code: "unavailable",
        message: "The planner requires a Browser Run binding.",
      });

    const credentials: Layer.Layer<CredentialSource> = credentialSourceLayer(env);

    return plannerApplication(
      liveModel.pipe(Layer.provide(credentials)),
      "openai-selectable-v1",
      defaultPlannerSettings.model,
      CloudflareBrowser.layer({ handlers: ReadTravelPageLive }, { browser: env.BROWSER }),
    );
  }),
);

/** Private namespace RPCs; authenticated ingress and source authorization own access. */
export const plannerRpc = {
  modelCredential: () =>
    Effect.flatMap(CredentialStore, (store) => store.sealed).pipe(
      Effect.flatMap(encodeStoredCredential),
    ),
  plannerProgress: () =>
    Effect.flatMap(ProgressStore, (progress) => progress.read).pipe(
      Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(PlannerProgress))),
    ),
  plannerDiagnostics: () =>
    readDiagnostics.pipe(
      Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(RecordedDiagnostics))),
    ),
  tripRepository: (request: string) => serveTripRepository(request),
  tripApp: (request: string) => serveAppRepository(request),
  plannerWorker: (request: string) =>
    Schema.decodeEffect(Schema.fromJsonString(WorkerLocator))(request).pipe(
      Effect.flatMap(plannerWorker),
      Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(PlannerWorkerDetail))),
    ),
  plannerWorkerStatus: (request: string) =>
    Schema.decodeEffect(Schema.fromJsonString(WorkerStatusRequest))(request).pipe(
      Effect.flatMap(workerStatus),
      Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(PlannerWorkerDetail))),
    ),
  plannerState: () =>
    Effect.gen(function* () {
      const identity = yield* ThreadObjectIdentity;
      const snapshot = yield* plannerSnapshot(identity.threadId);

      return yield* Schema.encodeEffect(Schema.fromJsonString(PlannerSnapshot))(snapshot);
    }),
  plannerFetch: (request: Request) =>
    Effect.scoped(
      Effect.gen(function* () {
        const context =
          yield* Effect.context<Exclude<Layer.Services<typeof RpcHttp>, HttpRouter.HttpRouter>>();

        const web = yield* Effect.acquireRelease(
          Effect.sync(() =>
            HttpRouter.toWebHandler(RpcHttp.pipe(Layer.provide(Layer.succeedContext(context))), {
              disableLogger: true,
            }),
          ),
          (handler) => Effect.promise(() => handler.dispose()),
        );

        const response = yield* Effect.promise(() => web.handler(request));
        // Finite RPC responses are consumed before their request-owned runtime is finalized.
        const body = yield* Effect.promise(() => response.arrayBuffer());

        return new Response(body, { status: response.status, headers: response.headers });
      }),
    ),
};

type ApplicationServices = Layer.Success<typeof PlannerLive> | TripSiteStore | WorkerEnvironment;

type NativeRpc<Handlers> = {
  [Key in keyof Handlers]: Handlers[Key] extends (
    ...args: infer Args
  ) => Effect.Effect<infer Value, infer _Error, infer _Requirements>
    ? (...args: Args) => Promise<Value>
    : never;
};

/** The application RPCs and framework handlers share one Alchemy-owned Object runtime. */
export const makeTravelPlannerThread = <
  E,
  Handlers extends ThreadObject.Handlers<ApplicationServices> = {},
>(
  sites: Layer.Layer<TripSiteStore, E, WorkerEnvironment>,
  application = PlannerLive,
  ownership: Pick<ThreadObject.Options, "ownershipLeaseDuration" | "leaseRenewalInterval"> = {},
  handlers?: Handlers,
) => {
  const rpc = { ...plannerRpc, ...handlers };

  class AccountThreads extends AlchemyDurableObject<AccountThreads, ThreadObject.Rpc<typeof rpc>>()(
    "ACCOUNT_THREADS",
  ) {}

  const live = AccountThreads.make(
    ThreadObject.make(
      application.pipe(Layer.provideMerge(sites)),
      {
        ...ownership,
        namespaceBinding: "ACCOUNT_THREADS",
        deploymentId: "travel-planner-v1",
        producerPrefix: "travel-planner",
        settlementPollInterval: 100,
        maxQueueDepthPerLane: 8,
        maxInputBytes: 16 * 1024,
      },
      rpc,
    ),
  );

  const entrypoint = Worker(
    "PlannerObjects",
    { main: import.meta.url },
    Effect.gen(function* () {
      yield* AccountThreads;

      return { fetch: Effect.succeed(HttpServerResponse.empty({ status: 404 })) };
    }).pipe(Effect.provide(live)),
  );

  // Alchemy's public bridge emits a native class; retain the inferred RPC surface here.
  const Native: new (
    ctx: DurableObjectState,
    env: Cloudflare.Env,
  ) => DurableObject<Cloudflare.Env> & NativeRpc<ThreadObject.Rpc<typeof plannerRpc & Handlers>> =
    makeDurableObjectBridge(DurableObject, { entrypoint, stack: runtimeStack })("ACCOUNT_THREADS");

  return Native;
};
