import { ConversationId } from "@effect-agent/core";
import type {
  RunContextPreparation,
  RunCostEstimator,
  RunToolFailureObserver,
} from "@effect-agent/engine";
import {
  CurrentToolFailureObserver,
  RunContextPreparationPassthrough,
  RunToolAuthorization,
  toolFailureObserverLayer,
} from "@effect-agent/engine";
import {
  AgentBindingResolver,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  ProducerId,
  operationAuthorizerLayer,
  ToolReconciler,
  type ConversationStore,
  type DurableRuntimeFailpointHandler,
  type OperationAuthorizerService,
  type ResolvedBinding,
  type SubmissionLedger,
  type WakeScheduler,
} from "@effect-agent/session";
import {
  conversationStoreLayer,
  executePortRequest,
  routedConversationStoreLayer,
  routedSubmissionLedgerLayer,
  storageConfigLayer,
  storageFailpointLayer,
  submissionLedgerLayer,
  type DoStorageFailpointHandler,
  type DoStorageInitializationError,
  type DoStorageOptions,
  type PortRequest,
  type PortResponse,
} from "@effect-agent/storage-cloudflare";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import type { Crypto } from "effect";
import { Context, Duration, Effect, Layer, Schema } from "effect";

import {
  ConversationMaintenance,
  ConversationMaintenanceFailpoint,
  DurableAlarmService,
  type ConversationMaintenanceFailpointHandler,
} from "./alarm.ts";
import {
  ConversationObjectIdentity,
  DurableObjectContext,
  type ConversationObjectNamespace,
} from "./bindings.ts";
import {
  CLOUDFLARE_RUNTIME_DEFAULTS,
  CloudflareDurableRuntimeConfig,
  CloudflareDurableRuntimeConfigValue,
  CloudflarePlatformConfigError,
} from "./config.ts";
import { ProgressWaitRegistry } from "./progress-wait.ts";
import { conversationPortTransportLayer } from "./transport.ts";
import { cloudflareWakeSchedulerLayer } from "./wake-scheduler.ts";

/**
 * Raw (unvalidated) construction options for `CloudflareDurableRuntime.layer`, mirroring
 * `NodeDurableRuntimeOptions`. Optional fields default to the documented production values
 * (`CLOUDFLARE_RUNTIME_DEFAULTS`); everything is schema-decoded into
 * `CloudflareDurableRuntimeConfigValue` before any resource opens (deployment §5 gate 1).
 */
export interface CloudflareDurableRuntimeOptions {
  readonly deploymentId: string;
  /** Head of the minted producer identity `{producerPrefix}:{conversationId}`. */
  readonly producerPrefix: string;
  /** Milliseconds; default 30s (D5). */
  readonly ownershipLeaseDuration?: number | undefined;
  /** Milliseconds; default 100. */
  readonly alarmBackoffBase?: number | undefined;
  /** Milliseconds; default 5000. */
  readonly alarmBackoffCap?: number | undefined;
  /** Milliseconds; default 1000. Bounds every alarm re-arm delay. */
  readonly wakeScanInterval?: number | undefined;
  /** Milliseconds; default 500. */
  readonly settlementPollInterval?: number | undefined;
  /** Milliseconds; default 10000. */
  readonly leaseRenewalInterval?: number | undefined;
  /** Milliseconds; default 500. */
  readonly abortPollInterval?: number | undefined;
  /** Deployment-owned pricing authority used by durable cost budgets and settlements. */
  readonly estimateCostMicrousd?: RunCostEstimator | undefined;
  /** Closed trusted Tool failure reporting. Omission masks ambient observers at construction. */
  readonly toolFailureObserver?: RunToolFailureObserver | undefined;
  /** Milliseconds; default 25. */
  readonly observationPollInterval?: number | undefined;
  /** Bytes; default just under the 2 MB platform value limit. */
  readonly maxStoredValueBytes?: number | undefined;
  /** Default false. */
  readonly verifyOnOpen?: boolean | undefined;
  /** Nonterminal Submissions per lane before admission refuses; default 256. */
  readonly maxQueueDepthPerLane?: number | undefined;
  /** Encoded input bytes per Submission; default = the stored-value bound. */
  readonly maxInputBytes?: number | undefined;
  /** `ctx.storage.sql.databaseSize` ceiling at admission; default 9 GB (10 GB platform cap). */
  readonly maxDatabaseBytes?: number | undefined;
  /**
   * Durable Object storage fault injection (`ledger:*` / `append:*` locations). Handlers are
   * constructed per incarnation WITH the live `DurableObjectState`, so eviction harnesses can
   * map an armed hit to `ctx.abort()` — the platform's real failure mode. Default none.
   */
  readonly storageFailpoint?: ((ctx: DurableObjectState) => DoStorageFailpointHandler) | undefined;
  /** Coordinator fault injection (`submit:*` / `terminalize:*` locations); default none. */
  readonly runtimeFailpoint?:
    | ((ctx: DurableObjectState) => DurableRuntimeFailpointHandler)
    | undefined;
  /** Conversation-maintenance generation/alarm fault injection; default none. */
  readonly maintenanceFailpoint?:
    | ((ctx: DurableObjectState) => ConversationMaintenanceFailpointHandler)
    | undefined;
  /** Host-supplied fail-closed authorization policy; defaults to service possession. */
  readonly operationAuthorizer?: OperationAuthorizerService | undefined;
  /**
   * Reconciliation policy consulted for open ordinary Tool Calls before an Unknown Outcome
   * is recorded (durability §10, DUR-009). Defaults to the fail-closed
   * `ToolReconciler.uncertain`.
   */
  readonly toolReconciler?: Layer.Layer<ToolReconciler> | undefined;
  /**
   * Registered worker Bindings resolved at durable claim time:
   * build each with `DurableWorkerBinding.make(binding, digests)`. The callback receives the live
   * Object context and derived identities and is evaluated once per incarnation during Layer
   * construction. Defaults to the empty registration (every resolved claim fails closed).
   */
  readonly bindings?: CloudflareBindingSource | undefined;
  /**
   * Prompt preparation/compaction acquired with this Durable Object incarnation. The Layer may depend
   * only on `Crypto.Crypto`, which this platform supplies with `BrowserCrypto`; hosts must close
   * every application-specific service before passing it here. Default pass-through.
   */
  readonly runContext?: CloudflareRunContextSource | undefined;
  /**
   * Independent action-time Tool authorization acquired once per incarnation, including after
   * eviction. Close application dependencies before passing the Layer; default allow-all.
   */
  readonly toolAuthorization?: CloudflareToolAuthorizationSource | undefined;
}

/** Per-incarnation host values available to Effect-native runtime extension factories. */
export interface CloudflareRuntimeSourceContext {
  readonly ctx: DurableObjectState;
  readonly env: unknown;
  readonly conversationId: ConversationId;
  readonly producerId: ProducerId;
}

/** Per-incarnation host values available while registered worker Bindings are captured. */
export interface CloudflareBindingSourceContext extends CloudflareRuntimeSourceContext {}

/** Captures registered worker Bindings once for each Durable Object incarnation. */
export type CloudflareBindingSource = (
  context: CloudflareBindingSourceContext,
) => Effect.Effect<ReadonlyArray<ResolvedBinding>, never, never>;

/** Prompt preparation and compaction captured once; only platform Crypto may remain. */
export type CloudflareRunContextLayer = Layer.Layer<RunContextPreparation, never, Crypto.Crypto>;

/** One Layer or a per-incarnation factory over explicit Cloudflare host values. */
export type CloudflareRunContextSource =
  | CloudflareRunContextLayer
  | ((context: CloudflareRuntimeSourceContext) => CloudflareRunContextLayer);

/** Action-time Tool policy captured separately from prompt preparation. */
export type CloudflareToolAuthorizationLayer = Layer.Layer<
  RunToolAuthorization,
  never,
  Crypto.Crypto
>;

/** One Layer or a per-incarnation factory over explicit Cloudflare host values. */
export type CloudflareToolAuthorizationSource =
  | CloudflareToolAuthorizationLayer
  | ((context: CloudflareRuntimeSourceContext) => CloudflareToolAuthorizationLayer);

/** Every construction failure of the assembled Cloudflare durable runtime stack. */
export type CloudflareDurableRuntimeInitializationError =
  | CloudflarePlatformConfigError
  | DoStorageInitializationError;

/** The services `CloudflareDurableRuntime.layer` provides. */
export type CloudflareDurableRuntimeServices =
  | DurableAgentRuntime
  | SubmissionLedger
  | ConversationStore
  | WakeScheduler
  | DurableRuntimeConfig
  | AgentBindingResolver
  | CloudflareDurableRuntimeConfig
  | ConversationObjectIdentity
  | DurableAlarmService
  | ConversationMaintenance
  | ConversationObjectPorts
  | ProgressWaitRegistry;

/**
 * Owner-side execution port for a `portCall` request the wire endpoint has already decoded.
 * It executes against THIS Object's LOCAL port facets — never the routed decorators, so a
 * request cannot bounce between Objects — and returns the typed response for the endpoint to
 * encode.
 */
export class ConversationObjectPorts extends Context.Service<
  ConversationObjectPorts,
  {
    readonly handle: (request: PortRequest) => Effect.Effect<PortResponse>;
  }
>()("@effect-agent/platform-cloudflare/ConversationObjectPorts") {}

const decodeConfigValue = Schema.decodeUnknownEffect(CloudflareDurableRuntimeConfigValue);
const decodeConversationId = Schema.decodeUnknownEffect(ConversationId);
const decodeProducerId = Schema.decodeUnknownEffect(ProducerId);

const configFromOptions = (
  options: CloudflareDurableRuntimeOptions,
): Effect.Effect<CloudflareDurableRuntimeConfigValue, CloudflarePlatformConfigError> =>
  decodeConfigValue({
    deploymentId: options.deploymentId,
    producerPrefix: options.producerPrefix,
    ownershipLeaseDuration:
      options.ownershipLeaseDuration ?? CLOUDFLARE_RUNTIME_DEFAULTS.ownershipLeaseDuration,
    alarmBackoffBase: options.alarmBackoffBase ?? CLOUDFLARE_RUNTIME_DEFAULTS.alarmBackoffBase,
    alarmBackoffCap: options.alarmBackoffCap ?? CLOUDFLARE_RUNTIME_DEFAULTS.alarmBackoffCap,
    wakeScanInterval: options.wakeScanInterval ?? CLOUDFLARE_RUNTIME_DEFAULTS.wakeScanInterval,
    settlementPollInterval:
      options.settlementPollInterval ?? CLOUDFLARE_RUNTIME_DEFAULTS.settlementPollInterval,
    leaseRenewalInterval:
      options.leaseRenewalInterval ?? CLOUDFLARE_RUNTIME_DEFAULTS.leaseRenewalInterval,
    abortPollInterval: options.abortPollInterval ?? CLOUDFLARE_RUNTIME_DEFAULTS.abortPollInterval,
    observationPollInterval:
      options.observationPollInterval ?? CLOUDFLARE_RUNTIME_DEFAULTS.observationPollInterval,
    maxStoredValueBytes:
      options.maxStoredValueBytes ?? CLOUDFLARE_RUNTIME_DEFAULTS.maxStoredValueBytes,
    verifyOnOpen: options.verifyOnOpen ?? CLOUDFLARE_RUNTIME_DEFAULTS.verifyOnOpen,
    limits: {
      maxQueueDepthPerLane:
        options.maxQueueDepthPerLane ?? CLOUDFLARE_RUNTIME_DEFAULTS.maxQueueDepthPerLane,
      maxInputBytes: Math.min(
        options.maxInputBytes ?? CLOUDFLARE_RUNTIME_DEFAULTS.maxInputBytes,
        options.maxStoredValueBytes ?? CLOUDFLARE_RUNTIME_DEFAULTS.maxStoredValueBytes,
      ),
      maxDatabaseBytes: options.maxDatabaseBytes ?? CLOUDFLARE_RUNTIME_DEFAULTS.maxDatabaseBytes,
    },
  }).pipe(
    Effect.mapError((error) =>
      CloudflarePlatformConfigError.make({
        message: `Invalid Cloudflare durable runtime configuration: ${error.message}`,
        cause: error,
      }),
    ),
  );

/**
 * The Conversation this Object owns, from the Object identity rule (plan §1.2): Conversation
 * Objects are addressed exclusively by `idFromName(conversationId)`, so `ctx.id.name` IS the
 * Conversation ID. An unnamed Object (from `newUniqueId`) is a deployment error, not a lane.
 */
const conversationIdFromState = (
  ctx: DurableObjectState,
): Effect.Effect<ConversationId, CloudflarePlatformConfigError> =>
  ctx.id.name === undefined
    ? Effect.fail(
        CloudflarePlatformConfigError.make({
          message:
            "This Durable Object was not created via idFromName(conversationId); Conversation " +
            "Objects must be addressed by their Conversation identity (plan §1.2).",
        }),
      )
    : decodeConversationId(ctx.id.name).pipe(
        Effect.mapError((error) =>
          CloudflarePlatformConfigError.make({
            message: `The Durable Object name is not a valid ConversationId: ${error.message}`,
            cause: error,
          }),
        ),
      );

const resolveBindings = (
  source: CloudflareDurableRuntimeOptions["bindings"],
  context: CloudflareBindingSourceContext,
): Effect.Effect<ReadonlyArray<ResolvedBinding>> =>
  source === undefined ? Effect.succeed([]) : Effect.suspend(() => source(context));

const resolveRunContext = (
  source: CloudflareRunContextSource,
  context: CloudflareRuntimeSourceContext,
): CloudflareRunContextLayer => (typeof source === "function" ? source(context) : source);

/**
 * The DC Layer assembly (deployment §12: a Layer-assembly library, not an app entrypoint;
 * plan §1.4). `layer(options)` decodes the configuration, derives this Object's Conversation
 * and producer identities, opens the Object's private SQLite database through
 * `@effect/sql-sqlite-do` for BOTH the Conversation Log and the Submission Ledger (so claims
 * fence the same producer epochs — ADR-0011 D7 transposed), wraps the local port facets with
 * the WP2 cross-Object routing decorators over the Durable Object RPC transport, wires the
 * alarm-backed wake scheduler and the maintenance pass, defaults reconciliation to the
 * fail-closed `ToolReconciler.uncertain`, and provides a ready `DurableAgentRuntime` on top.
 *
 * Storage compatibility is verified during construction: an incompatible database fails the
 * Layer typed (`DoStorageCompatibilityError`) before anything is mutated (DEPLOY-008).
 *
 * Requires only the two binding services (`DurableObjectContext`,
 * `ConversationObjectNamespace`) — platform values enter exclusively through Layers
 * (DEPLOY-010).
 */
export class CloudflareDurableRuntime {
  static layer(
    options: CloudflareDurableRuntimeOptions,
  ): Layer.Layer<
    CloudflareDurableRuntimeServices,
    CloudflareDurableRuntimeInitializationError,
    DurableObjectContext | ConversationObjectNamespace
  > {
    return Layer.unwrap(
      Effect.gen(function* () {
        const { ctx, env } = yield* DurableObjectContext;
        const config = yield* configFromOptions(options);
        const conversationId = yield* conversationIdFromState(ctx);
        const producerId = yield* decodeProducerId(
          `${config.producerPrefix}:${conversationId}`,
        ).pipe(
          Effect.mapError((error) =>
            CloudflarePlatformConfigError.make({
              message: `The minted producer identity is invalid: ${error.message}`,
              cause: error,
            }),
          ),
        );

        const identityLayer = Layer.succeed(ConversationObjectIdentity)({
          conversationId,
          producerId,
        });
        const cloudflareConfigLayer = Layer.succeed(CloudflareDurableRuntimeConfig)(config);

        const storageOptions: DoStorageOptions = {
          storage: ctx.storage,
          observationPollInterval: config.observationPollInterval,
          ownershipLeaseDuration: config.ownershipLeaseDuration,
          maxStoredValueBytes: config.maxStoredValueBytes,
          verifyOnOpen: config.verifyOnOpen,
          failpoint: options.storageFailpoint?.(ctx),
        };
        const infrastructure = Layer.mergeAll(
          storageConfigLayer(storageOptions),
          storageFailpointLayer(storageOptions),
          SqliteClient.layer({ storage: ctx.storage }),
          BrowserCrypto.layer,
        );

        /**
         * ONE local-facet instance (a shared Layer value) serves both the routed decorators
         * and the owner-side `portCall` executor — the executor must never see the routed
         * ports (plan §1.3: re-routing could bounce a request between Objects).
         */
        const localPorts = Layer.mergeAll(conversationStoreLayer, submissionLedgerLayer).pipe(
          Layer.provide(infrastructure),
        );

        const portsEndpointLayer = Layer.effect(ConversationObjectPorts)(
          Effect.gen(function* () {
            const local = yield* Effect.context<SubmissionLedger | ConversationStore>();
            return ConversationObjectPorts.of({
              handle: (request) => executePortRequest(request).pipe(Effect.provide(local)),
            });
          }),
        ).pipe(Layer.provide(localPorts));

        const routedPorts = Layer.mergeAll(
          routedSubmissionLedgerLayer({ localConversationId: conversationId }),
          routedConversationStoreLayer({ localConversationId: conversationId }),
        ).pipe(Layer.provide(localPorts), Layer.provide(conversationPortTransportLayer));

        const runtimeConfigLayer = Layer.succeed(DurableRuntimeConfig)(
          DurableRuntimeConfig.make({
            deploymentId: config.deploymentId,
            producerId,
            settlementPollInterval: Duration.millis(config.settlementPollInterval),
            leaseRenewalInterval: Duration.millis(config.leaseRenewalInterval),
            abortPollInterval: Duration.millis(config.abortPollInterval),
            ...(options.estimateCostMicrousd === undefined
              ? {}
              : { estimateCostMicrousd: options.estimateCostMicrousd }),
          }),
        );

        const runtimeFailpointLayer =
          options.runtimeFailpoint === undefined
            ? DurableRuntimeFailpoint.layer
            : Layer.succeed(DurableRuntimeFailpoint)({ hit: options.runtimeFailpoint(ctx) });
        const maintenanceFailpointLayer =
          options.maintenanceFailpoint === undefined
            ? ConversationMaintenanceFailpoint.layer
            : Layer.succeed(ConversationMaintenanceFailpoint)({
                hit: options.maintenanceFailpoint(ctx),
              });
        const reconcilerLayer = options.toolReconciler ?? ToolReconciler.uncertain;
        const authorizerLayer =
          options.operationAuthorizer === undefined
            ? Layer.empty
            : operationAuthorizerLayer(options.operationAuthorizer);
        const observerLayer =
          options.toolFailureObserver === undefined
            ? Layer.succeed(CurrentToolFailureObserver)(undefined)
            : toolFailureObserverLayer(options.toolFailureObserver);
        const bindingResolverLayer = Layer.effect(AgentBindingResolver)(
          Effect.map(
            resolveBindings(options.bindings, { ctx, env, conversationId, producerId }),
            (bindings) => AgentBindingResolver.fromBindings(bindings),
          ),
        );
        const runContextLayer =
          options.runContext === undefined
            ? RunContextPreparationPassthrough
            : resolveRunContext(options.runContext, { ctx, env, conversationId, producerId }).pipe(
                Layer.provide(BrowserCrypto.layer),
              );
        const toolAuthorizationSource = options.toolAuthorization;
        const toolAuthorizationLayer =
          toolAuthorizationSource === undefined
            ? RunToolAuthorization.allowAll
            : (typeof toolAuthorizationSource === "function"
                ? toolAuthorizationSource({ ctx, env, conversationId, producerId })
                : toolAuthorizationSource
              ).pipe(Layer.provide(BrowserCrypto.layer));

        const base = Layer.mergeAll(
          identityLayer,
          cloudflareConfigLayer,
          DurableAlarmService.layer,
          maintenanceFailpointLayer,
          ProgressWaitRegistry.layer,
        );

        const runtimeStack = DurableAgentRuntime.layerWithContext.pipe(
          Layer.provideMerge(routedPorts),
          Layer.provideMerge(cloudflareWakeSchedulerLayer),
          Layer.provideMerge(runtimeConfigLayer),
          Layer.provideMerge(bindingResolverLayer),
          Layer.provide(
            Layer.mergeAll(
              runtimeFailpointLayer,
              reconcilerLayer,
              authorizerLayer,
              observerLayer,
              runContextLayer,
              toolAuthorizationLayer,
              BrowserCrypto.layer,
            ),
          ),
          Layer.provideMerge(base),
        );

        return Layer.mergeAll(
          runtimeStack,
          ConversationMaintenance.layer.pipe(Layer.provide(runtimeStack)),
          portsEndpointLayer,
        );
      }),
    );
  }
}
