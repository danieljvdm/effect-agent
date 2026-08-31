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
  compileRegistrations,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  ProducerId,
  operationAuthorizerLayer,
  ToolReconciler,
  type AgentRegistration,
  type ConversationStore,
  type DigestError,
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
  type DoStorageFailpoint,
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
 * Raw (unvalidated) construction options for `ConversationObject.make`, mirroring
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
}

/** Services supplied before the application graph is built, including its dependencies. */
export type CloudflareBootstrapServices =
  | CloudflareDurableRuntimeConfig
  | ConversationObjectIdentity
  | DurableRuntimeConfig
  | Crypto.Crypto
  | DoStorageFailpoint
  | DurableRuntimeFailpoint
  | ConversationMaintenanceFailpoint
  | RunContextPreparation
  | RunToolAuthorization
  | ToolReconciler;

/** Every construction failure of the assembled Cloudflare durable runtime stack. */
export type CloudflareDurableRuntimeInitializationError =
  | CloudflarePlatformConfigError
  | DigestError
  | DoStorageInitializationError;

/** The services `ConversationObject.layer` provides. */
export type CloudflareDurableRuntimeServices =
  | DurableAgentRuntime
  | SubmissionLedger
  | ConversationStore
  | WakeScheduler
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

/**
 * Validate deployment settings and derive services before building application dependencies.
 * The native class factory builds this Layer inside its constructor gate. Custom Effect hosts
 * can provide it around the complete application Layer with the native context already supplied.
 */
export const layerConfig = (
  options: CloudflareDurableRuntimeOptions,
): Layer.Layer<CloudflareBootstrapServices, CloudflarePlatformConfigError, DurableObjectContext> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const { ctx } = yield* DurableObjectContext;
      const config = yield* configFromOptions(options);
      const conversationId = yield* conversationIdFromState(ctx);
      const producerId = yield* decodeProducerId(`${config.producerPrefix}:${conversationId}`).pipe(
        Effect.mapError((error) =>
          CloudflarePlatformConfigError.make({
            message: `The minted producer identity is invalid: ${error.message}`,
            cause: error,
          }),
        ),
      );
      return Layer.mergeAll(
        Layer.succeed(CloudflareDurableRuntimeConfig, config),
        Layer.succeed(ConversationObjectIdentity, { conversationId, producerId }),
        DurableRuntimeConfig.layer({
          deploymentId: config.deploymentId,
          producerId,
          settlementPollInterval: Duration.millis(config.settlementPollInterval),
          leaseRenewalInterval: Duration.millis(config.leaseRenewalInterval),
          abortPollInterval: Duration.millis(config.abortPollInterval),
          ...(options.estimateCostMicrousd === undefined
            ? {}
            : { estimateCostMicrousd: options.estimateCostMicrousd }),
        }),
        BrowserCrypto.layer,
        storageFailpointLayer({ storage: ctx.storage, failpoint: options.storageFailpoint?.(ctx) }),
        options.runtimeFailpoint === undefined
          ? DurableRuntimeFailpoint.layer
          : Layer.succeed(DurableRuntimeFailpoint, { hit: options.runtimeFailpoint(ctx) }),
        options.maintenanceFailpoint === undefined
          ? ConversationMaintenanceFailpoint.layer
          : Layer.succeed(ConversationMaintenanceFailpoint, {
              hit: options.maintenanceFailpoint(ctx),
            }),
        options.toolReconciler ?? ToolReconciler.uncertain,
        options.operationAuthorizer === undefined
          ? Layer.empty
          : operationAuthorizerLayer(options.operationAuthorizer),
        options.toolFailureObserver === undefined
          ? Layer.succeed(CurrentToolFailureObserver, undefined)
          : toolFailureObserverLayer(options.toolFailureObserver),
        RunContextPreparationPassthrough,
        RunToolAuthorization.allowAll,
      );
    }),
  );

/**
 * Register typed Agents and version declarations. Hashing and dependency capture happen in
 * this Layer's Scope, after application Layers have been provided. Every Agent's instruction,
 * Tool, Schema, and model requirements remain visible until satisfied by Layer composition.
 * Use Layer.unwrap for registration values that need effectful application setup.
 */
export const layer = <const Entries extends ReadonlyArray<AgentRegistration>>(
  registrations: Entries,
) => Layer.unwrap(Effect.map(compileRegistrations(registrations), layerFromBindings));

/** Internal assembly shared by registration compilation and low-level adapter fixtures. */
export const layerFromBindings = (
  bindings: ReadonlyArray<ResolvedBinding>,
): Layer.Layer<
  CloudflareDurableRuntimeServices,
  DoStorageInitializationError,
  DurableObjectContext | ConversationObjectNamespace | CloudflareBootstrapServices
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const { ctx } = yield* DurableObjectContext;
      const config = yield* CloudflareDurableRuntimeConfig;
      const { conversationId } = yield* ConversationObjectIdentity;
      const storageOptions: DoStorageOptions = {
        storage: ctx.storage,
        observationPollInterval: config.observationPollInterval,
        ownershipLeaseDuration: config.ownershipLeaseDuration,
        maxStoredValueBytes: config.maxStoredValueBytes,
        verifyOnOpen: config.verifyOnOpen,
      };
      const infrastructure = Layer.mergeAll(
        storageConfigLayer(storageOptions),
        SqliteClient.layer({ storage: ctx.storage }),
      );

      // The same local ports serve routed decorators and owner-side RPC execution.
      // The RPC executor must never receive routed ports and bounce requests between Objects.
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
      const base = Layer.mergeAll(DurableAlarmService.layer, ProgressWaitRegistry.layer);
      const runtimeStack = DurableAgentRuntime.layerWithServices.pipe(
        Layer.provideMerge(routedPorts),
        Layer.provideMerge(cloudflareWakeSchedulerLayer),
        Layer.provideMerge(base),
      );
      return Layer.mergeAll(
        runtimeStack,
        ConversationMaintenance.layer(bindings).pipe(Layer.provide(runtimeStack)),
        portsEndpointLayer,
      );
    }),
  );
