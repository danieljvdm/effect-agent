import { ThreadId } from "@effect-agent/core";
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
  threadStoreLayer,
  executePortRequest,
  routedThreadStoreLayer,
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
import {
  compileRegistrations,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  ProducerId,
  operationAuthorizerLayer,
  ToolReconciler,
  type AgentRegistration,
  type ThreadStore,
  type DigestError,
  type DurableRuntimeFailpointHandler,
  type OperationAuthorizerService,
  type ResolvedBinding,
  type SubmissionLedger,
  type WakeScheduler,
} from "@effect-agent/thread";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import type { Crypto } from "effect";
import { Context, Duration, Effect, Layer, Schema } from "effect";

import {
  ThreadMaintenance,
  ThreadMaintenanceFailpoint,
  DurableAlarmService,
  type ThreadMaintenanceFailpointHandler,
} from "./alarm.ts";
import {
  ThreadObjectIdentity,
  DurableObjectContext,
  type ThreadObjectNamespace,
} from "./bindings.ts";
import {
  CLOUDFLARE_RUNTIME_DEFAULTS,
  CloudflareDurableRuntimeConfig,
  CloudflareDurableRuntimeConfigValue,
  CloudflarePlatformConfigError,
} from "./config.ts";
import { ProgressWaitRegistry } from "./progress-wait.ts";
import { threadPortTransportLayer } from "./transport.ts";
import { cloudflareWakeSchedulerLayer } from "./wake-scheduler.ts";

/**
 * Raw (unvalidated) construction options for `ThreadObject.make`, mirroring
 * `NodeDurableAgentRuntimeOptions`. Optional fields default to the documented production values
 * (`CLOUDFLARE_RUNTIME_DEFAULTS`); everything is schema-decoded into
 * `CloudflareDurableRuntimeConfigValue` before any resource opens (deployment §5 gate 1).
 */
export interface CloudflareDurableRuntimeOptions {
  readonly deploymentId: string;
  /** Head of the minted producer identity `{producerPrefix}:{threadId}`. */
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
  /** Thread-maintenance generation/alarm fault injection; default none. */
  readonly maintenanceFailpoint?:
    | ((ctx: DurableObjectState) => ThreadMaintenanceFailpointHandler)
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
  | ThreadObjectIdentity
  | DurableRuntimeConfig
  | Crypto.Crypto
  | DoStorageFailpoint
  | DurableRuntimeFailpoint
  | ThreadMaintenanceFailpoint
  | RunContextPreparation
  | RunToolAuthorization
  | ToolReconciler;

/** Every construction failure of the assembled Cloudflare durable runtime stack. */
export type CloudflareDurableRuntimeInitializationError =
  | CloudflarePlatformConfigError
  | DigestError
  | DoStorageInitializationError;

/** The services `ThreadObject.layer` provides. */
export type CloudflareDurableRuntimeServices =
  | DurableAgentRuntime
  | SubmissionLedger
  | ThreadStore
  | WakeScheduler
  | DurableAlarmService
  | ThreadMaintenance
  | ThreadObjectPorts
  | ProgressWaitRegistry;

/**
 * Owner-side execution port for a `portCall` request the wire endpoint has already decoded.
 * It executes against THIS Object's LOCAL port facets — never the routed decorators, so a
 * request cannot bounce between Objects — and returns the typed response for the endpoint to
 * encode.
 */
export class ThreadObjectPorts extends Context.Service<
  ThreadObjectPorts,
  {
    readonly handle: (request: PortRequest) => Effect.Effect<PortResponse>;
  }
>()("@effect-agent/platform-cloudflare/ThreadObjectPorts") {}

const decodeConfigValue = Schema.decodeUnknownEffect(CloudflareDurableRuntimeConfigValue);
const decodeThreadId = Schema.decodeUnknownEffect(ThreadId);
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
 * The Thread this Object owns, from the Object identity rule (plan §1.2): Thread
 * Objects are addressed exclusively by `idFromName(threadId)`, so `ctx.id.name` IS the
 * Thread ID. An unnamed Object (from `newUniqueId`) is a deployment error, not a lane.
 */
const threadIdFromState = (
  ctx: DurableObjectState,
): Effect.Effect<ThreadId, CloudflarePlatformConfigError> =>
  ctx.id.name === undefined
    ? Effect.fail(
        CloudflarePlatformConfigError.make({
          message:
            "This Durable Object was not created via idFromName(threadId); Thread " +
            "Objects must be addressed by their Thread identity (plan §1.2).",
        }),
      )
    : decodeThreadId(ctx.id.name).pipe(
        Effect.mapError((error) =>
          CloudflarePlatformConfigError.make({
            message: `The Durable Object name is not a valid ThreadId: ${error.message}`,
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
      const threadId = yield* threadIdFromState(ctx);

      const producerId = yield* decodeProducerId(`${config.producerPrefix}:${threadId}`).pipe(
        Effect.mapError((error) =>
          CloudflarePlatformConfigError.make({
            message: `The minted producer identity is invalid: ${error.message}`,
            cause: error,
          }),
        ),
      );

      return Layer.mergeAll(
        Layer.succeed(CloudflareDurableRuntimeConfig, config),
        Layer.succeed(ThreadObjectIdentity, { threadId, producerId }),
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
          ? ThreadMaintenanceFailpoint.layer
          : Layer.succeed(ThreadMaintenanceFailpoint, {
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

/**
 * Assemble the durable runtime from already-resolved Agent Bindings.
 * Use `ThreadObject.layer` to compile typed Agent registrations instead.
 * Supply host services through `ThreadObject.make` or `ThreadObject.layerConfig` and
 * the Durable Object context and namespace Layers when composing a custom host.
 */
export const layerFromBindings = (
  bindings: ReadonlyArray<ResolvedBinding>,
): Layer.Layer<
  CloudflareDurableRuntimeServices,
  DoStorageInitializationError,
  DurableObjectContext | ThreadObjectNamespace | CloudflareBootstrapServices
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const { ctx } = yield* DurableObjectContext;
      const config = yield* CloudflareDurableRuntimeConfig;
      const { threadId } = yield* ThreadObjectIdentity;

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
      const localPorts = Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
        Layer.provide(infrastructure),
      );

      const portsEndpointLayer = Layer.effect(ThreadObjectPorts)(
        Effect.gen(function* () {
          const local = yield* Effect.context<SubmissionLedger | ThreadStore>();

          return ThreadObjectPorts.of({
            handle: (request) => executePortRequest(request).pipe(Effect.provide(local)),
          });
        }),
      ).pipe(Layer.provide(localPorts));

      const routedPorts = Layer.mergeAll(
        routedSubmissionLedgerLayer({ localThreadId: threadId }),
        routedThreadStoreLayer({ localThreadId: threadId }),
      ).pipe(Layer.provide(localPorts), Layer.provide(threadPortTransportLayer));

      const base = Layer.mergeAll(DurableAlarmService.layer, ProgressWaitRegistry.layer);

      const runtimeStack = DurableAgentRuntime.layerWithBindings(bindings).pipe(
        Layer.provideMerge(routedPorts),
        Layer.provideMerge(cloudflareWakeSchedulerLayer),
        Layer.provideMerge(base),
      );

      return Layer.mergeAll(
        runtimeStack,
        ThreadMaintenance.layer.pipe(Layer.provide(runtimeStack)),
        portsEndpointLayer,
      );
    }),
  );
