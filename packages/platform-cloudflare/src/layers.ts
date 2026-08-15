import { ConversationId } from "@effect-agent/core";
import {
  AgentBindingResolver,
  ConversationStore,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  ProducerId,
  SubmissionLedger,
  ToolReconciler,
  WakeScheduler,
  type DurableRuntimeFailpointHandler,
  type ResolvedBinding,
} from "@effect-agent/session";
import {
  conversationStoreLayer,
  handleEncodedPortRequest,
  routedConversationStoreLayer,
  routedSubmissionLedgerLayer,
  storageConfigLayer,
  storageFailpointLayer,
  submissionLedgerLayer,
  type DoStorageFailpointHandler,
  type DoStorageInitializationError,
  type DoStorageOptions,
} from "@effect-agent/storage-cloudflare";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Context, Duration, Effect, Layer, Schema } from "effect";

import { ConversationMaintenance, DurableAlarmService } from "./alarm.ts";
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
  /** Milliseconds; default 25. */
  readonly observationPollInterval?: number | undefined;
  /**
   * Milliseconds; default 2000. Cooperative budget for the background exporter flush registered
   * after each native RPC, wake, or alarm span. Delivery never awaits the flush.
   */
  readonly telemetryFlushTimeout?: number | undefined;
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
  /**
   * Reconciliation policy consulted for open ordinary Tool Calls before an Unknown Outcome
   * is recorded (durability §10, DUR-009). Defaults to the fail-closed
   * `ToolReconciler.uncertain`.
   */
  readonly toolReconciler?: Layer.Layer<ToolReconciler> | undefined;
  /**
   * Registered worker Bindings resolved at durable claim time (S2, spec/subagents.md §11):
   * build each with `DurableWorkerBinding.make(binding, digests)`. An Effect or callback form is
   * accepted because capture can be effectful. The callback receives the live Object context and
   * derived identities and is evaluated once per incarnation during Layer construction. Defaults
   * to the empty registration (every resolved claim fails closed).
   */
  readonly bindings?: CloudflareBindingSource | undefined;
}

/** Per-incarnation host values available while registered worker Bindings are captured. */
export interface CloudflareBindingSourceContext {
  readonly ctx: DurableObjectState;
  readonly env: unknown;
  readonly conversationId: ConversationId;
  readonly producerId: ProducerId;
}

/**
 * Registered worker Bindings, or a closed Effect/callback that captures them once for each
 * Durable Object incarnation. Callback Effects cannot require services or fail typed.
 */
export type CloudflareBindingSource =
  | ReadonlyArray<ResolvedBinding>
  | Effect.Effect<ReadonlyArray<ResolvedBinding>, never, never>
  | ((
      context: CloudflareBindingSourceContext,
    ) =>
      | ReadonlyArray<ResolvedBinding>
      | Effect.Effect<ReadonlyArray<ResolvedBinding>, never, never>);

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
  | ConversationObjectPorts;

/**
 * Owner-side endpoint body for the Conversation Object's `portCall` (plan §1.3): decode,
 * execute against THIS Object's LOCAL port facets — never the routed decorators, so a
 * request cannot bounce between Objects — and answer the encoded response envelope. Total by
 * construction (protocol anomalies answer `PortFailed(PortProtocolError)`).
 */
export class ConversationObjectPorts extends Context.Service<
  ConversationObjectPorts,
  {
    readonly handle: (encoded: unknown) => Effect.Effect<unknown>;
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
    telemetryFlushTimeout:
      options.telemetryFlushTimeout ?? CLOUDFLARE_RUNTIME_DEFAULTS.telemetryFlushTimeout,
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
  source === undefined
    ? Effect.succeed([])
    : Effect.isEffect(source)
      ? source
      : typeof source === "function"
        ? Effect.suspend(() => {
            const bindings = source(context);
            return Effect.isEffect(bindings) ? bindings : Effect.succeed(bindings);
          })
        : Effect.succeed(source);

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
 * Requires the two binding services (`DurableObjectContext`, `ConversationObjectNamespace`).
 * Host observability belongs to the Worker composition edge because native entrypoint lifecycle
 * instrumentation, rather than this durable runtime assembly, consumes it (DEPLOY-010).
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
              handle: (encoded) => handleEncodedPortRequest(encoded).pipe(Effect.provide(local)),
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
          }),
        );

        const runtimeFailpointLayer =
          options.runtimeFailpoint === undefined
            ? DurableRuntimeFailpoint.layer
            : Layer.succeed(DurableRuntimeFailpoint)({ hit: options.runtimeFailpoint(ctx) });
        const reconcilerLayer = options.toolReconciler ?? ToolReconciler.uncertain;
        const bindingResolverLayer = Layer.effect(AgentBindingResolver)(
          Effect.map(
            resolveBindings(options.bindings, { ctx, env, conversationId, producerId }),
            (bindings) => AgentBindingResolver.fromBindings(bindings),
          ),
        );

        const base = Layer.mergeAll(
          identityLayer,
          cloudflareConfigLayer,
          DurableAlarmService.layer,
        );

        const runtimeStack = DurableAgentRuntime.layer.pipe(
          Layer.provideMerge(routedPorts),
          Layer.provideMerge(cloudflareWakeSchedulerLayer),
          Layer.provideMerge(runtimeConfigLayer),
          Layer.provideMerge(bindingResolverLayer),
          Layer.provide(
            Layer.mergeAll(runtimeFailpointLayer, reconcilerLayer, BrowserCrypto.layer),
          ),
          Layer.provideMerge(base),
        );

        const application = Layer.mergeAll(
          runtimeStack,
          ConversationMaintenance.layer.pipe(Layer.provide(runtimeStack)),
          portsEndpointLayer,
        );

        return application;
      }),
    );
  }
}
