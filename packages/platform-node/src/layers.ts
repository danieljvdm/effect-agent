import type { SubmissionId } from "@effect-agent/core";
import {
  AgentBindingResolver,
  type ChildAdmissionAuthorizer,
  type ConversationStore,
  DEFAULT_OWNERSHIP_LEASE_DURATION,
  DeploymentId,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  ProducerId,
  type OperationAuthorizer,
  ReleaseOwnershipRequest,
  SubmissionLedger,
  ToolReconciler,
  type WakeScheduler,
  type DurableRuntimeFailpointHandler,
  type OwnershipToken,
  type ResolvedBinding,
} from "@effect-agent/session";
import {
  conversationStoreLayer,
  SqliteStorageConfig,
  SqliteStorageConfigValue,
  storageFailpointLayer,
  submissionLedgerLayer,
  type SqliteStorageFailpointHandler,
  type SqliteStorageInitializationError,
} from "@effect-agent/storage-sqlite";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Context, Duration, Effect, Layer, Ref, Schema } from "effect";

import { NodeWakeSchedulerConfig, nodeWakeSchedulerLayer } from "./wake-scheduler.ts";

const PositiveMillis = Schema.Int.check(Schema.isGreaterThan(0));
const NonNegativeMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const WorkerConcurrency = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(1),
  Schema.isLessThanOrEqualTo(64),
);

/** The supplied Node durable runtime configuration failed schema validation (DEPLOY-003). */
export class NodePlatformConfigError extends Schema.TaggedError<NodePlatformConfigError>()(
  "NodePlatformConfigError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * Validated Node durable runtime configuration (deployment §4: decoded once during Layer
 * construction, exposed as a typed service). Every cadence is in milliseconds and every bound is
 * finite; `workerConcurrency` caps how many worker loops `NodeDurableHost.runWorkers` drives.
 */
export class NodeDurableRuntimeConfigValue extends Schema.Class<NodeDurableRuntimeConfigValue>(
  "@effect-agent/platform-node/NodeDurableRuntimeConfigValue",
)({
  /** SQLite database file backing BOTH the Conversation Log and the Submission Ledger. */
  filename: Schema.NonEmptyString,
  deploymentId: DeploymentId,
  producerId: ProducerId,
  /** Submission ownership lease duration (D5); liveness hint only, epochs stay authoritative. */
  ownershipLeaseDuration: PositiveMillis,
  /** Finite bound on concurrent worker loops per host (rule 10). */
  workerConcurrency: WorkerConcurrency,
  /** Ledger-scan fallback cadence of the Node wake scheduler (deployment §3). */
  wakeScanInterval: PositiveMillis,
  /** `awaitSettlement` ledger re-check cadence when no wake arrives. */
  settlementPollInterval: PositiveMillis,
  /** Worker ownership-lease renewal cadence. */
  leaseRenewalInterval: PositiveMillis,
  /** Active-Run abort-intent poll cadence. */
  abortPollInterval: PositiveMillis,
  /** Bounded SQLITE_BUSY retry window for write-lock acquisition. */
  busyTimeout: NonNegativeMillis,
  /** Canonical observation poll cadence of the SQLite store. */
  observationPollInterval: NonNegativeMillis,
  /** Opt-in full payload/digest-chain audit while opening the store. */
  verifyOnOpen: Schema.Boolean,
}) {}

/** Explicit configuration authority for the assembled Node durable runtime. */
export class NodeDurableRuntimeConfig extends Context.Service<
  NodeDurableRuntimeConfig,
  NodeDurableRuntimeConfigValue
>()("@effect-agent/platform-node/NodeDurableRuntimeConfig") {}

/**
 * Raw (unvalidated) construction options for `NodeDurableRuntime.layer`. Optional fields default
 * to the documented production values; everything is schema-decoded into
 * `NodeDurableRuntimeConfigValue` before any resource opens (deployment §5 gate 1). Security
 * policies are deliberately absent: the Layer requires the authorizer services from its caller.
 */
export interface NodeDurableRuntimeOptions {
  readonly filename: string;
  readonly deploymentId: string;
  readonly producerId: string;
  /** Milliseconds; default `DEFAULT_OWNERSHIP_LEASE_DURATION` (30s, D5). */
  readonly ownershipLeaseDuration?: number | undefined;
  /** Default 1; bounded to 1..64. */
  readonly workerConcurrency?: number | undefined;
  /** Milliseconds; default 1000. */
  readonly wakeScanInterval?: number | undefined;
  /** Milliseconds; default 500. */
  readonly settlementPollInterval?: number | undefined;
  /** Milliseconds; default 10000. */
  readonly leaseRenewalInterval?: number | undefined;
  /** Milliseconds; default 500. */
  readonly abortPollInterval?: number | undefined;
  /** Milliseconds; default 5000. */
  readonly busyTimeout?: number | undefined;
  /** Milliseconds; default 25. */
  readonly observationPollInterval?: number | undefined;
  /** Default false. */
  readonly verifyOnOpen?: boolean | undefined;
  /** SQLite adapter fault injection (`ledger:*` / `append:*` locations); default none. */
  readonly storageFailpoint?: SqliteStorageFailpointHandler | undefined;
  /** Coordinator fault injection (`submit:*` / `terminalize:*` locations); default none. */
  readonly runtimeFailpoint?: DurableRuntimeFailpointHandler | undefined;
  /**
   * Reconciliation policy consulted for open ordinary Tool Calls before an Unknown Outcome is
   * recorded (durability §10, DUR-009). Defaults to the fail-closed `ToolReconciler.uncertain`:
   * with no registered policy, every open call stays Unknown and routes to the authorized
   * DUR-017 resolution path.
   */
  readonly toolReconciler?: Layer.Layer<ToolReconciler> | undefined;
  /**
   * Registered worker Bindings resolved at durable claim time (S2, spec/subagents.md §11):
   * build each with `DurableWorkerBinding.make(binding, digests)` so
   * `NodeDurableHost.runResolvedWorkers` / `DurableAgentRuntime.runResolvedWorker` can serve
   * parent and child lanes from one pool with exact-digest resolution (SUB-023). Defaults to
   * the empty registration: every resolved claim then fails closed (`BindingUnavailable` for a
   * root, the framework `ChildCompatibilityFailure` Settlement for a parent-linked child).
   */
  readonly bindings?: ReadonlyArray<ResolvedBinding> | undefined;
}

/** Every construction failure of the assembled Node durable runtime stack. */
export type NodeDurableRuntimeInitializationError =
  | NodePlatformConfigError
  | SqliteStorageInitializationError;

/** The services `NodeDurableRuntime.layer` provides. */
export type NodeDurableRuntimeServices =
  | DurableAgentRuntime
  | SubmissionLedger
  | ConversationStore
  | WakeScheduler
  | DurableRuntimeConfig
  | NodeDurableRuntimeConfig
  | AgentBindingResolver;

const decodeConfigValue = Schema.decodeUnknownEffect(NodeDurableRuntimeConfigValue);

const configFromOptions = (
  options: NodeDurableRuntimeOptions,
): Effect.Effect<NodeDurableRuntimeConfigValue, NodePlatformConfigError> =>
  decodeConfigValue({
    filename: options.filename,
    deploymentId: options.deploymentId,
    producerId: options.producerId,
    ownershipLeaseDuration:
      options.ownershipLeaseDuration ?? Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
    workerConcurrency: options.workerConcurrency ?? 1,
    wakeScanInterval: options.wakeScanInterval ?? 1_000,
    settlementPollInterval: options.settlementPollInterval ?? 500,
    leaseRenewalInterval: options.leaseRenewalInterval ?? 10_000,
    abortPollInterval: options.abortPollInterval ?? 500,
    busyTimeout: options.busyTimeout ?? 5_000,
    observationPollInterval: options.observationPollInterval ?? 25,
    verifyOnOpen: options.verifyOnOpen ?? false,
  }).pipe(
    Effect.mapError((error) =>
      NodePlatformConfigError.make({
        message: `Invalid Node durable runtime configuration: ${error.message}`,
        cause: error,
      }),
    ),
  );

/** SQLite storage configuration derived from the single validated Node configuration. */
const sqliteStorageConfigLayer: Layer.Layer<SqliteStorageConfig, never, NodeDurableRuntimeConfig> =
  Layer.effect(SqliteStorageConfig)(
    Effect.gen(function* () {
      const config = yield* NodeDurableRuntimeConfig;
      return SqliteStorageConfigValue.make({
        observationPollInterval: config.observationPollInterval,
        busyTimeout: config.busyTimeout,
        ownershipLeaseDuration: config.ownershipLeaseDuration,
        verifyOnOpen: config.verifyOnOpen,
      });
    }),
  );

/** Session coordinator configuration derived from the single validated Node configuration. */
const durableRuntimeConfigLayer: Layer.Layer<
  DurableRuntimeConfig,
  never,
  NodeDurableRuntimeConfig
> = Layer.effect(DurableRuntimeConfig)(
  Effect.gen(function* () {
    const config = yield* NodeDurableRuntimeConfig;
    return DurableRuntimeConfig.make({
      deploymentId: config.deploymentId,
      producerId: config.producerId,
      settlementPollInterval: Duration.millis(config.settlementPollInterval),
      leaseRenewalInterval: Duration.millis(config.leaseRenewalInterval),
      abortPollInterval: Duration.millis(config.abortPollInterval),
    });
  }),
);

/** Wake fallback-scan cadence derived from the single validated Node configuration. */
const wakeSchedulerConfigLayer: Layer.Layer<
  NodeWakeSchedulerConfig,
  never,
  NodeDurableRuntimeConfig
> = Layer.effect(NodeWakeSchedulerConfig)(
  Effect.gen(function* () {
    const config = yield* NodeDurableRuntimeConfig;
    return { scanInterval: Duration.millis(config.wakeScanInterval) };
  }),
);

const releaseTrackedOwnership = (
  ledger: SubmissionLedger["Service"],
  registry: Ref.Ref<ReadonlyMap<SubmissionId, OwnershipToken>>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const tracked = yield* Ref.getAndSet(registry, new Map<SubmissionId, OwnershipToken>());
    for (const [submissionId, ownershipToken] of tracked) {
      yield* ledger
        .releaseOwnership(ReleaseOwnershipRequest.make({ submissionId, ownershipToken }))
        .pipe(
          Effect.catchTags({
            // A newer epoch already owns (or settled) the lane: nothing left to drain.
            OwnershipLost: () => Effect.void,
            // Drain is best-effort by design: the lease still expires and the durability protocol,
            // not graceful shutdown, provides correctness (DEPLOY-006).
            LedgerError: (error) =>
              Effect.logWarning("Ownership drain failed; the lease will expire instead", error),
          }),
        );
    }
  });

/**
 * Shutdown-drain decorator for a `SubmissionLedger` (deployment §6 step 6): every ownership
 * period granted through this Layer is tracked — claims start tracking, renewals follow token
 * rotation, releases and settlement finalizations stop it — and every ownership still held when
 * the Layer's Scope closes is released so another host can claim the lane immediately instead of
 * waiting for lease expiry. The drain is a liveness courtesy only; producer-epoch fencing remains
 * the correctness authority (DUR-006), and a forced kill simply falls back to lease expiry.
 */
export const ownershipDrainLayer: Layer.Layer<SubmissionLedger, never, SubmissionLedger> =
  Layer.effect(SubmissionLedger)(
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const registry = yield* Ref.make<ReadonlyMap<SubmissionId, OwnershipToken>>(
        new Map<SubmissionId, OwnershipToken>(),
      );

      const track = (submissionId: SubmissionId, ownershipToken: OwnershipToken) =>
        Ref.update(registry, (tracked) => new Map(tracked).set(submissionId, ownershipToken));
      const untrack = (submissionId: SubmissionId) =>
        Ref.update(registry, (tracked) => {
          const next = new Map(tracked);
          next.delete(submissionId);
          return next;
        });

      yield* Effect.addFinalizer(() => releaseTrackedOwnership(ledger, registry));

      return SubmissionLedger.of({
        capabilities: ledger.capabilities,
        admit: ledger.admit,
        markReady: ledger.markReady,
        lookup: ledger.lookup,
        // The S2 subagent ops forward untouched: none of them grants an ownership period, so
        // the drain has nothing to track for them (`suspend` below already stops tracking the
        // waitingForChild ownership period the moment it ends).
        resolveAdmission: ledger.resolveAdmission,
        recordChildSettled: ledger.recordChildSettled,
        reserveChildBudget: ledger.reserveChildBudget,
        attachChildToReservation: ledger.attachChildToReservation,
        beginChildBudgetRelease: ledger.beginChildBudgetRelease,
        releaseChildBudget: ledger.releaseChildBudget,
        claim: (request) =>
          ledger
            .claim(request)
            .pipe(
              Effect.tap((claimed) =>
                claimed._tag === "Some"
                  ? track(claimed.value.submissionId, claimed.value.ownershipToken)
                  : Effect.void,
              ),
            ),
        renewOwnership: (request) =>
          ledger.renewOwnership(request).pipe(
            Effect.tap((renewal) => track(request.submissionId, renewal.ownershipToken)),
            Effect.tapError((error) =>
              error._tag === "OwnershipLost" ? untrack(request.submissionId) : Effect.void,
            ),
          ),
        releaseOwnership: (request) =>
          ledger.releaseOwnership(request).pipe(
            Effect.tap(() => untrack(request.submissionId)),
            Effect.tapError((error) =>
              error._tag === "OwnershipLost" ? untrack(request.submissionId) : Effect.void,
            ),
          ),
        markInputApplied: ledger.markInputApplied,
        reserveSettlement: ledger.reserveSettlement,
        finalizeSettlement: (request) =>
          ledger.finalizeSettlement(request).pipe(Effect.tap(() => untrack(request.submissionId))),
        requestAbort: ledger.requestAbort,
        claimJoining: ledger.claimJoining,
        markJoined: ledger.markJoined,
        revertJoining: ledger.revertJoining,
        // Suspension ends the ownership period by contract, so the drain stops tracking it.
        suspend: (request) =>
          ledger.suspend(request).pipe(Effect.tap(() => untrack(request.submissionId))),
        resumeSuspension: ledger.resumeSuspension,
        recordApprovalDecision: ledger.recordApprovalDecision,
        markUnknown: ledger.markUnknown,
        recordUnknownResolution: ledger.recordUnknownResolution,
        repairSettlementFromCanonical: ledger.repairSettlementFromCanonical,
        scanNonterminal: ledger.scanNonterminal,
        loadRecoverySnapshot: ledger.loadRecoverySnapshot,
      });
    }),
  );

/**
 * The DN Layer assembly (deployment §12: a Layer-assembly library, not an app entrypoint).
 * `layer(options)` decodes the configuration, opens ONE SQLite database serving both the
 * Conversation Log and the Submission Ledger (so claims fence the same producer epochs), wires
 * the Node wake scheduler with its ledger-scan fallback, wraps the ledger with the shutdown
 * ownership drain, defaults the Tool reconciliation policy to the fail-closed
 * `ToolReconciler.uncertain` (override via `options.toolReconciler`), and provides a ready
 * `DurableAgentRuntime` on top. Storage compatibility is
 * verified during construction: an incompatible database file fails the Layer with
 * `SqliteStorageCompatibilityError` before anything is mutated (DEPLOY-008).
 */
export class NodeDurableRuntime {
  /** Validated configuration Layer; fails typed when the supplied options are out of bounds. */
  static configLayer(
    options: NodeDurableRuntimeOptions,
  ): Layer.Layer<NodeDurableRuntimeConfig, NodePlatformConfigError> {
    return Layer.effect(NodeDurableRuntimeConfig)(configFromOptions(options));
  }

  /** The full DN runtime stack over one SQLite file, requiring both authorization policy ports. */
  static layer(
    options: NodeDurableRuntimeOptions,
  ): Layer.Layer<
    NodeDurableRuntimeServices,
    NodeDurableRuntimeInitializationError,
    OperationAuthorizer | ChildAdmissionAuthorizer
  > {
    const infrastructure = Layer.mergeAll(
      sqliteStorageConfigLayer,
      storageFailpointLayer({ filename: options.filename, failpoint: options.storageFailpoint }),
      SqliteClient.layer({ filename: options.filename }),
      NodeCrypto.layer,
    );
    const runtimeFailpointLayer =
      options.runtimeFailpoint === undefined
        ? DurableRuntimeFailpoint.layer
        : Layer.succeed(DurableRuntimeFailpoint)({ hit: options.runtimeFailpoint });
    const reconcilerLayer = options.toolReconciler ?? ToolReconciler.uncertain;
    const bindingResolverLayer = AgentBindingResolver.layer(options.bindings ?? []);
    const ports = Layer.mergeAll(
      conversationStoreLayer,
      nodeWakeSchedulerLayer.pipe(
        Layer.provideMerge(ownershipDrainLayer.pipe(Layer.provide(submissionLedgerLayer))),
      ),
    );
    return DurableAgentRuntime.layer.pipe(
      Layer.provideMerge(Layer.mergeAll(ports, durableRuntimeConfigLayer, bindingResolverLayer)),
      Layer.provide(
        Layer.mergeAll(wakeSchedulerConfigLayer, runtimeFailpointLayer, reconcilerLayer),
      ),
      Layer.provideMerge(infrastructure),
      Layer.provideMerge(NodeDurableRuntime.configLayer(options)),
    );
  }
}
