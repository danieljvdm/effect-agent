import { DEFAULT_OWNERSHIP_LEASE_DURATION, DeploymentId } from "@effect-agent/session";
import { DEFAULT_MAX_STORED_VALUE_BYTES } from "@effect-agent/storage-cloudflare";
import { Context, Duration, Schema } from "effect";

/**
 * Schema-validated configuration for the Cloudflare durable runtime (deployment spec §4:
 * decoded once during Layer construction, exposed as a typed service; DEPLOY-003). Every
 * cadence is in milliseconds; every bound is finite and checked before any resource opens.
 */

const PositiveMillis = Schema.Int.check(Schema.isGreaterThan(0));
const NonNegativeMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** The supplied Cloudflare durable runtime configuration failed validation (DEPLOY-003). */
export class CloudflarePlatformConfigError extends Schema.TaggedError<CloudflarePlatformConfigError>()(
  "CloudflarePlatformConfigError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * An admission was refused by a host resource limit BEFORE any ledger row existed
 * (deployment spec §8, DEPLOY-007: "Admission has explicit bounded quota and overload
 * behavior... a typed rejection"). This is the DC analogue of `NodeDurableHost`'s
 * `AdmissionClosed` host gate: the port surface stays untouched, the refusal happens in the
 * Conversation Object's submit entry point before `DurableAgentRuntime.submit` runs, and
 * nothing was admitted or written.
 */
export class AdmissionLimitExceeded extends Schema.TaggedError<AdmissionLimitExceeded>()(
  "AdmissionLimitExceeded",
  {
    limit: Schema.Literals(["queue-depth", "input-bytes", "database-bytes"]),
    actual: Schema.Int,
    maximum: Schema.Int,
  },
) {
  override get message() {
    return (
      `Admission refused before any ledger row existed: ${this.limit} ${this.actual} exceeds ` +
      `the configured maximum ${this.maximum}. Accepted work is unaffected; retry after the ` +
      "lane drains or raise the limit (DEPLOY-007)."
    );
  }
}

/** Cloudflare account tier whose SQLite-backed Durable Object cap is being deployed. */
export const CloudflareDatabasePlan = Schema.Literals(["free", "paid"]);
export type CloudflareDatabasePlan = typeof CloudflareDatabasePlan.Type;

/** Current per-Object SQLite database caps published by Cloudflare. */
export const CLOUDFLARE_FREE_DATABASE_CAP_BYTES = 1_000_000_000;
export const CLOUDFLARE_PAID_DATABASE_CAP_BYTES = 10_000_000_000;

/** Backward-compatible name for the largest platform cap accepted by the Schema. */
export const CLOUDFLARE_DATABASE_CAP_BYTES = CLOUDFLARE_PAID_DATABASE_CAP_BYTES;

/** Conservative Free-plan default, leaving 100 MB before the platform cap. */
export const DEFAULT_MAX_DATABASE_BYTES = 900_000_000;

/** Paid-plan default, leaving 1 GB before the platform cap. */
export const DEFAULT_PAID_MAX_DATABASE_BYTES = 9_000_000_000;

/**
 * Explicit bounded admission quotas checked by the Conversation Object BEFORE admission
 * (exit gate "resource limits are checked before admission").
 */
export class CloudflareAdmissionLimitsValue extends Schema.Class<CloudflareAdmissionLimitsValue>(
  "@effect-agent/platform-cloudflare/CloudflareAdmissionLimitsValue",
)({
  /** Maximum nonterminal Submissions per Conversation lane before new admissions refuse. */
  maxQueueDepthPerLane: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(100_000),
  ),
  /** Maximum encoded input bytes; never above the storage per-value bound. */
  maxInputBytes: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(2_000_000)),
  /** Maximum `ctx.storage.sql.databaseSize` at admission; validated against the selected plan. */
  maxDatabaseBytes: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

/**
 * Validated Cloudflare durable runtime configuration. The producer identity of one
 * Conversation Object is `{producerPrefix}:{conversationId}` — stable across incarnations of
 * the same deployment, distinct across deployments — and producer-epoch fencing (not the
 * producer name) remains the correctness authority (DUR-006).
 */
class CloudflareDurableRuntimeConfigModel extends Schema.Class<CloudflareDurableRuntimeConfigModel>(
  "@effect-agent/platform-cloudflare/CloudflareDurableRuntimeConfigValue",
)({
  deploymentId: DeploymentId,
  /** Deployment account tier whose real per-Object database cap applies. */
  databasePlan: CloudflareDatabasePlan,
  /** Head of the minted producer identity `{producerPrefix}:{conversationId}`. */
  producerPrefix: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  /** Submission ownership lease duration (D5); fences work across Object incarnations. */
  ownershipLeaseDuration: PositiveMillis,
  /** Base delay of the alarm re-arm backoff when a pass makes no progress. */
  alarmBackoffBase: PositiveMillis,
  /** Ceiling of the alarm re-arm backoff. */
  alarmBackoffCap: PositiveMillis,
  /**
   * The maintenance-pass scan cadence and the ceiling of every re-arm delay: nonterminal
   * work is revisited at least this often (wake/scan pairing, persistence §14).
   */
  wakeScanInterval: PositiveMillis,
  /** `awaitSettlement` ledger re-check cadence when no wake arrives. */
  settlementPollInterval: PositiveMillis,
  /** Worker ownership-lease renewal cadence during an active Attempt. */
  leaseRenewalInterval: PositiveMillis,
  /** Active-Run abort-intent poll cadence. */
  abortPollInterval: PositiveMillis,
  /** Canonical observation poll cadence of the Durable Object store. */
  observationPollInterval: NonNegativeMillis,
  /** Per-value byte bound; must stay under the platform's 2 MB SQLite value limit. */
  maxStoredValueBytes: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(2_000_000),
  ),
  /** Opt-in full payload/digest-chain audit while opening the store. */
  verifyOnOpen: Schema.Boolean,
  limits: CloudflareAdmissionLimitsValue,
}) {}

/**
 * Canonical whole-configuration Schema. The cross-field check binds the positive database
 * admission limit to the selected deployment plan's actual per-Object ceiling.
 */
export const CloudflareDurableRuntimeConfigValue = CloudflareDurableRuntimeConfigModel.check(
  Schema.makeFilter((config) => {
    const cap =
      config.databasePlan === "paid"
        ? CLOUDFLARE_PAID_DATABASE_CAP_BYTES
        : CLOUDFLARE_FREE_DATABASE_CAP_BYTES;
    return config.limits.maxDatabaseBytes <= cap
      ? undefined
      : {
          path: ["limits", "maxDatabaseBytes"],
          issue:
            `maxDatabaseBytes ${config.limits.maxDatabaseBytes} exceeds the ` +
            `${config.databasePlan} plan cap ${cap}`,
        };
  }),
);
export type CloudflareDurableRuntimeConfigValue = typeof CloudflareDurableRuntimeConfigValue.Type;

/** Explicit configuration authority for the assembled Cloudflare durable runtime. */
export class CloudflareDurableRuntimeConfig extends Context.Service<
  CloudflareDurableRuntimeConfig,
  CloudflareDurableRuntimeConfigValue
>()("@effect-agent/platform-cloudflare/CloudflareDurableRuntimeConfig") {}

/** Documented production defaults applied by `CloudflareDurableRuntime.layer`. */
export const CLOUDFLARE_RUNTIME_DEFAULTS = {
  databasePlan: "free" as const,
  ownershipLeaseDuration: Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
  alarmBackoffBase: 100,
  alarmBackoffCap: 5_000,
  wakeScanInterval: 1_000,
  settlementPollInterval: 500,
  leaseRenewalInterval: 10_000,
  abortPollInterval: 500,
  observationPollInterval: 25,
  maxStoredValueBytes: DEFAULT_MAX_STORED_VALUE_BYTES,
  verifyOnOpen: false,
  maxQueueDepthPerLane: 256,
  maxInputBytes: DEFAULT_MAX_STORED_VALUE_BYTES,
  maxDatabaseBytes: DEFAULT_MAX_DATABASE_BYTES,
} as const;
