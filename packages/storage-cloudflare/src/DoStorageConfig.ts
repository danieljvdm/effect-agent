import { Context, Schema } from "effect";

const ObservationPollInterval = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const OwnershipLeaseMillis = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * Default per-value byte bound, kept under the Durable Object platform's 2 MB SQLite value
 * limit with a safety margin. This is the DC analogue of Node's 16 MB `BoundedStoredText`
 * bound: both fail typed before mutating, only the threshold differs (a documented DN/DC
 * behavioral difference; Travel Planner payloads sit orders of magnitude below both).
 */
export const DEFAULT_MAX_STORED_VALUE_BYTES = 1_900_000;

/** The hard schema ceiling for the configurable bound: never at or above the platform limit. */
const MaxStoredValueBytes = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(2_000_000),
);

/**
 * Validated construction configuration consumed by the Durable Object storage Layers. The
 * storage identity itself belongs to the SqlClient Layer (built from `ctx.storage`);
 * duplicating it here could silently diverge from the handle actually in use.
 */
export class DoStorageConfigValue extends Schema.Class<DoStorageConfigValue>(
  "@effect-agent/storage-cloudflare/DoStorageConfigValue",
)({
  observationPollInterval: ObservationPollInterval,
  /**
   * Submission ownership lease duration in milliseconds (D5). Inside one Durable Object the
   * object itself is the serialized owner, so the lease's primary DC role is fencing work
   * across DO incarnations (an evicted incarnation's claim becomes reclaimable); correctness
   * never depends on it because every canonical append is fenced by producer epoch.
   */
  ownershipLeaseDuration: OwnershipLeaseMillis,
  /**
   * Maximum bytes for any single stored text value (canonical batch/record JSON, admission
   * input payload, checkpoint JSON). Enforced typed BEFORE any write; must stay under the
   * platform's 2 MB per-value limit.
   */
  maxStoredValueBytes: MaxStoredValueBytes,
  /**
   * Re-verify every stored payload and digest chain while opening the store. Per-operation
   * Schema decoding and the digest chain already fail clearly on corrupt rows, so the full
   * scan is an explicit opt-in integrity audit rather than a startup requirement.
   */
  verifyOnOpen: Schema.Boolean,
}) {}

/** Explicit Durable Object storage configuration authority. */
export class DoStorageConfig extends Context.Service<DoStorageConfig, DoStorageConfigValue>()(
  "@effect-agent/storage-cloudflare/DoStorageConfig",
) {}
