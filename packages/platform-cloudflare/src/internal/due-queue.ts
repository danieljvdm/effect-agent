import { Schema } from "effect";

export const LaneId = Schema.NonEmptyString.check(Schema.isMaxLength(256));

export const HostLaneId = LaneId.check(
  Schema.isPattern(/^(?!effect-agent:)/, {
    message: "The effect-agent: namespace is reserved for framework maintenance lanes",
  }),
);

export const Deadline = Schema.NullOr(Schema.Finite);
export const MinimumRetryMillis = 1_000;
export const MaximumNoProgressRearms = 8;

/** Scheduling metadata only. Domain outboxes, claims and receipts remain authoritative. */
export class DueLane extends Schema.Class<DueLane>("CloudflareDueLane")({
  id: LaneId,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  dueAt: Deadline,
  stalls: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 30 })),
  progressKey: Schema.NullOr(Schema.NonEmptyString.check(Schema.isMaxLength(512))),
  notBefore: Schema.Finite,
  state: Schema.Literals(["idle", "pending", "parked"]),
}) {}

export const Native = "effect-agent:native";
export const Publication = "effect-agent:publication";
export const Projection = "effect-agent:projection";
export const Messages = "effect-agent:messages";
export const RecoveryEvents = "effect-agent:recovery-events";
export const Lifecycle = "effect-agent:lifecycle";
export const LifecycleStart = "effect-agent:lifecycle-start";

const decode = Schema.decodeUnknownSync(Schema.Array(DueLane));

const views = new WeakMap<DurableObjectStorage, { rows: ReadonlyArray<DueLane> | undefined }>();

export const invalidate = (storage: DurableObjectStorage): void => {
  const view = views.get(storage);

  if (view !== undefined) view.rows = undefined;
};

/** Callers reserve the shared SQL connection through commit and invalidate on rollback.
 * Retain at most 128 lanes; SQLite and the alarm still reconstruct every cold owner. */
export const make = (storage: DurableObjectStorage) => {
  // Instrumentation may return a fresh SQL wrapper on every access.
  const sql = storage.sql;
  let view = views.get(storage);

  if (view === undefined) {
    view = { rows: undefined };
    views.set(storage, view);
  }
  const current = view;

  const retain = (changed: ReadonlyArray<DueLane>) => {
    if (current.rows === undefined) return;
    const ids = new Set(changed.map((row) => row.id));
    const next = [...current.rows.filter((row) => !ids.has(row.id)), ...changed];

    current.rows = next.length <= 128 ? next : undefined;
  };

  const initialize = () => {
    sql.exec(`CREATE TABLE IF NOT EXISTS platform_cloudflare_due_queue (
    id TEXT PRIMARY KEY, revision INTEGER NOT NULL, dueAt REAL, stalls INTEGER NOT NULL,
    progressKey TEXT, notBefore REAL NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending'
  )`);

    const columns = new Set(
      sql
        .exec("PRAGMA table_info(platform_cloudflare_due_queue)")
        .toArray()
        .map((row) => row.name),
    );

    if (!columns.has("progressKey"))
      sql.exec("ALTER TABLE platform_cloudflare_due_queue ADD COLUMN progressKey TEXT");
    if (!columns.has("notBefore"))
      sql.exec(
        "ALTER TABLE platform_cloudflare_due_queue ADD COLUMN notBefore REAL NOT NULL DEFAULT 0",
      );
    if (!columns.has("state")) {
      sql.exec(
        "ALTER TABLE platform_cloudflare_due_queue ADD COLUMN state TEXT NOT NULL DEFAULT 'pending'",
      );
      sql.exec("UPDATE platform_cloudflare_due_queue SET state = 'idle' WHERE dueAt IS NULL");
    }
  };

  /** Keep dormant rows so delete/reinsert cannot reuse a revision held by an older wave. */
  const read = () => {
    if (current.rows !== undefined) return current.rows;

    const rows = decode(
      sql
        .exec(
          "SELECT id, revision, dueAt, stalls, progressKey, notBefore, state FROM platform_cloudflare_due_queue",
        )
        .toArray(),
    );

    if (rows.length <= 128) current.rows = rows;

    return rows;
  };

  const dirty = (id: string, dueAt: number, progress = false, progressKey?: string) => {
    Schema.decodeSync(LaneId)(id);
    Schema.decodeSync(Schema.Finite)(dueAt);
    retain(
      decode(
        sql
          .exec(
            `INSERT INTO platform_cloudflare_due_queue (id, revision, dueAt, stalls, progressKey, notBefore) VALUES (?, 1, ?, 0, ?, 0)
     ON CONFLICT(id) DO UPDATE SET revision = revision + ?,
       dueAt = CASE WHEN ? = 0 AND stalls >= ${MaximumNoProgressRearms} THEN NULL
         WHEN ? = 1 THEN excluded.dueAt
         WHEN dueAt IS NULL THEN max(excluded.dueAt, notBefore)
         ELSE max(notBefore, min(dueAt, excluded.dueAt)) END,
       state = CASE WHEN ? = 1 THEN 'pending' WHEN stalls >= ${MaximumNoProgressRearms} THEN state ELSE 'pending' END,
       stalls = CASE WHEN ? = 1 THEN 0 ELSE stalls END,
       notBefore = CASE WHEN ? = 1 THEN 0 ELSE notBefore END,
       progressKey = coalesce(excluded.progressKey, progressKey) RETURNING id, revision, dueAt, stalls, progressKey, notBefore, state`,
            id,
            dueAt,
            progressKey ?? null,
            Number(progress),
            Number(progress),
            Number(progress),
            Number(progress),
            Number(progress),
            Number(progress),
          )
          .toArray(),
      ),
    );
  };

  /** A strictly increasing source cursor; old or repeated notices cannot renew a budget. */
  const progress = (id: string, dueAt: number, cursor: bigint) => {
    Schema.decodeSync(Schema.BigInt.check(Schema.isGreaterThanOrEqualToBigInt(0n)))(cursor);
    const previous = read().find((row) => row.id === id)?.progressKey;

    if (previous !== undefined && previous !== null && cursor <= BigInt(previous)) return false;
    dirty(id, dueAt, true, String(cursor));

    return true;
  };

  /** One initial native discovery wave; host lanes always require explicit enrollment. */
  const register = (id: string) => {
    retain(
      decode(
        sql
          .exec(
            "INSERT OR IGNORE INTO platform_cloudflare_due_queue (id, revision, dueAt, stalls, progressKey, notBefore) VALUES (?, 0, 0, 0, NULL, 0) RETURNING id, revision, dueAt, stalls, progressKey, notBefore, state",
            id,
          )
          .toArray(),
      ),
    );
  };

  /** Charge before fallible work so a crash cannot replay an uncharged attempt. */
  const claim = (lane: DueLane, nowMillis: number, native = false) => {
    const notBefore = nowMillis + Math.min(60_000, MinimumRetryMillis * 2 ** lane.stalls);

    const rows = decode(
      sql
        .exec(
          `UPDATE platform_cloudflare_due_queue SET stalls = stalls + 1, notBefore = ?,
       state = CASE WHEN stalls + 1 >= ${MaximumNoProgressRearms} THEN 'parked' ELSE 'pending' END,
       dueAt = CASE WHEN stalls + 1 >= ${MaximumNoProgressRearms} THEN NULL ELSE ? END
       WHERE id = ? AND revision = ? AND stalls < ${MaximumNoProgressRearms}
       AND dueAt IS NOT NULL AND (dueAt <= ? OR (? = 1 AND notBefore <= ?)) RETURNING id, revision, dueAt, stalls, progressKey, notBefore, state`,
          notBefore,
          notBefore,
          lane.id,
          lane.revision,
          nowMillis,
          Number(native),
          nowMillis,
        )
        .toArray(),
    );

    retain(rows);

    return rows[0];
  };

  const complete = (lane: DueLane, dueAt: number | null) => {
    Schema.decodeSync(Deadline)(dueAt);

    const guarded =
      dueAt === null || lane.stalls >= MaximumNoProgressRearms
        ? null
        : Math.max(dueAt, lane.notBefore);

    retain(
      decode(
        sql
          .exec(
            "UPDATE platform_cloudflare_due_queue SET dueAt = ?, state = ?, revision = revision + 1 WHERE id = ? AND revision = ? RETURNING id, revision, dueAt, stalls, progressKey, notBefore, state",
            guarded,
            dueAt === null ? "idle" : guarded === null ? "parked" : "pending",
            lane.id,
            lane.revision,
          )
          .toArray(),
      ),
    );
  };

  /** Native checkpoints already fence their own dirty/processed generation. */
  const checkpointNative = (dueAt: number | null) => {
    Schema.decodeSync(Deadline)(dueAt);
    const row = read().find((row) => row.id === Native);
    const publication = read().find((row) => row.id === Publication);

    const guarded =
      (row?.stalls ?? 0) >= MaximumNoProgressRearms || publication?.state === "parked"
        ? null
        : dueAt === null
          ? null
          : Math.max(dueAt, row?.notBefore ?? 0);

    retain(
      decode(
        sql
          .exec(
            "UPDATE platform_cloudflare_due_queue SET dueAt = ?, state = ? WHERE id = ? RETURNING id, revision, dueAt, stalls, progressKey, notBefore, state",
            guarded,
            dueAt === null ? "idle" : guarded === null ? "parked" : "pending",
            Native,
          )
          .toArray(),
      ),
    );
  };

  return { initialize, read, dirty, progress, register, claim, complete, checkpointNative };
};

export const next = (rows: ReadonlyArray<DueLane>) =>
  rows.reduce((next, row) => Math.min(next, row.dueAt ?? Infinity), Infinity);
