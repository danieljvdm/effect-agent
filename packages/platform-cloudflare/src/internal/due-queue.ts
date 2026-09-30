import { Schema } from "effect";

export const LaneId = Schema.NonEmptyString.check(Schema.isMaxLength(256));

export const HostLaneId = LaneId.check(
  Schema.isPattern(/^(?!effect-agent:)/, {
    message: "The effect-agent: namespace is reserved for framework maintenance lanes",
  }),
);

export const Deadline = Schema.NullOr(Schema.Finite);

/** Scheduling metadata only. Domain outboxes, claims and receipts remain authoritative. */
export class DueLane extends Schema.Class<DueLane>("CloudflareDueLane")({
  id: LaneId,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  dueAt: Deadline,
  stalls: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 30 })),
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
    id TEXT PRIMARY KEY, revision INTEGER NOT NULL, dueAt REAL, stalls INTEGER NOT NULL
  )`);
  };

  /** Keep dormant rows so delete/reinsert cannot reuse a revision held by an older wave. */
  const read = () => {
    if (current.rows !== undefined) return current.rows;

    const rows = decode(
      sql.exec("SELECT id, revision, dueAt, stalls FROM platform_cloudflare_due_queue").toArray(),
    );

    if (rows.length <= 128) current.rows = rows;

    return rows;
  };

  const dirty = (id: string, dueAt: number) => {
    Schema.decodeSync(LaneId)(id);
    Schema.decodeSync(Schema.Finite)(dueAt);
    retain(
      decode(
        sql
          .exec(
            `INSERT INTO platform_cloudflare_due_queue VALUES (?, 1, ?, 0)
     ON CONFLICT(id) DO UPDATE SET revision = revision + 1,
       dueAt = CASE WHEN dueAt IS NULL THEN excluded.dueAt ELSE min(dueAt, excluded.dueAt) END,
       stalls = 0 RETURNING id, revision, dueAt, stalls`,
            id,
            dueAt,
          )
          .toArray(),
      ),
    );
  };

  /** One initial native discovery wave; host lanes always require explicit enrollment. */
  const register = (id: string) => {
    retain(
      decode(
        sql
          .exec(
            "INSERT OR IGNORE INTO platform_cloudflare_due_queue VALUES (?, 0, 0, 0) RETURNING id, revision, dueAt, stalls",
            id,
          )
          .toArray(),
      ),
    );
  };

  const complete = (lane: DueLane, dueAt: number | null, failed = false) => {
    Schema.decodeSync(Deadline)(dueAt);
    retain(
      decode(
        sql
          .exec(
            "UPDATE platform_cloudflare_due_queue SET dueAt = ?, stalls = ?, revision = revision + 1 WHERE id = ? AND revision = ? RETURNING id, revision, dueAt, stalls",
            dueAt,
            failed ? Math.min(30, lane.stalls + 1) : 0,
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
    retain(
      decode(
        sql
          .exec(
            "UPDATE platform_cloudflare_due_queue SET dueAt = ? WHERE id = ? RETURNING id, revision, dueAt, stalls",
            dueAt,
            Native,
          )
          .toArray(),
      ),
    );
  };

  return { initialize, read, dirty, register, complete, checkpointNative };
};

export const next = (rows: ReadonlyArray<DueLane>) =>
  rows.reduce((next, row) => Math.min(next, row.dueAt ?? Infinity), Infinity);
