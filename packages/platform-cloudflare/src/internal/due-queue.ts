import { Effect, Exit, Scheduler, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { SqlError, UnknownError } from "effect/unstable/sql/SqlError";

export const LaneId = Schema.NonEmptyString.check(Schema.isMaxLength(256));

export const HostLaneId = LaneId.check(
  Schema.isPattern(/^(?!effect-agent:)/, {
    message: "The effect-agent: namespace is reserved for framework maintenance lanes",
  }),
);

export const Deadline = Schema.NullOr(Schema.Finite);
export const MinimumRetryMillis = 1_000;
export const MaximumNoProgressRearms = 8;
export const ParkedRetryMillis = 60 * 60_000;

/** Scheduling metadata only. Domain outboxes, claims and receipts remain authoritative. */
export class DueLane extends Schema.Class<DueLane>("CloudflareDueLane")({
  id: LaneId,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  dueAt: Deadline,
  stalls: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 30 })),
  progressKey: Schema.NullOr(Schema.NonEmptyString.check(Schema.isMaxLength(512))),
  notBefore: Schema.Finite,
  state: Schema.Literals(["idle", "pending", "parked"]),
  reported: Schema.Literals([0, 1]),
}) {}

export const Native = "effect-agent:native";
export const Publication = "effect-agent:publication";
export const Projection = "effect-agent:projection";
export const Messages = "effect-agent:messages";
export const RecoveryEvents = "effect-agent:recovery-events";
export const Lifecycle = "effect-agent:lifecycle";

const decode = Schema.decodeUnknownSync(Schema.Array(DueLane));

const equivalent = Schema.toEquivalence(DueLane);

type Change = { readonly before: DueLane | undefined; readonly after: DueLane };
type Frame = {
  readonly parent: Frame | undefined;
  readonly sqlDepth: number | undefined;
  readonly rows: ReadonlyArray<DueLane> | undefined;
  readonly changes: Map<string, Change>;
  readonly deadlines: Map<string, number>;
};
type View = {
  rows: ReadonlyArray<DueLane> | undefined;
  frame: Frame | undefined;
  retained: number;
  changes: Map<string, Change>;
  deadlines: Map<string, number>;
};
const views = new WeakMap<DurableObjectStorage, View>();

/** Only the explicit source boundary may retain speculative rows across operations. */
export const buffering = (storage: DurableObjectStorage): boolean =>
  views.get(storage)?.frame !== undefined;

export const ownsTransaction = (storage: DurableObjectStorage, depth: number): boolean =>
  views.get(storage)?.frame?.sqlDepth === depth;

export const invalidate = (storage: DurableObjectStorage): void => {
  const view = views.get(storage);

  if (view !== undefined) view.rows = undefined;
};

/** Callers reserve the shared SQL connection through commit. Keep the complete view
 * during a transaction, then retain at most 128 lanes between transactions. Intent
 * flushes before source commit; claims and prearming commit before fallible work. */
export const make = (storage: DurableObjectStorage) => {
  // Instrumentation may return a fresh SQL wrapper on every access.
  const sql = storage.sql;
  let view = views.get(storage);

  if (view === undefined) {
    view = {
      rows: undefined,
      frame: undefined,
      retained: 0,
      changes: new Map(),
      deadlines: new Map(),
    };
    views.set(storage, view);
  }
  const current = view;

  const trim = () => {
    if (current.frame === undefined && current.retained === 0 && (current.rows?.length ?? 0) > 128)
      current.rows = undefined;
  };

  /** Retain the pass's write-through view without deferring claims or source commits.
   * Failed source transactions still invalidate it; large views are released at exit. */
  const withView = <A, E, R>(body: Effect.Effect<A, E, R>) =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        current.retained++;
      }),
      () => body,
      () =>
        Effect.sync(() => {
          current.retained--;
          trim();
        }),
    );

  const begin = (sqlDepth?: number): Frame => {
    const frame: Frame = {
      parent: current.frame,
      sqlDepth,
      rows: current.rows,
      changes: current.changes,
      deadlines: current.deadlines,
    };

    current.frame = frame;
    current.changes = new Map(current.changes);
    current.deadlines = new Map(current.deadlines);

    return frame;
  };

  const finish = (frame: Frame, success: boolean) => {
    current.frame = frame.parent;
    if (!success) {
      // A child rollback must retain its parent's still-unflushed intent.
      current.rows = frame.parent === undefined ? undefined : frame.rows;
      current.changes = frame.changes;
      current.deadlines = frame.deadlines;
    }
    if (frame.parent === undefined) {
      current.changes.clear();
      current.deadlines.clear();
      trim();
    }
  };

  // Nine bindings per lane stay below Durable Object SQLite's 100-parameter limit.
  const write = (changes: ReadonlyArray<Change>) => {
    const rows = decode(
      sql
        .exec(
          `WITH changes (id, revision, dueAt, stalls, progressKey, notBefore, state, reported, expected_revision) AS (
            VALUES ${changes.map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?)").join(", ")}
          ) INSERT INTO platform_cloudflare_due_queue
            (id, revision, dueAt, stalls, progressKey, notBefore, state, reported)
          SELECT id, revision, dueAt, stalls, progressKey, notBefore, state, reported FROM changes WHERE 1
          ON CONFLICT(id) DO UPDATE SET revision = excluded.revision, dueAt = excluded.dueAt,
            stalls = excluded.stalls, progressKey = excluded.progressKey,
            notBefore = excluded.notBefore, state = excluded.state, reported = excluded.reported
          WHERE platform_cloudflare_due_queue.revision = (
            SELECT expected_revision FROM changes WHERE changes.id = excluded.id
          ) RETURNING id, revision, dueAt, stalls, progressKey, notBefore, state, reported`,
          ...changes.flatMap(({ before, after }) => [
            after.id,
            after.revision,
            after.dueAt,
            after.stalls,
            after.progressKey,
            after.notBefore,
            after.state,
            after.reported,
            before?.revision ?? -1,
          ]),
        )
        .toArray(),
    );

    // Partial revision acceptance must reject the enclosing source transaction.
    if (rows.length !== changes.length)
      throw new Error("Maintenance due revision changed during transaction");
  };

  const flush = (frame: Frame) => {
    if (frame.parent !== undefined) return;

    const changes = [...current.changes.values()].filter(
      ({ before, after }) => before === undefined || !equivalent(before, after),
    );

    for (let offset = 0; offset < changes.length; offset += 11)
      write(changes.slice(offset, offset + 11));
  };

  /** Own the flush inside the source transaction without changing the SQL service.
   * Acquire its shared connection permit through Effect requirements. Cleanup follows
   * native settlement, including commit rejection or interruption. */
  const withTransaction = <A, E, R>(body: Effect.Effect<A, E, R>) =>
    Effect.flatMap(SqlClient, (client) =>
      Effect.flatMap(Effect.serviceOption(client.transactionService), (ambient) => {
        if (ambient._tag === "Some" && !ownsTransaction(storage, ambient.value[1]))
          return Effect.fail(
            SqlError.make({
              reason: UnknownError.make({
                operation: "begin maintenance source transaction",
                message:
                  "Use ThreadMutationGate.withTransaction at the outermost source transaction",
                cause: new Error(
                  "Unwrapped SQL transaction cannot own buffered maintenance intent",
                ),
              }),
            }),
          );

        return Effect.uninterruptibleMask((restore) =>
          Effect.suspend(() => {
            let frame: Frame | undefined;

            return client
              .withTransaction(
                Effect.flatMap(Effect.serviceOption(client.transactionService), (transaction) =>
                  Effect.sync(() => {
                    if (transaction._tag === "None") throw new Error("SQL transaction unavailable");
                    frame = begin(transaction.value[1]);
                  }),
                ).pipe(
                  Effect.andThen(restore(body)),
                  Effect.tap(() =>
                    Effect.try({
                      try: () => {
                        if (frame !== undefined) flush(frame);
                      },
                      catch: (cause) =>
                        SqlError.make({
                          reason: UnknownError.make({
                            cause,
                            operation: "flush maintenance due queue",
                            message: "Maintenance intent could not commit",
                          }),
                        }),
                    }),
                  ),
                ),
              )
              .pipe(
                Effect.onExit((exit) =>
                  Effect.sync(() => {
                    if (frame !== undefined) finish(frame, Exit.isSuccess(exit));
                  }),
                ),
                // A released permit can resume another transaction. Do not auto-yield
                // between the driver's settlement and removing this transaction's frame.
                Effect.provideService(Scheduler.PreventSchedulerYield, true),
              );
          }),
        );
      }),
    );

  const transaction = async <A>(
    body: (transaction: DurableObjectTransaction) => Promise<A>,
  ): Promise<A> => {
    let frame: Frame | undefined;

    try {
      const result = await storage.transaction(async (transaction) => {
        frame = begin();
        const result = await body(transaction);

        flush(frame);

        return result;
      });

      if (frame !== undefined) finish(frame, true);

      return result;
    } catch (cause) {
      if (frame !== undefined) finish(frame, false);
      throw cause;
    }
  };

  const initialize = () => {
    sql.exec(`CREATE TABLE IF NOT EXISTS platform_cloudflare_due_queue (
    id TEXT PRIMARY KEY, revision INTEGER NOT NULL, dueAt REAL, stalls INTEGER NOT NULL,
    progressKey TEXT, notBefore REAL NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending',
    reported INTEGER NOT NULL DEFAULT 0
  )`);

    const columns = new Set(
      sql
        .exec("PRAGMA table_info(platform_cloudflare_due_queue)")
        .toArray()
        .map((row) => row.name),
    );

    if (!columns.has("reported"))
      sql.exec(
        "ALTER TABLE platform_cloudflare_due_queue ADD COLUMN reported INTEGER NOT NULL DEFAULT 0",
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
          "SELECT id, revision, dueAt, stalls, progressKey, notBefore, state, reported FROM platform_cloudflare_due_queue",
        )
        .toArray(),
    );

    if (current.frame !== undefined || current.retained > 0 || rows.length <= 128)
      current.rows = rows;

    return rows;
  };

  const set = (after: DueLane) => {
    const rows = read();
    const before = rows.find((row) => row.id === after.id);

    if (before !== undefined && equivalent(before, after)) return before;
    const previous = current.changes.get(after.id);
    const change = { before: previous === undefined ? before : previous.before, after };

    if (current.frame === undefined) write([change]);
    else current.changes.set(after.id, change);
    current.rows = [...rows.filter((row) => row.id !== after.id), after];
    trim();

    return after;
  };

  const dirty = (id: string, dueAt: number, progress = false, progressKey?: string) => {
    Schema.decodeSync(LaneId)(id);
    Schema.decodeSync(Schema.Finite)(dueAt);
    const row = read().find((row) => row.id === id);
    const earliest = Math.min(current.deadlines.get(id) ?? dueAt, dueAt);

    if (current.frame !== undefined) current.deadlines.set(id, earliest);

    return set(
      DueLane.make({
        id,
        revision: row === undefined ? 1 : row.revision + Number(progress),
        dueAt:
          row === undefined || progress
            ? earliest
            : Math.max(row.notBefore, Math.min(row.dueAt ?? Infinity, earliest)),
        state:
          progress || (row?.stalls ?? 0) < MaximumNoProgressRearms
            ? "pending"
            : (row?.state ?? "pending"),
        stalls: progress ? 0 : (row?.stalls ?? 0),
        notBefore: progress ? 0 : (row?.notBefore ?? 0),
        reported: progress ? 0 : (row?.reported ?? 0),
        progressKey: progressKey ?? row?.progressKey ?? null,
      }),
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
    if (read().some((row) => row.id === id)) return;
    set(
      DueLane.make({
        id,
        revision: 0,
        dueAt: 0,
        stalls: 0,
        progressKey: null,
        notBefore: 0,
        state: "pending",
        reported: 0,
      }),
    );
  };

  /** This transaction must commit before fallible work: never defer a debit across a wave. */
  const claim = (lane: DueLane, nowMillis: number, native = false) => {
    const row = read().find((row) => row.id === lane.id);

    if (
      row === undefined ||
      row.revision !== lane.revision ||
      row.dueAt === null ||
      !(row.dueAt <= nowMillis || (native && row.notBefore <= nowMillis))
    )
      return;
    const stalls = Math.min(row.stalls + 1, MaximumNoProgressRearms);

    const notBefore =
      nowMillis +
      (stalls >= MaximumNoProgressRearms
        ? ParkedRetryMillis
        : Math.min(60_000, MinimumRetryMillis * 2 ** row.stalls));

    return set(
      DueLane.make({
        ...row,
        stalls,
        notBefore,
        dueAt: notBefore,
        state: stalls >= MaximumNoProgressRearms ? "parked" : "pending",
      }),
    );
  };

  const complete = (lane: DueLane, dueAt: number | null) => {
    Schema.decodeSync(Deadline)(dueAt);
    const row = read().find((row) => row.id === lane.id);

    if (row === undefined || row.revision !== lane.revision) return;

    const after = DueLane.make({
      ...row,
      dueAt: dueAt === null ? null : Math.max(dueAt, row.notBefore),
      state: dueAt === null ? "idle" : row.stalls >= MaximumNoProgressRearms ? "parked" : "pending",
      stalls: dueAt === null ? 0 : row.stalls,
      notBefore: dueAt === null ? 0 : row.notBefore,
      reported: dueAt === null ? 0 : row.reported,
    });

    return set(DueLane.make({ ...after, revision: row.revision + 1 }));
  };

  /** Native checkpoints already fence their own dirty/processed generation. */
  const checkpointNative = (dueAt: number | null) => {
    Schema.decodeSync(Deadline)(dueAt);
    const rows = read();
    const row = rows.find((row) => row.id === Native);

    if (row === undefined) return;
    const publication = rows.find((row) => row.id === Publication);

    set(
      DueLane.make({
        ...row,
        dueAt:
          dueAt === null || publication?.state === "parked" ? null : Math.max(dueAt, row.notBefore),
        state:
          dueAt === null ? "idle" : row.stalls >= MaximumNoProgressRearms ? "parked" : "pending",
        stalls: dueAt === null ? 0 : row.stalls,
        notBefore: dueAt === null ? 0 : row.notBefore,
        reported: dueAt === null ? 0 : row.reported,
      }),
    );
  };

  const takeParkedReports = () =>
    read()
      .filter((row) => row.state === "parked" && row.reported === 0)
      .map((row) => set(DueLane.make({ ...row, reported: 1 })));

  return {
    initialize,
    withView,
    withTransaction,
    transaction,
    read,
    dirty,
    progress,
    register,
    claim,
    complete,
    checkpointNative,
    takeParkedReports,
  };
};

export const next = (rows: ReadonlyArray<DueLane>) =>
  rows.reduce((next, row) => Math.min(next, row.dueAt ?? Infinity), Infinity);
