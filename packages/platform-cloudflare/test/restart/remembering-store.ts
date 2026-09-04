import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import * as Remembering from "@effect-agent/core/RememberingStore";
import { Effect, Schema } from "effect";

import { Source } from "./remembering-contract.ts";

export class HostFailure extends Schema.TaggedError<HostFailure>()("RememberingFixtureFailure", {
  reason: Schema.Literals(["storage", "injected", "corrupt", "capacity", "retry", "denied"]),
  operation: Schema.String,
}) {}

export const Namespaces = MemoryNamespace.define({
  name: "test/remembering-owner",
  version: 1,
  identity: Schema.String,
});

export const namespace = Namespaces.make("owner");
export const target = { namespace, id: "profile" };

const compare = (left: Remembering.SourcePosition, right: Remembering.SourcePosition): number => {
  const compared = Remembering.comparePosition(left, right);

  if (compared === undefined) throw Remembering.AdmissionError.make({ reason: "suppressed" });

  return compared;
};

const JsonRow = Schema.Struct({ value: Schema.String });
const CountRow = Schema.Struct({ count: Schema.Natural });
const Lineage = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const OwnerHeader = Schema.Struct({ format: Schema.Literal(1), lineage: Lineage });
const referenceCodec = Schema.fromJsonString(Remembering.Checkpoint);
const sourceCodec = Schema.fromJsonString(Source);
const eventCodec = Schema.fromJsonString(Remembering.Invalidation);
const intentCodec = Schema.fromJsonString(Remembering.Intent.Wire);
const encodeCheckpoint = Schema.encodeSync(referenceCodec);
const bytes = (value: string) => new TextEncoder().encode(value).byteLength;

/** A compiling application binding, not a framework queue. References, suppression, receipts,
 * source rows and outbox rows share one owner database with the existing MemoryWriter profile.
 * This fixture never expires them: its declared 30-day replay/restore window is a minimum.
 * At bounded capacity it refuses new work, never an existing replay or cleanup obligation.
 */
export class OwnerStore implements Remembering.Store<HostFailure> {
  failAt = "";
  readonly maxActive = 2;
  readonly maxReferences = 64;
  readonly maxCheckpointBytes = 32_768;
  // Reserve a full bounded checkpoint for every retained reference, including suppression.
  readonly maxReferenceBytes = this.maxReferences * this.maxCheckpointBytes;

  constructor(
    readonly storage: DurableObjectStorage,
    private readonly externalLineage: string | undefined,
  ) {
    // Leave storage untouched until the host supplies valid external authority. Handlers
    // report this configuration failure through validate instead of a constructor defect.
    if (!Schema.is(Lineage)(externalLineage)) return;
    storage.transactionSync(() => {
      const existing = Schema.decodeUnknownSync(CountRow)(
        storage.sql
          .exec(
            "SELECT count(*) AS count FROM sqlite_master WHERE type = 'table' AND name GLOB 'remembering_*'",
          )
          .one(),
      );

      // A missing header beside old tables is an incompatible restore, never a fresh owner.
      if (existing.count > 0) return;
      storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS remembering_owner (singleton INTEGER PRIMARY KEY, format INTEGER NOT NULL, lineage TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS remembering_sources (id TEXT PRIMARY KEY, source_id TEXT NOT NULL, sequence INTEGER NOT NULL, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS remembering_outbox (id TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS remembering_references (id TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS remembering_jobs (id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS remembering_suppression (id TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS remembering_invalidations (id TEXT PRIMARY KEY, value TEXT NOT NULL);
      `);
      storage.sql.exec("INSERT INTO remembering_owner VALUES (1, 1, ?)", externalLineage);
    });
  }

  get configuredLineage(): string {
    if (!Schema.is(Lineage)(this.externalLineage))
      throw HostFailure.make({ reason: "corrupt", operation: "owner lineage configuration" });

    return this.externalLineage;
  }

  private validateOwner(): void {
    const configuredLineage = this.configuredLineage;

    const header = Schema.decodeUnknownSync(OwnerHeader)(
      this.storage.sql
        .exec("SELECT format, lineage FROM remembering_owner WHERE singleton = 1")
        .one(),
    );

    if (header.lineage !== configuredLineage)
      throw HostFailure.make({
        reason: "corrupt",
        operation: "owner lineage requires external reconciliation",
      });
  }

  readonly validate = Effect.try({
    try: () => this.validateOwner(),
    catch: (cause) =>
      Schema.is(HostFailure)(cause)
        ? cause
        : HostFailure.make({
            reason: "corrupt",
            operation: "owner lineage requires external reconciliation",
          }),
  });

  /** Test-only corruption of a persisted restore header. There is deliberately no repair path. */
  corruptHeader = (kind: "missing" | "incompatible") =>
    this.transaction("test:header", () => {
      this.storage.sql.exec(
        kind === "missing"
          ? "DROP TABLE remembering_owner"
          : "UPDATE remembering_owner SET format = 2 WHERE singleton = 1",
      );
    });

  hit(point: string): void {
    if (this.failAt !== point) return;
    this.failAt = "";
    throw HostFailure.make({ reason: "injected", operation: point });
  }

  transaction = <A>(point: string, body: () => A) =>
    Effect.try({
      try: () => {
        this.validateOwner();
        this.hit(`${point}:before`);
        const result = this.storage.transactionSync(body);

        this.hit(`${point}:after`);

        return result;
      },
      catch: (cause) =>
        Schema.is(HostFailure)(cause) ||
        Schema.is(Remembering.AdmissionError)(cause) ||
        Schema.is(Remembering.CheckpointError)(cause)
          ? cause
          : HostFailure.make({ reason: "storage", operation: point }),
    });

  private value(table: string, id: string): string | null {
    const row = this.storage.sql.exec(`SELECT value FROM ${table} WHERE id = ?`, id).toArray()[0];

    return row === undefined ? null : Schema.decodeUnknownSync(JsonRow)(row).value;
  }

  count(table: "sources" | "outbox" | "references" | "jobs"): number {
    return Schema.decodeUnknownSync(CountRow)(
      this.storage.sql.exec(`SELECT count(*) AS count FROM remembering_${table}`).one(),
    ).count;
  }

  source(id: string): Source | null {
    const row = this.storage.sql
      .exec(
        "SELECT value FROM remembering_sources WHERE source_id = ? ORDER BY sequence DESC LIMIT 1",
        id,
      )
      .toArray()[0];

    return row === undefined
      ? null
      : Schema.decodeUnknownSync(sourceCodec)(Schema.decodeUnknownSync(JsonRow)(row).value);
  }

  visible(id: string, revision: string): boolean {
    const source = this.source(id);

    if (source === null || source.revision !== revision || source.author !== "human") return false;
    const raw = this.value("remembering_suppression", id);

    if (raw === null) return true;
    const event = Schema.decodeUnknownSync(eventCodec)(raw);

    return event.reason === "source-edit" && source.sequence >= event.position.sequence;
  }

  intent(id: string, source: Source): Remembering.Intent {
    return Remembering.Intent.make({
      version: 1,
      id,
      invocationId: `invocation:${id}`,
      source: {
        key: { namespace, id: source.id },
        locator: `chat://${source.id}`,
        revision: source.revision,
        position: { authorityGeneration: this.configuredLineage, sequence: source.sequence },
      },
      target,
    });
  }

  references(): ReadonlyArray<Remembering.Checkpoint> {
    return this.storage.sql
      .exec("SELECT value FROM remembering_references ORDER BY id")
      .toArray()
      .map((row) =>
        Schema.decodeUnknownSync(referenceCodec)(Schema.decodeUnknownSync(JsonRow)(row).value),
      );
  }

  pending(): ReadonlyArray<Remembering.Intent> {
    return this.storage.sql
      .exec(`SELECT r.value FROM remembering_references r
      JOIN remembering_jobs j ON j.id = r.id ORDER BY j.rowid LIMIT 2`)
      .toArray()
      .map(
        (row) =>
          Schema.decodeUnknownSync(referenceCodec)(Schema.decodeUnknownSync(JsonRow)(row).value)
            .intent,
      );
  }

  outbox(): ReadonlyArray<Remembering.Intent> {
    return this.storage.sql
      .exec("SELECT value FROM remembering_outbox ORDER BY rowid LIMIT 2")
      .toArray()
      .map((row) =>
        Schema.decodeUnknownSync(intentCodec)(Schema.decodeUnknownSync(JsonRow)(row).value),
      );
  }

  private checkpoint(intent: Remembering.Intent): Remembering.Checkpoint {
    const value = this.value("remembering_references", intent.id);

    if (value === null) throw Remembering.CheckpointError.make({ reason: "missing" });
    const checkpoint = Schema.decodeUnknownSync(referenceCodec)(value);

    if (
      Schema.encodeSync(intentCodec)(checkpoint.intent) !== Schema.encodeSync(intentCodec)(intent)
    )
      throw Remembering.CheckpointError.make({ reason: "fenced" });

    return checkpoint;
  }

  private persist(checkpoint: Remembering.Checkpoint): void {
    const encoded = encodeCheckpoint(checkpoint);
    const existing = this.value("remembering_references", checkpoint.intent.id);
    const total = this.references().reduce((sum, ref) => sum + bytes(encodeCheckpoint(ref)), 0);

    if (
      bytes(encoded) > this.maxCheckpointBytes ||
      total - bytes(existing ?? "") + bytes(encoded) > this.maxReferenceBytes
    )
      throw HostFailure.make({ reason: "capacity", operation: "checkpoint bytes" });
    this.storage.sql.exec(
      "INSERT OR REPLACE INTO remembering_references VALUES (?, ?)",
      checkpoint.intent.id,
      encoded,
    );
    if (
      checkpoint.progress._tag === "Completed" &&
      (checkpoint.suppression === null || checkpoint.progress.cleaned)
    )
      this.storage.sql.exec("DELETE FROM remembering_jobs WHERE id = ?", checkpoint.intent.id);
    else
      this.storage.sql.exec(
        "INSERT OR IGNORE INTO remembering_jobs VALUES (?)",
        checkpoint.intent.id,
      );
  }

  private admitted(intent: Remembering.Intent): Remembering.Admission {
    intent = Schema.decodeUnknownSync(Remembering.Intent.Wire)(intent);
    const encoded = Schema.encodeSync(intentCodec)(intent);
    const existing = this.value("remembering_references", intent.id);

    if (existing !== null) {
      const checkpoint = Schema.decodeUnknownSync(referenceCodec)(existing);

      if (Schema.encodeSync(intentCodec)(checkpoint.intent) !== encoded)
        throw Remembering.AdmissionError.make({ reason: "conflict" });

      return Remembering.Admission.make({ id: intent.id, status: "duplicate" });
    }
    const source = this.source(intent.source.key.id);
    const suppression = this.value("remembering_suppression", intent.source.key.id);
    const event = suppression === null ? null : Schema.decodeUnknownSync(eventCodec)(suppression);

    if (
      !source ||
      source.revision !== intent.source.revision ||
      source.sequence !== intent.source.position.sequence ||
      intent.source.position.authorityGeneration !== this.configuredLineage ||
      intent.source.key.namespace.address !== namespace.address ||
      intent.target.namespace.address !== namespace.address ||
      (event !== null &&
        (event.reason === "forget" ||
          compare(intent.source.position, event.position) <
            (event.reason === "source-edit" ? 0 : 1)))
    )
      throw Remembering.AdmissionError.make({ reason: "suppressed" });
    if (this.count("jobs") >= this.maxActive || this.count("references") >= this.maxReferences)
      throw Remembering.AdmissionError.make({ reason: "capacity" });
    this.persist(
      Remembering.Checkpoint.make({
        version: 0,
        intent,
        suppression: null,
        progress: { _tag: "Pending" },
      }),
    );

    return Remembering.Admission.make({ id: intent.id, status: "queued" });
  }

  admit: Remembering.Store<HostFailure>["admit"] = (intent) =>
    this.transaction("admission", () => this.admitted(intent)).pipe(
      Effect.catchTag("RememberingCheckpointError", () =>
        Effect.fail(HostFailure.make({ reason: "corrupt", operation: "admission" })),
      ),
    );

  read: Remembering.Store<HostFailure>["read"] = (intent) =>
    this.transaction("read", () => this.checkpoint(intent)).pipe(
      Effect.catchTag("RememberingAdmissionError", () =>
        Effect.fail(HostFailure.make({ reason: "corrupt", operation: "read" })),
      ),
    );

  save: Remembering.Store<HostFailure>["save"] = ({ intent, expectedVersion, progress }) =>
    this.transaction(`save:${progress._tag}`, () => {
      const current = this.checkpoint(intent);

      if (current.version !== expectedVersion)
        throw Remembering.CheckpointError.make({ reason: "fenced" });

      const updated = Remembering.Checkpoint.make({
        ...current,
        version: current.version + 1,
        progress,
      });

      this.persist(updated);

      return updated;
    }).pipe(
      Effect.catchTag("RememberingAdmissionError", () =>
        Effect.fail(HostFailure.make({ reason: "corrupt", operation: "save" })),
      ),
    );

  private invalidated(event: Remembering.Invalidation): Remembering.InvalidationReceipt {
    event = Schema.decodeUnknownSync(Remembering.Invalidation)(event);
    if (
      event.source.namespace.address !== namespace.address ||
      event.position.authorityGeneration !== this.configuredLineage ||
      this.source(event.source.id) === null
    )
      throw Remembering.AdmissionError.make({ reason: "invalid-input" });
    const encoded = Schema.encodeSync(eventCodec)(event);
    const receipt = this.value("remembering_invalidations", event.id);

    if (receipt !== null) {
      if (receipt !== encoded) throw Remembering.AdmissionError.make({ reason: "conflict" });

      return Remembering.InvalidationReceipt.make({
        id: event.id,
        status: "duplicate",
        affected: 0,
      });
    }

    const receipts = this.storage.sql
      .exec("SELECT value FROM remembering_invalidations")
      .toArray()
      .map((row) => Schema.decodeUnknownSync(JsonRow)(row).value);

    if (
      receipts.length >= 256 ||
      receipts.reduce((sum, value) => sum + bytes(value), bytes(encoded)) > 262_144
    )
      throw Remembering.AdmissionError.make({ reason: "capacity" });
    const previous = this.value("remembering_suppression", event.source.id);

    if (previous !== null) {
      const prior = Schema.decodeUnknownSync(eventCodec)(previous);

      if (
        prior.reason === "forget" ||
        compare(prior.position, event.position) > 0 ||
        (compare(prior.position, event.position) === 0 &&
          prior.reason !== "source-edit" &&
          event.reason === "source-edit")
      ) {
        this.storage.sql.exec(
          "INSERT INTO remembering_invalidations VALUES (?, ?)",
          event.id,
          encoded,
        );

        return Remembering.InvalidationReceipt.make({ id: event.id, status: "stale", affected: 0 });
      }
    }
    let affected = 0;

    for (const checkpoint of this.references()) {
      if (
        checkpoint.intent.source.key.id !== event.source.id ||
        compare(checkpoint.intent.source.position, event.position) >=
          (event.reason === "source-edit" ? 0 : 1)
      )
        continue;
      this.persist(
        Remembering.Checkpoint.make({
          ...checkpoint,
          version: checkpoint.version + 1,
          suppression: event,
        }),
      );
      affected++;
    }
    this.storage.sql.exec(
      "INSERT OR REPLACE INTO remembering_suppression VALUES (?, ?)",
      event.source.id,
      encoded,
    );
    this.storage.sql.exec("INSERT INTO remembering_invalidations VALUES (?, ?)", event.id, encoded);

    return Remembering.InvalidationReceipt.make({ id: event.id, status: "accepted", affected });
  }

  invalidate: Remembering.Store<HostFailure>["invalidate"] = (event) =>
    this.transaction("suppression", () => this.invalidated(event)).pipe(
      Effect.catchTag("RememberingCheckpointError", () =>
        Effect.fail(HostFailure.make({ reason: "corrupt", operation: "invalidate" })),
      ),
    );

  commit = (source: Source, automatic: boolean) =>
    this.transaction("source", () => {
      source = Schema.decodeUnknownSync(Source)(source);
      const previous = this.source(source.id);

      if (
        previous !== null &&
        (source.sequence < previous.sequence ||
          (source.sequence === previous.sequence &&
            JSON.stringify(source) !== JSON.stringify(previous)))
      )
        throw Remembering.AdmissionError.make({ reason: "conflict" });
      const rowId = JSON.stringify([source.id, source.revision]);
      const existing = this.value("remembering_sources", rowId);

      if (existing !== null && existing !== Schema.encodeSync(sourceCodec)(source))
        throw Remembering.AdmissionError.make({ reason: "conflict" });
      if (existing === null && this.count("sources") >= 128)
        throw HostFailure.make({ reason: "capacity", operation: "source rows" });
      if (previous !== null && previous.revision !== source.revision)
        this.invalidated(
          Remembering.Invalidation.make({
            version: 1,
            id: `edit:${source.id}:${source.sequence}`,
            source: { namespace, id: source.id },
            position: { authorityGeneration: this.configuredLineage, sequence: source.sequence },
            reason: "source-edit",
          }),
        );
      this.storage.sql.exec(
        "INSERT OR REPLACE INTO remembering_sources VALUES (?, ?, ?, ?)",
        rowId,
        source.id,
        source.sequence,
        Schema.encodeSync(sourceCodec)(source),
      );
      if (automatic) {
        const intent = this.intent(`automatic:${source.id}:${source.revision}`, source);

        // One bounded outbox envelope per retained source revision. Admission backlog cannot
        // exhaust an additional quota after the authoritative source commit has been accepted.
        this.storage.sql.exec(
          "INSERT OR IGNORE INTO remembering_outbox VALUES (?, ?)",
          intent.id,
          Schema.encodeSync(intentCodec)(intent),
        );
      }
    });

  admitOutbox = (intent: Remembering.Intent) =>
    this.transaction("outbox", () => {
      let admission: Remembering.Admission | null;

      try {
        admission = this.admitted(intent);
      } catch (cause) {
        if (!Schema.is(Remembering.AdmissionError)(cause) || cause.reason !== "suppressed")
          throw cause;
        // Superseded source revisions are conclusively refused. Their authoritative source
        // rows and suppression remain retained; capacity failures keep the ready envelope.
        admission = null;
      }
      this.storage.sql.exec("DELETE FROM remembering_outbox WHERE id = ?", intent.id);

      return admission;
    });
}
