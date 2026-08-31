import {
  ScheduleCapacityError,
  ScheduleDueCursor,
  defaultSchedulingLimits,
  scheduleUsesCapacity,
  ScheduleChange,
  ScheduleConflict,
  ScheduleFailpoint,
  ScheduleKey,
  ScheduleNotFound,
  ScheduleOwner,
  SchedulePageRequest,
  ScheduleRecord,
  ScheduleStorageError,
  ScheduleStore,
  applyScheduleChange,
  compareScheduleKeys,
  compareScheduleNames,
  scheduleDeadline,
  scheduleKeyString,
  scheduleKeyOf,
  scheduleOwnerKey,
} from "@effect-agent/thread";
import { Effect, Layer, Ref, Result, Schema } from "effect";

interface MemoryScheduleState {
  readonly records: ReadonlyMap<string, string>;
}

const storageError = (operation: string, reason: "unavailable" | "corrupt") =>
  ScheduleStorageError.make({ operation, reason });

const encodeRecord = (operation: string, record: ScheduleRecord) =>
  Effect.try({
    try: () => Schema.encodeSync(Schema.fromJsonString(ScheduleRecord))(record),
    catch: () => storageError(operation, "corrupt"),
  });

const decodeRecord = (operation: string, encoded: string) =>
  Effect.try({
    try: () => Schema.decodeSync(Schema.fromJsonString(ScheduleRecord))(encoded),
    catch: () => storageError(operation, "corrupt"),
  });

const decodeInput = <A, I>(operation: string, schema: Schema.Codec<A, I>, value: unknown) =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(schema)(value),
    catch: () => storageError(operation, "corrupt"),
  });

const sameOwner = (record: ScheduleRecord, owner: ScheduleOwner): boolean =>
  scheduleOwnerKey(record.owner) === scheduleOwnerKey(owner);

const makeScheduleStore = Effect.gen(function* () {
  const state = yield* Ref.make<MemoryScheduleState>({ records: new Map() });
  const failpoint = yield* ScheduleFailpoint;

  const insert: ScheduleStore["Service"]["insert"] = Effect.fn("MemoryScheduleStore.insert")(
    (record, ownerLimit) =>
      Effect.gen(function* () {
        const encoded = yield* encodeRecord("insert", record);
        yield* failpoint.hit("schedule:insert:before");
        const decision = yield* Effect.uninterruptible(
          Ref.modify(
            state,
            (
              current,
            ): readonly [
              Result.Result<
                ScheduleRecord,
                ScheduleConflict | ScheduleCapacityError | ScheduleStorageError
              >,
              MemoryScheduleState,
            ] => {
              const key = scheduleKeyString(record);
              const existingText = current.records.get(key);
              if (existingText !== undefined) {
                const decoded = Result.try({
                  try: () => Schema.decodeSync(Schema.fromJsonString(ScheduleRecord))(existingText),
                  catch: () => storageError("insert", "corrupt"),
                });
                if (Result.isFailure(decoded)) {
                  return [Result.fail(decoded.failure), current];
                }
                if (decoded.success.creationFingerprint === record.creationFingerprint) {
                  return [Result.succeed(decoded.success), current];
                }
                return [
                  Result.fail(
                    ScheduleConflict.make({ reason: "creation", key: scheduleKeyOf(record) }),
                  ),
                  current,
                ];
              }

              let ownerCount = 0;
              for (const text of current.records.values()) {
                const decoded = Result.try({
                  try: () => Schema.decodeSync(Schema.fromJsonString(ScheduleRecord))(text),
                  catch: () => storageError("insert", "corrupt"),
                });
                if (Result.isFailure(decoded)) {
                  return [Result.fail(decoded.failure), current];
                }
                if (
                  sameOwner(decoded.success, record.owner) &&
                  scheduleUsesCapacity(decoded.success)
                )
                  ownerCount += 1;
              }
              if (ownerCount >= ownerLimit) {
                return [Result.fail(ScheduleCapacityError.make({ limit: ownerLimit })), current];
              }
              const records = new Map(current.records);
              records.set(key, encoded);
              const next = { records };
              return [Result.succeed(record), next];
            },
          ),
        );
        const inserted = yield* Effect.fromResult(decision);
        yield* failpoint.hit("schedule:insert:after");
        return yield* decodeRecord("insert", yield* encodeRecord("insert", inserted));
      }),
  );

  const get: ScheduleStore["Service"]["get"] = Effect.fn("MemoryScheduleStore.get")(
    function* (key) {
      const decodedKey = yield* decodeInput("get", ScheduleKey, key);
      const text = (yield* Ref.get(state)).records.get(scheduleKeyString(decodedKey));
      return text === undefined ? null : yield* decodeRecord("get", text);
    },
  );

  const list: ScheduleStore["Service"]["list"] = Effect.fn("MemoryScheduleStore.list")(
    function* (request) {
      const decodedRequest = yield* decodeInput("list", SchedulePageRequest, request);
      const records: Array<ScheduleRecord> = [];
      for (const text of (yield* Ref.get(state)).records.values()) {
        const record = yield* decodeRecord("list", text);
        if (
          sameOwner(record, decodedRequest.owner) &&
          (decodedRequest.after === undefined ||
            compareScheduleNames(record.scheduleId, decodedRequest.after) > 0)
        ) {
          records.push(record);
        }
      }
      records.sort((left, right) => compareScheduleNames(left.scheduleId, right.scheduleId));
      const hasNext = records.length > decodedRequest.limit;
      const items = records.slice(0, decodedRequest.limit);
      return {
        items,
        next: hasNext ? (items.at(-1)?.scheduleId ?? null) : null,
      };
    },
  );

  const change: ScheduleStore["Service"]["change"] = Effect.fn("MemoryScheduleStore.change")(
    (key, command, ownerLimit = defaultSchedulingLimits.maxSchedulesPerOwner) =>
      Effect.gen(function* () {
        const decodedKey = yield* decodeInput("change", ScheduleKey, key);
        const decodedCommand = yield* decodeInput("change", ScheduleChange, command);
        yield* failpoint.hit(`schedule:${decodedCommand._tag.toLowerCase()}:before`);
        const decision = yield* Effect.uninterruptible(
          Ref.modify(
            state,
            (
              current,
            ): readonly [
              Result.Result<
                ScheduleRecord,
                ScheduleConflict | ScheduleStorageError | ScheduleNotFound | ScheduleCapacityError
              >,
              MemoryScheduleState,
            ] => {
              const storageKey = scheduleKeyString(decodedKey);
              const text = current.records.get(storageKey);
              if (text === undefined) {
                return [Result.fail(ScheduleNotFound.make({ key: decodedKey })), current];
              }
              const decoded = Result.try({
                try: () => Schema.decodeSync(Schema.fromJsonString(ScheduleRecord))(text),
                catch: () => storageError("change", "corrupt"),
              });
              if (Result.isFailure(decoded)) {
                return [Result.fail(decoded.failure), current];
              }
              const applied = applyScheduleChange(decoded.success, decodedCommand);
              if (Result.isFailure(applied)) {
                return [Result.fail(applied.failure), current];
              }
              if (applied.success === decoded.success) {
                return [Result.succeed(decoded.success), current];
              }
              if (!scheduleUsesCapacity(decoded.success) && scheduleUsesCapacity(applied.success)) {
                let count = 0;
                for (const text of current.records.values()) {
                  const candidate = Schema.decodeUnknownResult(
                    Schema.fromJsonString(ScheduleRecord),
                  )(text);
                  if (Result.isFailure(candidate))
                    return [Result.fail(storageError("change", "corrupt")), current];
                  if (
                    sameOwner(candidate.success, decodedKey.owner) &&
                    scheduleUsesCapacity(candidate.success)
                  )
                    count += 1;
                }
                if (count >= ownerLimit)
                  return [Result.fail(ScheduleCapacityError.make({ limit: ownerLimit })), current];
              }
              const encoded = Result.try({
                try: () =>
                  Schema.encodeSync(Schema.fromJsonString(ScheduleRecord))(applied.success),
                catch: () => storageError("change", "corrupt"),
              });
              if (Result.isFailure(encoded)) {
                return [Result.fail(encoded.failure), current];
              }
              const records = new Map(current.records);
              records.set(storageKey, encoded.success);
              const next = { records };
              return [Result.succeed(applied.success), next];
            },
          ),
        );
        const changed = yield* Effect.fromResult(decision);
        yield* failpoint.hit(`schedule:${decodedCommand._tag.toLowerCase()}:after`);
        return yield* decodeRecord("change", yield* encodeRecord("change", changed));
      }),
  );

  const due: ScheduleStore["Service"]["due"] = Effect.fn("MemoryScheduleStore.due")(
    function* (nowMillis, limit, owner, after) {
      const decodedOwner =
        owner === undefined ? undefined : yield* decodeInput("due", ScheduleOwner, owner);
      const cursor =
        after === undefined ? undefined : yield* decodeInput("due", ScheduleDueCursor, after);
      const records: Array<ScheduleDueCursor> = [];
      for (const text of (yield* Ref.get(state)).records.values()) {
        const record = yield* decodeRecord("due", text);
        const deadline = scheduleDeadline(record);
        if (
          deadline !== null &&
          deadline <= nowMillis &&
          (decodedOwner === undefined || sameOwner(record, decodedOwner)) &&
          (cursor === undefined ||
            deadline > cursor.deadlineAtMillis ||
            (deadline === cursor.deadlineAtMillis && compareScheduleKeys(record, cursor) > 0))
        ) {
          records.push({ ...scheduleKeyOf(record), deadlineAtMillis: deadline });
        }
      }
      records.sort((left, right) => {
        const byDeadline = left.deadlineAtMillis - right.deadlineAtMillis;
        return byDeadline !== 0 ? byDeadline : compareScheduleKeys(left, right);
      });
      return records.slice(0, limit);
    },
  );

  const nextDeadline: ScheduleStore["Service"]["nextDeadline"] = Effect.fn(
    "MemoryScheduleStore.nextDeadline",
  )(function* (owner) {
    const decodedOwner =
      owner === undefined ? undefined : yield* decodeInput("nextDeadline", ScheduleOwner, owner);
    let earliest: number | null = null;
    for (const text of (yield* Ref.get(state)).records.values()) {
      const record = yield* decodeRecord("nextDeadline", text);
      if (decodedOwner !== undefined && !sameOwner(record, decodedOwner)) continue;
      const deadline = scheduleDeadline(record);
      if (deadline !== null && (earliest === null || deadline < earliest)) earliest = deadline;
    }
    return earliest;
  });

  return ScheduleStore.of({ insert, get, list, change, due, nextDeadline });
});

export const memoryScheduleStoreLayer = (): Layer.Layer<ScheduleStore> =>
  Layer.effect(ScheduleStore, makeScheduleStore);

export const MemoryScheduleStoreLive: Layer.Layer<ScheduleStore> = memoryScheduleStoreLayer();
