import { ConversationId, IdGenerator, RunId, TurnId } from "@effect-agent/core";
import { Context, Deferred, Effect, Layer, Option, Ref, Schema } from "effect";

import {
  ActivityCatalog,
  ActivitySearchResult,
  ActivityUnavailable,
  FlightCatalog,
  FlightOption,
  FlightUnavailable,
  LodgingCatalog,
  LodgingOption,
  LodgingUnavailable,
  QuoteId,
  TravelGuidance,
  TravelPlannerToolkit,
  TravelPlannerToolkitLayer,
} from "./definition.ts";

export class CatalogLifecycleCounts extends Schema.Class<CatalogLifecycleCounts>(
  "CatalogLifecycleCounts",
)({
  acquired: Schema.Natural,
  finalized: Schema.Natural,
}) {}
export class CatalogLifecycle extends Context.Service<
  CatalogLifecycle,
  {
    readonly markAcquired: Effect.Effect<void>;
    readonly markFinalized: Effect.Effect<void>;
    readonly counts: Effect.Effect<CatalogLifecycleCounts>;
  }
>()("@effect-agent/testing/travel-planner/CatalogLifecycle") {
  static readonly layerNoDeps = Layer.effect(
    this,
    Effect.gen(function* () {
      const acquired = yield* Ref.make(0);
      const finalized = yield* Ref.make(0);
      return CatalogLifecycle.of({
        markAcquired: Ref.update(acquired, (n) => n + 1),
        markFinalized: Ref.update(finalized, (n) => n + 1),
        counts: Effect.all({ acquired: Ref.get(acquired), finalized: Ref.get(finalized) }).pipe(
          Effect.map((counts) => CatalogLifecycleCounts.make(counts)),
        ),
      });
    }),
  );
}

const flight = FlightOption.make({
  quoteId: Schema.decodeSync(QuoteId)("quote-sfo-lhr-001"),
  flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
  estimatedCents: 180_000,
  currency: "USD",
});
const lodging = LodgingOption.make({
  lodging: "Bloomsbury House · refundable studio · 4 nights",
  estimatedCents: 104_000,
  currency: "USD",
});
const activities = ActivitySearchResult.make({
  activities: ["British Museum timed entry", "Thames evening walk"],
});

/**
 * Deterministic controls for a Tool batch whose completions are released in a
 * caller-selected order. This is intentionally a test fixture: it uses no
 * clock or sleep and lets engine scheduler tests prove parallel starts and
 * declaration-order prompt materialization.
 */
export interface TravelPlannerCompletionControls {
  readonly flightStarted: Effect.Effect<void>;
  readonly lodgingStarted: Effect.Effect<void>;
  readonly activityStarted: Effect.Effect<void>;
  readonly releaseFlight: Effect.Effect<void>;
  readonly releaseLodging: Effect.Effect<void>;
  readonly releaseActivity: Effect.Effect<void>;
}

export const ReverseCompletionToolkitLayer = Effect.gen(function* () {
  const flightStarted = yield* Deferred.make<void>();
  const lodgingStarted = yield* Deferred.make<void>();
  const activityStarted = yield* Deferred.make<void>();
  const releaseFlight = yield* Deferred.make<void>();
  const releaseLodging = yield* Deferred.make<void>();
  const releaseActivity = yield* Deferred.make<void>();
  const awaitRelease = <A>(
    started: Deferred.Deferred<void>,
    release: Deferred.Deferred<void>,
    value: A,
  ) =>
    Deferred.succeed(started, undefined).pipe(
      Effect.andThen(Deferred.await(release)),
      Effect.as(value),
    );
  return {
    controls: {
      flightStarted: Deferred.await(flightStarted),
      lodgingStarted: Deferred.await(lodgingStarted),
      activityStarted: Deferred.await(activityStarted),
      releaseFlight: Deferred.succeed(releaseFlight, undefined).pipe(Effect.asVoid),
      releaseLodging: Deferred.succeed(releaseLodging, undefined).pipe(Effect.asVoid),
      releaseActivity: Deferred.succeed(releaseActivity, undefined).pipe(Effect.asVoid),
    },
    layer: TravelPlannerToolkit.toLayer({
      search_flights: () => awaitRelease(flightStarted, releaseFlight, flight),
      search_lodging: () => awaitRelease(lodgingStarted, releaseLodging, lodging),
      search_activities: () => awaitRelease(activityStarted, releaseActivity, activities),
    }),
  };
});

export const FlightCatalogLayer = Layer.effect(
  FlightCatalog,
  Effect.gen(function* () {
    const lifecycle = yield* CatalogLifecycle;
    yield* Effect.acquireRelease(lifecycle.markAcquired, () => lifecycle.markFinalized);
    return FlightCatalog.of({
      search: (query) =>
        query.origin === query.destination
          ? Effect.fail(
              FlightUnavailable.make({
                query: `${query.origin}-${query.destination}`,
                message: "Origin and destination must differ.",
              }),
            )
          : Effect.succeed(flight),
    });
  }),
);
export const LodgingCatalogLayer = Layer.effect(
  LodgingCatalog,
  Effect.gen(function* () {
    const lifecycle = yield* CatalogLifecycle;
    yield* Effect.acquireRelease(lifecycle.markAcquired, () => lifecycle.markFinalized);
    return LodgingCatalog.of({
      search: (query) =>
        query.nights < 1
          ? Effect.fail(
              LodgingUnavailable.make({
                query: query.destination,
                message: "At least one night is required.",
              }),
            )
          : Effect.succeed(lodging),
    });
  }),
);
export const ActivityCatalogLayer = Layer.effect(
  ActivityCatalog,
  Effect.gen(function* () {
    const lifecycle = yield* CatalogLifecycle;
    yield* Effect.acquireRelease(lifecycle.markAcquired, () => lifecycle.markFinalized);
    return ActivityCatalog.of({
      search: (query) =>
        query.destination === ""
          ? Effect.fail(
              ActivityUnavailable.make({
                query: query.destination,
                message: "Destination is required.",
              }),
            )
          : Effect.succeed(activities),
    });
  }),
);
/** Stable supplier-side booking identity, minted deterministically from the idempotency key. */
export const BookingRef = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/testing/travel-planner/BookingRef"),
);
export type BookingRef = typeof BookingRef.Type;

/** The supplier desk operations the P5 booking Tools and Steps invoke. */
export const SupplierOperation = Schema.Literals([
  "book-flight",
  "cancel-booking",
  "reserve-flight",
  "reserve-lodging",
  "issue-confirmation",
]);
export type SupplierOperation = typeof SupplierOperation.Type;

/**
 * One row of external supplier truth. The desk deduplicates by `idempotencyKey` — replaying a
 * call with the same key returns this exact record without creating a second booking — which is
 * precisely the honesty model of DUR-010: the framework never makes an external call
 * exactly-once; the supplier's idempotency key does.
 */
export class SupplierBookingRecord extends Schema.Class<SupplierBookingRecord>(
  "@effect-agent/testing/travel-planner/SupplierBookingRecord",
)({
  bookingRef: BookingRef,
  idempotencyKey: Schema.NonEmptyString,
  operation: SupplierOperation,
  detail: Schema.NonEmptyString,
  status: Schema.Literals(["confirmed", "cancelled"]),
}) {}

export class SupplierUnavailable extends Schema.TaggedError<SupplierUnavailable>()(
  "SupplierUnavailable",
  { message: Schema.String },
) {}

export interface SupplierBookRequest {
  readonly operation: SupplierOperation;
  readonly idempotencyKey: string;
  readonly detail: string;
}

/** Controls returned by an armed crash window (`holdAfterWrite`). */
export interface SupplierHoldControls {
  /** Resolves once the armed call has performed its supplier write and is blocked. */
  readonly held: Effect.Effect<void>;
  /** Releases the blocked call (tests that interrupt the Attempt never call this). */
  readonly release: Effect.Effect<void>;
}

/** The desk-internal idempotency key of one cancellation: cancel is idempotent by bookingRef. */
export const cancelBookingIdempotencyKey = (bookingRef: string): string =>
  `cancel-booking:${bookingRef}`;

/** The deterministic bookingRef the desk mints for one idempotency key. */
export const supplierBookingRefFor = (idempotencyKey: string): BookingRef =>
  Schema.decodeSync(BookingRef)(`ref:${idempotencyKey}`);

interface SupplierHoldWindow {
  readonly held: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
}

interface SupplierDeskState {
  readonly bookings: ReadonlyMap<string, SupplierBookingRecord>;
  readonly counts: ReadonlyMap<string, number>;
  readonly holds: ReadonlyMap<string, SupplierHoldWindow>;
}

/**
 * The deterministic in-memory supplier: an idempotency-keyed booking store with per-key call
 * counters and injectable crash windows.
 *
 * - `book`/`cancel` always count the call (at-least-once execution stays observable), then
 *   dedupe the external effect by idempotency key — the supplier-side contract the P5 Tools and
 *   Steps rely on.
 * - `holdAfterWrite` arms a one-shot crash window: the next call with that key performs its
 *   supplier write, signals `held`, and never returns. Interrupting the Attempt at that point
 *   models "the external effect happened but no outcome was recorded" without any wall clock.
 * - `bookings`/`lookup` expose external truth for the reconciler and for never-fabricate
 *   assertions.
 */
export class SupplierBookingDesk extends Context.Service<
  SupplierBookingDesk,
  {
    readonly book: (
      request: SupplierBookRequest,
    ) => Effect.Effect<SupplierBookingRecord, SupplierUnavailable>;
    readonly cancel: (
      bookingRef: BookingRef,
    ) => Effect.Effect<SupplierBookingRecord, SupplierUnavailable>;
    readonly lookup: (
      idempotencyKey: string,
    ) => Effect.Effect<Option.Option<SupplierBookingRecord>>;
    readonly bookings: Effect.Effect<ReadonlyArray<SupplierBookingRecord>>;
    readonly callCount: (idempotencyKey: string) => Effect.Effect<number>;
    readonly holdAfterWrite: (idempotencyKey: string) => Effect.Effect<SupplierHoldControls>;
  }
>()("@effect-agent/testing/travel-planner/SupplierBookingDesk") {
  static readonly layer: Layer.Layer<SupplierBookingDesk> = Layer.effect(
    this,
    Effect.gen(function* () {
      const state = yield* Ref.make<SupplierDeskState>({
        bookings: new Map(),
        counts: new Map(),
        holds: new Map(),
      });

      const enterHold = (hold: Option.Option<SupplierHoldWindow>) =>
        Option.isSome(hold)
          ? Deferred.succeed(hold.value.held, undefined).pipe(
              Effect.andThen(Deferred.await(hold.value.release)),
            )
          : Effect.void;

      const book = (request: SupplierBookRequest) =>
        Ref.modify(state, (current) => {
          const counts = new Map(current.counts).set(
            request.idempotencyKey,
            (current.counts.get(request.idempotencyKey) ?? 0) + 1,
          );
          const existing = current.bookings.get(request.idempotencyKey);
          const record =
            existing ??
            SupplierBookingRecord.make({
              bookingRef: supplierBookingRefFor(request.idempotencyKey),
              idempotencyKey: request.idempotencyKey,
              operation: request.operation,
              detail: request.detail,
              status: "confirmed",
            });
          const bookings =
            existing === undefined
              ? new Map(current.bookings).set(request.idempotencyKey, record)
              : current.bookings;
          const hold = Option.fromNullishOr(current.holds.get(request.idempotencyKey));
          const holds = Option.isSome(hold)
            ? (() => {
                const next = new Map(current.holds);
                next.delete(request.idempotencyKey);
                return next;
              })()
            : current.holds;
          return [
            { record, hold },
            { bookings, counts, holds },
          ] as const;
        }).pipe(Effect.flatMap(({ hold, record }) => enterHold(hold).pipe(Effect.as(record))));

      const cancel = (bookingRef: BookingRef) =>
        Ref.modify(state, (current) => {
          const key = cancelBookingIdempotencyKey(bookingRef);
          const counts = new Map(current.counts).set(key, (current.counts.get(key) ?? 0) + 1);
          const existingEntry = [...current.bookings.entries()].find(
            ([, record]) => record.bookingRef === bookingRef,
          );
          if (existingEntry === undefined) {
            return [
              { record: Option.none<SupplierBookingRecord>(), hold: Option.none() },
              { ...current, counts },
            ] as const;
          }
          const [storeKey, existing] = existingEntry;
          const cancelled =
            existing.status === "cancelled"
              ? existing
              : SupplierBookingRecord.make({ ...existing, status: "cancelled" });
          const bookings = new Map(current.bookings).set(storeKey, cancelled);
          const hold = Option.fromNullishOr(current.holds.get(key));
          const holds = Option.isSome(hold)
            ? (() => {
                const next = new Map(current.holds);
                next.delete(key);
                return next;
              })()
            : current.holds;
          return [
            { record: Option.some(cancelled), hold },
            { bookings, counts, holds },
          ] as const;
        }).pipe(
          Effect.flatMap(({ hold, record }) =>
            Option.isNone(record)
              ? Effect.fail(
                  SupplierUnavailable.make({
                    message: `The supplier desk has no booking under ${bookingRef}.`,
                  }),
                )
              : enterHold(hold).pipe(Effect.as(record.value)),
          ),
        );

      return SupplierBookingDesk.of({
        book,
        cancel,
        lookup: (idempotencyKey) =>
          Ref.get(state).pipe(
            Effect.map((current) => Option.fromNullishOr(current.bookings.get(idempotencyKey))),
          ),
        bookings: Ref.get(state).pipe(Effect.map((current) => [...current.bookings.values()])),
        callCount: (idempotencyKey) =>
          Ref.get(state).pipe(Effect.map((current) => current.counts.get(idempotencyKey) ?? 0)),
        holdAfterWrite: (idempotencyKey) =>
          Effect.gen(function* () {
            const held = yield* Deferred.make<void>();
            const release = yield* Deferred.make<void>();
            yield* Ref.update(state, (current) => ({
              ...current,
              holds: new Map(current.holds).set(idempotencyKey, { held, release }),
            }));
            return {
              held: Deferred.await(held),
              release: Deferred.succeed(release, undefined).pipe(Effect.asVoid),
            };
          }),
      });
    }),
  );
}

export const TravelGuidanceLayer = Layer.succeed(
  TravelGuidance,
  TravelGuidance.of({
    instructions: (input) =>
      Effect.succeed(
        [
          "You are the Effect Agent Travel Planner P1 interpreter fixture.",
          `The user asked: ${input.request}`,
          "Call search_flights, search_lodging, and search_activities exactly once in one Tool batch.",
          "Then return only a JSON object of exactly this shape, no prose:",
          '{"itineraries": [{"title": "<short itinerary name>", "route": "<origin-destination>", "dates": "<date range>", "flight": "<flight description from the Tool result>", "lodging": "<lodging description from the Tool result>", "activities": ["<activity>", "..."], "estimatedTotalCents": <positive integer total in cents>, "currency": "USD", "quoteId": "<quoteId from the flight Tool result>", "assumptions": ["<assumption>", "..."], "unresolvedConstraints": [], "nextAction": "review"}]}',
          "Use the Tool results verbatim; activity results may legitimately be an empty array.",
          "This is read-only planning. Require review before any mutation.",
        ].join("\n"),
      ),
  }),
);
export const DeterministicIdGeneratorLayer = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const conversation = yield* Ref.make(0);
    const run = yield* Ref.make(0);
    const turn = yield* Ref.make(0);
    return IdGenerator.of({
      nextConversationId: Ref.updateAndGet(conversation, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(ConversationId)(`conversation-${n}`)),
      ),
      nextRunId: Ref.updateAndGet(run, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(RunId)(`run-${n}`)),
      ),
      nextTurnId: Ref.updateAndGet(turn, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(TurnId)(`turn-${n}`)),
      ),
    });
  }),
);
export const TravelPlannerRuntimeLayer = Layer.mergeAll(
  TravelPlannerToolkitLayer,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  ActivityCatalogLayer,
  TravelGuidanceLayer,
  DeterministicIdGeneratorLayer,
).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));
