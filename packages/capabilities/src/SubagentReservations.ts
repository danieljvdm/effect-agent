import { RunId, ToolCallId } from "@effect-agent/core/Identifiers";
import {
  SubagentDelegationCaps,
  SubagentReservationAmounts,
} from "@effect-agent/core/SubagentContract";
import { Context, Effect, Layer, Ref, Schema, type Scope, Semaphore } from "effect";

const Natural = Schema.Natural;

/**
 * Stable identity of one parent-owned Subagent budget reservation.
 *
 * Derivation rule (see {@link makeBudgetReservationId}):
 * `encodeURIComponent(parentRunId) + ":" + encodeURIComponent(parentToolCallId)`.
 * Component encoding escapes `:`; the mapping is injective for every
 * `(parentRunId, parentToolCallId)` pair.
 */
export const BudgetReservationId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/capabilities/BudgetReservationId"),
);

export type BudgetReservationId = typeof BudgetReservationId.Type;

const decodeBudgetReservationId = Schema.decodeSync(BudgetReservationId);

/** Derive the stable reservation identity of one Subagent Invocation. */
export const makeBudgetReservationId = (
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
): BudgetReservationId =>
  decodeBudgetReservationId(
    `${encodeURIComponent(parentRunId)}:${encodeURIComponent(parentToolCallId)}`,
  );

/** One enforced hierarchical delegation budget dimension. */
export const SubagentBudgetDimension = Schema.Literals([
  "total-child-invocations",
  "concurrent-children",
  "turns",
  "tool-calls",
  "duration",
  "input-tokens",
  "output-tokens",
  "cost",
  "result-bytes",
]);

export type SubagentBudgetDimension = typeof SubagentBudgetDimension.Type;

const AmountFields = SubagentReservationAmounts.fields;

const OptionalAmountFields = {
  turns: Schema.optionalKey(Natural),
  toolCalls: Schema.optionalKey(Natural),
  durationMillis: Schema.optionalKey(Natural),
  inputTokens: Schema.optionalKey(Natural),
  outputTokens: Schema.optionalKey(Natural),
  costMicrousd: Schema.optionalKey(Natural),
  resultBytes: Schema.optionalKey(Natural),
} as const;

/** Observed child usage. An absent dimension was never reported and settles conservatively. */
export class SubagentObservedUsage extends Schema.Class<SubagentObservedUsage>(
  "@effect-agent/capabilities/SubagentObservedUsage",
)(OptionalAmountFields) {}

/** Remaining delegable allocation per configured dimension. Absent means not configured. */
export class SubagentAvailableAmounts extends Schema.Class<SubagentAvailableAmounts>(
  "@effect-agent/capabilities/SubagentAvailableAmounts",
)(OptionalAmountFields) {}

/** Reservation lifecycle: absent → reserved → releasePending → released. */
export const SubagentReservationStatus = Schema.Literals([
  "reserved",
  "releasePending",
  "released",
]);

export type SubagentReservationStatus = typeof SubagentReservationStatus.Type;

/** All-or-nothing allocation request for one Subagent Invocation. */
export class SubagentReservationRequest extends Schema.Class<SubagentReservationRequest>(
  "@effect-agent/capabilities/SubagentReservationRequest",
)({
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  allocation: SubagentReservationAmounts,
}) {}

/** Accounting snapshot of one reservation: allocated, observed, covered, released, and overrun. */
export class SubagentReservationView extends Schema.Class<SubagentReservationView>(
  "@effect-agent/capabilities/SubagentReservationView",
)({
  reservationId: BudgetReservationId,
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  status: SubagentReservationStatus,
  allocated: SubagentReservationAmounts,
  observedConsumed: SubagentObservedUsage,
  coveredConsumed: SubagentReservationAmounts,
  released: SubagentReservationAmounts,
  overrun: SubagentReservationAmounts,
}) {}

/** Aggregate parent delegation budget snapshot including every reservation it owns. */
export class SubagentParentBudgetView extends Schema.Class<SubagentParentBudgetView>(
  "@effect-agent/capabilities/SubagentParentBudgetView",
)({
  parentRunId: RunId,
  caps: SubagentDelegationCaps,
  totalChildInvocations: Natural,
  available: SubagentAvailableAmounts,
  cumulativeOverrun: SubagentReservationAmounts,
  reservations: Schema.Array(SubagentReservationView),
}) {}

/** All-or-nothing admission failed: one dimension cannot cover the requested allocation. */
export class SubagentBudgetExhausted extends Schema.TaggedError<SubagentBudgetExhausted>()(
  "SubagentBudgetExhausted",
  {
    parentRunId: RunId,
    dimension: SubagentBudgetDimension,
    limitValue: Natural,
    observedValue: Natural,
  },
) {}

/** The stable key is already reserved with a different allocation; neither request may win silently. */
export class SubagentReservationConflict extends Schema.TaggedError<SubagentReservationConflict>()(
  "SubagentReservationConflict",
  {
    reservationId: BudgetReservationId,
    existingAllocation: SubagentReservationAmounts,
    requestedAllocation: SubagentReservationAmounts,
  },
) {}

/** No reservation exists for this identity. */
export class SubagentReservationUnknown extends Schema.TaggedError<SubagentReservationUnknown>()(
  "SubagentReservationUnknown",
  {
    reservationId: BudgetReservationId,
  },
) {}

/** No delegation budget was registered for this parent Run. */
export class SubagentParentBudgetUnknown extends Schema.TaggedError<SubagentParentBudgetUnknown>()(
  "SubagentParentBudgetUnknown",
  {
    parentRunId: RunId,
  },
) {}

/** The parent Run is already registered with different caps; neither set may win silently. */
export class SubagentParentBudgetConflict extends Schema.TaggedError<SubagentParentBudgetConflict>()(
  "SubagentParentBudgetConflict",
  {
    parentRunId: RunId,
  },
) {}

/** A parent budget cannot retire while one of its child reservations is unsettled. */
export class SubagentParentBudgetActive extends Schema.TaggedError<SubagentParentBudgetActive>()(
  "SubagentParentBudgetActive",
  {
    parentRunId: RunId,
    openReservations: Natural,
  },
) {}

/**
 * Parent-owned hierarchical Subagent budget reservation authority.
 *
 * Every transition is a single atomic `Ref` update, so parallel children can
 * never observe and spend the same remainder. The post-response usage hook in
 * `budget.ts` is deliberately not reused here: it accounts after the fact and
 * is not a reservation service.
 */
export class SubagentReservations extends Context.Service<
  SubagentReservations,
  {
    /**
     * Register the finite delegable budget of one parent Run. Re-registering
     * identical caps returns the existing registration; different caps fail
     * rather than silently winning.
     */
    readonly registerParent: (
      parentRunId: RunId,
      caps: SubagentDelegationCaps,
    ) => Effect.Effect<SubagentParentBudgetView, SubagentParentBudgetConflict>;
    /**
     * Atomically reserve an all-or-nothing child allocation from the parent's
     * remaining delegable budget, checking every dimension before committing
     * any. The same key with the same allocation is idempotent and never
     * double-reserves; a distinct key charges the monotonic total-invocation
     * counter, which release never refunds.
     */
    readonly reserve: (
      request: SubagentReservationRequest,
    ) => Effect.Effect<
      SubagentReservationView,
      SubagentBudgetExhausted | SubagentReservationConflict | SubagentParentBudgetUnknown
    >;
    /**
     * Record observed child usage against a reservation. Usage above the
     * reserved amount is recorded as overrun and charged to the parent
     * aggregate, never clipped. Usage observed after settlement begins is
     * pure overrun and cannot create budget.
     */
    readonly observe: (
      reservationId: BudgetReservationId,
      usage: SubagentObservedUsage,
    ) => Effect.Effect<SubagentReservationView, SubagentReservationUnknown>;
    /**
     * `reserved → releasePending`: freeze the settlement decision. A dimension
     * with no observed usage conservatively consumes its reserved amount.
     * Idempotent once settlement has begun.
     */
    readonly beginRelease: (
      reservationId: BudgetReservationId,
    ) => Effect.Effect<SubagentReservationView, SubagentReservationUnknown>;
    /**
     * `releasePending → released`: return the unused allocation to the parent
     * exactly once. Called on a still-reserved reservation it settles through
     * `releasePending` in the same atomic update. Idempotent once released.
     */
    readonly release: (
      reservationId: BudgetReservationId,
    ) => Effect.Effect<SubagentReservationView, SubagentReservationUnknown>;
    /**
     * Acquire one bounded child execution slot as a Scope-owned resource, so
     * an interrupted child always frees its slot. The Semaphore bounds only
     * concurrent children; total invocations are enforced separately by
     * `reserve`'s monotonic counter. A configured cap of zero fails closed
     * instead of queueing forever; an unconfigured cap leaves execution
     * ungated by this service.
     */
    readonly acquireChildSlot: (
      parentRunId: RunId,
    ) => Effect.Effect<void, SubagentParentBudgetUnknown | SubagentBudgetExhausted, Scope.Scope>;
    /**
     * Reclaim one terminal parent registration and all of its released child
     * reservations. Retirement is idempotent; an unknown parent is already
     * retired. Open reservations fail typed and leave the ledger unchanged.
     * The Run owner must call this after its final accounting snapshot no
     * longer needs to be observable.
     */
    readonly retireParent: (parentRunId: RunId) => Effect.Effect<void, SubagentParentBudgetActive>;
    /** Read the aggregate parent view for auditing and conservation checks. */
    readonly parentSnapshot: (
      parentRunId: RunId,
    ) => Effect.Effect<SubagentParentBudgetView, SubagentParentBudgetUnknown>;
  }
>()("@effect-agent/capabilities/SubagentReservations") {}

type ReservableDimensionKey = keyof typeof AmountFields;

type Amounts = Readonly<Record<ReservableDimensionKey, number>>;
type PartialAmounts = Readonly<Partial<Record<ReservableDimensionKey, number>>>;

interface DimensionSpec {
  readonly key: ReservableDimensionKey;
  readonly dimension: SubagentBudgetDimension;
  readonly cap: (caps: SubagentDelegationCaps) => number | undefined;
}

const dimensionSpecs: ReadonlyArray<DimensionSpec> = [
  { key: "turns", dimension: "turns", cap: (caps) => caps.maxTurns },
  { key: "toolCalls", dimension: "tool-calls", cap: (caps) => caps.maxToolCalls },
  { key: "durationMillis", dimension: "duration", cap: (caps) => caps.maxDurationMillis },
  { key: "inputTokens", dimension: "input-tokens", cap: (caps) => caps.maxInputTokens },
  { key: "outputTokens", dimension: "output-tokens", cap: (caps) => caps.maxOutputTokens },
  { key: "costMicrousd", dimension: "cost", cap: (caps) => caps.maxCostMicrousd },
  { key: "resultBytes", dimension: "result-bytes", cap: (caps) => caps.maxResultBytes },
];

const zeroAmounts: Amounts = {
  turns: 0,
  toolCalls: 0,
  durationMillis: 0,
  inputTokens: 0,
  outputTokens: 0,
  costMicrousd: 0,
  resultBytes: 0,
};

interface ReservationState {
  readonly parentRunId: RunId;
  readonly parentToolCallId: ToolCallId;
  readonly status: SubagentReservationStatus;
  readonly allocated: Amounts;
  readonly observed: PartialAmounts;
  readonly covered: Amounts;
  readonly overrun: Amounts;
  /** Frozen when settlement begins; zero before. */
  readonly releasable: Amounts;
  /** Applied exactly once at `released`; zero before. */
  readonly released: Amounts;
}

interface ParentBudgetState {
  readonly parentRunId: RunId;
  readonly caps: SubagentDelegationCaps;
  readonly gate: Semaphore.Semaphore | undefined;
  /** Monotonic count of distinct reservations; never refunded by release. */
  readonly totalChildInvocations: number;
  /** Remaining delegable allocation for configured dimensions only. */
  readonly available: PartialAmounts;
  readonly cumulativeOverrun: Amounts;
}

interface ReservationLedger {
  readonly parents: ReadonlyMap<RunId, ParentBudgetState>;
  readonly reservations: ReadonlyMap<BudgetReservationId, ReservationState>;
}

type TransitionResult<A, E> =
  | { readonly _tag: "ok"; readonly value: A }
  | { readonly _tag: "error"; readonly error: E }
  | { readonly _tag: "corrupt"; readonly message: string };

const ok = <A>(value: A): TransitionResult<A, never> => ({ _tag: "ok", value });
const fail = <E>(error: E): TransitionResult<never, E> => ({ _tag: "error", error });

const resolve = <A, E>(result: TransitionResult<A, E>): Effect.Effect<A, E> => {
  switch (result._tag) {
    case "ok":
      return Effect.succeed(result.value);
    case "error":
      return Effect.fail(result.error);
    case "corrupt":
      return Effect.die(new Error(result.message));
  }
};

const sameCaps = (a: SubagentDelegationCaps, b: SubagentDelegationCaps): boolean =>
  a.maxTotalChildInvocations === b.maxTotalChildInvocations &&
  a.maxConcurrentChildren === b.maxConcurrentChildren &&
  a.maxTurns === b.maxTurns &&
  a.maxToolCalls === b.maxToolCalls &&
  a.maxDurationMillis === b.maxDurationMillis &&
  a.maxInputTokens === b.maxInputTokens &&
  a.maxOutputTokens === b.maxOutputTokens &&
  a.maxCostMicrousd === b.maxCostMicrousd &&
  a.maxResultBytes === b.maxResultBytes;

const sameAmounts = (a: Amounts, b: Amounts): boolean =>
  dimensionSpecs.every((spec) => a[spec.key] === b[spec.key]);

const optionalAmounts = (partial: PartialAmounts): { [K in ReservableDimensionKey]?: number } => {
  const out: { [K in ReservableDimensionKey]?: number } = {};

  for (const spec of dimensionSpecs) {
    const value = partial[spec.key];

    if (value !== undefined) {
      out[spec.key] = value;
    }
  }

  return out;
};

const reservationView = (
  reservationId: BudgetReservationId,
  state: ReservationState,
): SubagentReservationView =>
  SubagentReservationView.make({
    reservationId,
    parentRunId: state.parentRunId,
    parentToolCallId: state.parentToolCallId,
    status: state.status,
    allocated: SubagentReservationAmounts.make(state.allocated),
    observedConsumed: SubagentObservedUsage.make(optionalAmounts(state.observed)),
    coveredConsumed: SubagentReservationAmounts.make(state.covered),
    released: SubagentReservationAmounts.make(state.released),
    overrun: SubagentReservationAmounts.make(state.overrun),
  });

const parentView = (
  ledger: ReservationLedger,
  parent: ParentBudgetState,
): SubagentParentBudgetView => {
  const reservations: Array<SubagentReservationView> = [];

  for (const [reservationId, reservation] of ledger.reservations) {
    if (reservation.parentRunId === parent.parentRunId) {
      reservations.push(reservationView(reservationId, reservation));
    }
  }

  return SubagentParentBudgetView.make({
    parentRunId: parent.parentRunId,
    caps: parent.caps,
    totalChildInvocations: parent.totalChildInvocations,
    available: SubagentAvailableAmounts.make(optionalAmounts(parent.available)),
    cumulativeOverrun: SubagentReservationAmounts.make(parent.cumulativeOverrun),
    reservations,
  });
};

const corruptParent = (parentRunId: RunId): TransitionResult<never, never> => ({
  _tag: "corrupt",
  message: `SubagentReservations ledger invariant violated: reservation references unregistered parent Run ${parentRunId}`,
});

const registerTransition = (
  ledger: ReservationLedger,
  parentRunId: RunId,
  caps: SubagentDelegationCaps,
  gate: Semaphore.Semaphore | undefined,
): readonly [
  TransitionResult<SubagentParentBudgetView, SubagentParentBudgetConflict>,
  ReservationLedger,
] => {
  const existing = ledger.parents.get(parentRunId);

  if (existing !== undefined) {
    return sameCaps(existing.caps, caps)
      ? [ok(parentView(ledger, existing)), ledger]
      : [fail(SubagentParentBudgetConflict.make({ parentRunId })), ledger];
  }
  const available: { [K in ReservableDimensionKey]?: number } = {};

  for (const spec of dimensionSpecs) {
    const cap = spec.cap(caps);

    if (cap !== undefined) {
      available[spec.key] = cap;
    }
  }

  const parent: ParentBudgetState = {
    parentRunId,
    caps,
    gate,
    totalChildInvocations: 0,
    available,
    cumulativeOverrun: zeroAmounts,
  };

  const next: ReservationLedger = {
    parents: new Map(ledger.parents).set(parentRunId, parent),
    reservations: ledger.reservations,
  };

  return [ok(parentView(next, parent)), next];
};

const reserveTransition = (
  ledger: ReservationLedger,
  request: SubagentReservationRequest,
): readonly [
  TransitionResult<
    SubagentReservationView,
    SubagentBudgetExhausted | SubagentReservationConflict | SubagentParentBudgetUnknown
  >,
  ReservationLedger,
] => {
  const parent = ledger.parents.get(request.parentRunId);

  if (parent === undefined) {
    return [fail(SubagentParentBudgetUnknown.make({ parentRunId: request.parentRunId })), ledger];
  }
  const reservationId = makeBudgetReservationId(request.parentRunId, request.parentToolCallId);
  const existing = ledger.reservations.get(reservationId);

  if (existing !== undefined) {
    return sameAmounts(existing.allocated, request.allocation)
      ? [ok(reservationView(reservationId, existing)), ledger]
      : [
          fail(
            SubagentReservationConflict.make({
              reservationId,
              existingAllocation: SubagentReservationAmounts.make(existing.allocated),
              requestedAllocation: request.allocation,
            }),
          ),
          ledger,
        ];
  }
  const maxInvocations = parent.caps.maxTotalChildInvocations;

  if (maxInvocations !== undefined && parent.totalChildInvocations + 1 > maxInvocations) {
    return [
      fail(
        SubagentBudgetExhausted.make({
          parentRunId: request.parentRunId,
          dimension: "total-child-invocations",
          limitValue: maxInvocations,
          observedValue: parent.totalChildInvocations + 1,
        }),
      ),
      ledger,
    ];
  }
  for (const spec of dimensionSpecs) {
    const cap = spec.cap(parent.caps);

    if (cap === undefined) {
      continue;
    }
    // Overrun already charged to the parent aggregate reduces delegable
    // headroom without mutating `available`.
    const headroom = (parent.available[spec.key] ?? 0) - parent.cumulativeOverrun[spec.key];
    const requested = request.allocation[spec.key];

    if (requested > headroom) {
      return [
        fail(
          SubagentBudgetExhausted.make({
            parentRunId: request.parentRunId,
            dimension: spec.dimension,
            limitValue: cap,
            observedValue: cap - headroom + requested,
          }),
        ),
        ledger,
      ];
    }
  }

  const nextAvailable: { [K in ReservableDimensionKey]?: number } = {
    ...parent.available,
  };

  for (const spec of dimensionSpecs) {
    const available = parent.available[spec.key];

    if (available !== undefined) {
      nextAvailable[spec.key] = available - request.allocation[spec.key];
    }
  }

  const reservation: ReservationState = {
    parentRunId: request.parentRunId,
    parentToolCallId: request.parentToolCallId,
    status: "reserved",
    allocated: request.allocation,
    observed: {},
    covered: zeroAmounts,
    overrun: zeroAmounts,
    releasable: zeroAmounts,
    released: zeroAmounts,
  };

  const next: ReservationLedger = {
    parents: new Map(ledger.parents).set(request.parentRunId, {
      ...parent,
      totalChildInvocations: parent.totalChildInvocations + 1,
      available: nextAvailable,
    }),
    reservations: new Map(ledger.reservations).set(reservationId, reservation),
  };

  return [ok(reservationView(reservationId, reservation)), next];
};

const observeTransition = (
  ledger: ReservationLedger,
  reservationId: BudgetReservationId,
  usage: SubagentObservedUsage,
): readonly [
  TransitionResult<SubagentReservationView, SubagentReservationUnknown>,
  ReservationLedger,
] => {
  const reservation = ledger.reservations.get(reservationId);

  if (reservation === undefined) {
    return [fail(SubagentReservationUnknown.make({ reservationId })), ledger];
  }
  const parent = ledger.parents.get(reservation.parentRunId);

  if (parent === undefined) {
    return [corruptParent(reservation.parentRunId), ledger];
  }
  const observed: { [K in ReservableDimensionKey]?: number } = { ...reservation.observed };
  const covered: Record<ReservableDimensionKey, number> = { ...reservation.covered };
  const overrun: Record<ReservableDimensionKey, number> = { ...reservation.overrun };

  const cumulativeOverrun: Record<ReservableDimensionKey, number> = {
    ...parent.cumulativeOverrun,
  };

  for (const spec of dimensionSpecs) {
    const delta = usage[spec.key];

    if (delta === undefined) {
      continue;
    }
    observed[spec.key] = (observed[spec.key] ?? 0) + delta;

    // Once settlement began the accounting decision is frozen, so late or
    // corrected usage is pure overrun and cannot silently create budget.
    const coverable =
      reservation.status === "reserved"
        ? Math.min(delta, reservation.allocated[spec.key] - covered[spec.key])
        : 0;

    covered[spec.key] += coverable;
    const overrunDelta = delta - coverable;

    overrun[spec.key] += overrunDelta;
    cumulativeOverrun[spec.key] += overrunDelta;
  }
  const nextReservation: ReservationState = { ...reservation, observed, covered, overrun };

  const next: ReservationLedger = {
    parents: new Map(ledger.parents).set(parent.parentRunId, { ...parent, cumulativeOverrun }),
    reservations: new Map(ledger.reservations).set(reservationId, nextReservation),
  };

  return [ok(reservationView(reservationId, nextReservation)), next];
};

/**
 * Freeze the settlement decision. A dimension that was never observed
 * conservatively consumes its full reserved amount.
 */
const freezeSettlement = (reservation: ReservationState): ReservationState => {
  const observed: { [K in ReservableDimensionKey]?: number } = { ...reservation.observed };
  const covered: Record<ReservableDimensionKey, number> = { ...reservation.covered };
  const releasable: Record<ReservableDimensionKey, number> = { ...zeroAmounts };

  for (const spec of dimensionSpecs) {
    if (observed[spec.key] === undefined) {
      observed[spec.key] = reservation.allocated[spec.key];
      covered[spec.key] = reservation.allocated[spec.key];
    }
    releasable[spec.key] = reservation.allocated[spec.key] - covered[spec.key];
  }

  return { ...reservation, status: "releasePending", observed, covered, releasable };
};

const beginReleaseTransition = (
  ledger: ReservationLedger,
  reservationId: BudgetReservationId,
): readonly [
  TransitionResult<SubagentReservationView, SubagentReservationUnknown>,
  ReservationLedger,
] => {
  const reservation = ledger.reservations.get(reservationId);

  if (reservation === undefined) {
    return [fail(SubagentReservationUnknown.make({ reservationId })), ledger];
  }
  if (reservation.status !== "reserved") {
    return [ok(reservationView(reservationId, reservation)), ledger];
  }
  const frozen = freezeSettlement(reservation);

  const next: ReservationLedger = {
    parents: ledger.parents,
    reservations: new Map(ledger.reservations).set(reservationId, frozen),
  };

  return [ok(reservationView(reservationId, frozen)), next];
};

const releaseTransition = (
  ledger: ReservationLedger,
  reservationId: BudgetReservationId,
): readonly [
  TransitionResult<SubagentReservationView, SubagentReservationUnknown>,
  ReservationLedger,
] => {
  const reservation = ledger.reservations.get(reservationId);

  if (reservation === undefined) {
    return [fail(SubagentReservationUnknown.make({ reservationId })), ledger];
  }
  if (reservation.status === "released") {
    return [ok(reservationView(reservationId, reservation)), ledger];
  }
  const parent = ledger.parents.get(reservation.parentRunId);

  if (parent === undefined) {
    return [corruptParent(reservation.parentRunId), ledger];
  }
  const frozen = reservation.status === "reserved" ? freezeSettlement(reservation) : reservation;
  const nextAvailable: { [K in ReservableDimensionKey]?: number } = { ...parent.available };

  for (const spec of dimensionSpecs) {
    const available = parent.available[spec.key];

    if (available !== undefined) {
      nextAvailable[spec.key] = available + frozen.releasable[spec.key];
    }
  }
  const settled: ReservationState = { ...frozen, status: "released", released: frozen.releasable };

  const next: ReservationLedger = {
    parents: new Map(ledger.parents).set(parent.parentRunId, {
      ...parent,
      available: nextAvailable,
    }),
    reservations: new Map(ledger.reservations).set(reservationId, settled),
  };

  return [ok(reservationView(reservationId, settled)), next];
};

const retireParentTransition = (
  ledger: ReservationLedger,
  parentRunId: RunId,
): readonly [TransitionResult<void, SubagentParentBudgetActive>, ReservationLedger] => {
  if (!ledger.parents.has(parentRunId)) {
    return [ok(undefined), ledger];
  }
  let openReservations = 0;

  for (const reservation of ledger.reservations.values()) {
    if (reservation.parentRunId === parentRunId && reservation.status !== "released") {
      openReservations += 1;
    }
  }
  if (openReservations > 0) {
    return [fail(SubagentParentBudgetActive.make({ parentRunId, openReservations })), ledger];
  }
  const parents = new Map(ledger.parents);

  parents.delete(parentRunId);
  const reservations = new Map(ledger.reservations);

  for (const [reservationId, reservation] of reservations) {
    if (reservation.parentRunId === parentRunId) {
      reservations.delete(reservationId);
    }
  }

  return [ok(undefined), { parents, reservations }];
};

/**
 * In-memory reservation ledger. All state lives in one `Ref` owned by the
 * Layer's Scope; every transition is a single atomic `Ref.modify`.
 */
export const SubagentReservationsMemoryLive: Layer.Layer<SubagentReservations> = Layer.effect(
  SubagentReservations,
  Effect.gen(function* () {
    const state = yield* Ref.make<ReservationLedger>({
      parents: new Map(),
      reservations: new Map(),
    });

    return SubagentReservations.of({
      registerParent: Effect.fn("SubagentReservations.registerParent")(
        function* (parentRunId, caps) {
          const gate =
            caps.maxConcurrentChildren !== undefined && caps.maxConcurrentChildren > 0
              ? yield* Semaphore.make(caps.maxConcurrentChildren)
              : undefined;

          const result = yield* Ref.modify(state, (ledger) =>
            registerTransition(ledger, parentRunId, caps, gate),
          );

          return yield* resolve(result);
        },
      ),
      reserve: Effect.fn("SubagentReservations.reserve")(function* (request) {
        const result = yield* Ref.modify(state, (ledger) => reserveTransition(ledger, request));

        return yield* resolve(result);
      }),
      observe: Effect.fn("SubagentReservations.observe")(function* (reservationId, usage) {
        const result = yield* Ref.modify(state, (ledger) =>
          observeTransition(ledger, reservationId, usage),
        );

        return yield* resolve(result);
      }),
      beginRelease: Effect.fn("SubagentReservations.beginRelease")(function* (reservationId) {
        const result = yield* Ref.modify(state, (ledger) =>
          beginReleaseTransition(ledger, reservationId),
        );

        return yield* resolve(result);
      }),
      release: Effect.fn("SubagentReservations.release")(function* (reservationId) {
        const result = yield* Ref.modify(state, (ledger) =>
          releaseTransition(ledger, reservationId),
        );

        return yield* resolve(result);
      }),
      acquireChildSlot: Effect.fn("SubagentReservations.acquireChildSlot")(function* (parentRunId) {
        const ledger = yield* Ref.get(state);
        const parent = ledger.parents.get(parentRunId);

        if (parent === undefined) {
          return yield* SubagentParentBudgetUnknown.make({ parentRunId });
        }
        const cap = parent.caps.maxConcurrentChildren;

        if (cap === undefined) {
          return;
        }
        if (cap === 0 || parent.gate === undefined) {
          return yield* SubagentBudgetExhausted.make({
            parentRunId,
            dimension: "concurrent-children",
            limitValue: cap,
            observedValue: 1,
          });
        }
        const gate = parent.gate;

        yield* Effect.acquireRelease(
          gate.take(1),
          (permits) => gate.release(permits).pipe(Effect.asVoid),
          { interruptible: true },
        );
      }),
      retireParent: Effect.fn("SubagentReservations.retireParent")(function* (parentRunId) {
        const result = yield* Ref.modify(state, (ledger) =>
          retireParentTransition(ledger, parentRunId),
        );

        return yield* resolve(result);
      }),
      parentSnapshot: Effect.fn("SubagentReservations.parentSnapshot")(function* (parentRunId) {
        const ledger = yield* Ref.get(state);
        const parent = ledger.parents.get(parentRunId);

        if (parent === undefined) {
          return yield* SubagentParentBudgetUnknown.make({ parentRunId });
        }

        return parentView(ledger, parent);
      }),
    });
  }),
);
