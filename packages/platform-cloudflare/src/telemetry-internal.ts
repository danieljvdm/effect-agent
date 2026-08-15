import { Cause, Duration, Effect, Exit } from "effect";

import { CloudflareRuntimeTelemetry, type CloudflareTelemetryExportError } from "./telemetry.ts";

/** Bounded typed control signal used only while a native entrypoint span is active. */
class CloudflareNativeSpanFailure extends Error {
  override readonly name = "ConversationObjectDeliveryFailed";

  constructor() {
    super("Cloudflare Conversation Object delivery failed");
  }
}

const stripCloudflareNativeSpanFailures = <E>(
  cause: Cause.Cause<E | CloudflareNativeSpanFailure>,
): { readonly found: boolean; readonly residual: Cause.Cause<E> } => {
  const found = cause.reasons.some(
    (reason) => Cause.isFailReason(reason) && reason.error instanceof CloudflareNativeSpanFailure,
  );
  if (!found) {
    // Effect v4 Cause is flat. Preserve the exact marker-free Cause object and narrow only its
    // covariant error parameter after the complete Reason scan.
    return { found: false, residual: cause as Cause.Cause<E> };
  }
  const reasons = cause.reasons.filter((reason): reason is Cause.Reason<E> => {
    if (Cause.isFailReason(reason) && reason.error instanceof CloudflareNativeSpanFailure) {
      return false;
    }
    return true;
  });
  return { found: true, residual: Cause.fromReasons(reasons) };
};

/**
 * @internal Replace an entrypoint Cause with a bounded typed marker inside a span, then remove
 * only that marker and restore the exact invocation-local Cause plus any newly composed reasons.
 */
export const withCloudflareNativeSpanFailure = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  trace: (
    masked: Effect.Effect<A, E | CloudflareNativeSpanFailure, R>,
  ) => Effect.Effect<A, E | CloudflareNativeSpanFailure, R>,
): Effect.Effect<A, E, R> =>
  Effect.suspend(() => {
    let original: Cause.Cause<E> | undefined;
    return trace(
      effect.pipe(
        Effect.catchCause((cause) => {
          original = cause;
          return Effect.fail(new CloudflareNativeSpanFailure());
        }),
      ),
    ).pipe(
      Effect.catchCause((cause) => {
        const { found, residual } = stripCloudflareNativeSpanFailures(cause);
        if (!found) return Effect.failCause(residual);
        const restored = original === undefined ? residual : Cause.combine(original, residual);
        return Effect.failCause(restored);
      }),
    );
  });

const logCloudflareTelemetryCause = (
  cause: Cause.Cause<CloudflareTelemetryExportError | Cause.TimeoutError>,
): Effect.Effect<void> => {
  type FailureKind = "exporter" | "timeout" | "defect" | "interrupted";
  const kinds = new Set<FailureKind>();
  for (const reason of cause.reasons) {
    if (Cause.isFailReason(reason)) {
      kinds.add(Cause.isTimeoutError(reason.error) ? "timeout" : "exporter");
    } else if (Cause.isDieReason(reason)) {
      kinds.add("defect");
    } else {
      kinds.add("interrupted");
    }
  }
  return Effect.forEach(
    kinds,
    (kind) => {
      const diagnostic =
        kind === "defect"
          ? Effect.logError("Cloudflare telemetry flush defect")
          : Effect.logWarning(
              kind === "exporter"
                ? "Cloudflare telemetry exporter failed"
                : kind === "timeout"
                  ? "Cloudflare telemetry flush exceeded its time budget"
                  : "Cloudflare telemetry flush interrupted",
            );
      // The typed error retains its foreign Cause for explicit host-controlled inspection. Never
      // pass untrusted exporter, defect, interrupt, or platform values to the configured Logger.
      return diagnostic.pipe(
        Effect.annotateLogs({
          "effect_agent.cloudflare.telemetry.failure_kind": kind,
        }),
      );
    },
    { discard: true },
  );
};

/**
 * @internal Cooperative background flush budget. Interruptible exporters stop at the configured
 * timeout; uninterruptible exporters remain bounded by Cloudflare's waitUntil lifecycle instead
 * of holding the native delivery open.
 */
export const flushCloudflareRuntimeTelemetry = (
  timeoutMillis: number,
): Effect.Effect<
  void,
  CloudflareTelemetryExportError | Cause.TimeoutError,
  CloudflareRuntimeTelemetry
> =>
  Effect.flatMap(CloudflareRuntimeTelemetry, ({ flush }) => flush).pipe(
    Effect.interruptible,
    Effect.timeout(Duration.millis(timeoutMillis)),
    Effect.onExit((exit) =>
      Exit.isFailure(exit) ? logCloudflareTelemetryCause(exit.cause) : Effect.void,
    ),
    Effect.asVoid,
  );

/** @internal Convert the shared exporter result into the always-fulfilled waitUntil bridge. */
export const fulfillCloudflareTelemetryBackground = (result: Promise<unknown>): Promise<void> =>
  result.then(
    () => undefined,
    () => undefined,
  );

export interface CloudflareTelemetryFlushReservation {
  /** Shared, always-fulfilled background Promise registered by this batch's first owner only. */
  readonly background: Promise<void>;
  /** Shared raw cycle result used by deterministic coordinator tests and diagnostics. */
  readonly cycle: Promise<void>;
  /** True only for the first delivery reserved into this bounded batch. */
  readonly owner: boolean;
  /** True when this delivery was coalesced without retaining its settlement Promise. */
  readonly dropped: boolean;
  /** Cancel this batch when its synchronous waitUntil registration fails. */
  readonly cancel: () => void;
}

/** @internal One active exporter attempt plus bounded trailing/queued batches. */
export interface CloudflareTelemetryFlushCoordinator {
  readonly reserve: <A>(delivery: Promise<A>) => CloudflareTelemetryFlushReservation;
}

type FlushCyclePhase = "pending" | "first" | "trailing" | "settling";
type FlushBatchPhase = "pending" | "waiting" | "running" | "settled" | "cancelled";

/** @internal Hard cap on delivery Promises retained by one exporter batch. */
export const MAX_CLOUDFLARE_TELEMETRY_BATCH_DELIVERIES = 64;

interface FlushBatch {
  readonly result: Promise<void>;
  readonly background: Promise<void>;
  readonly resolveResult: () => void;
  readonly rejectResult: (cause: unknown) => void;
  readonly ready: Promise<void>;
  readonly resolveReady: () => void;
  phase: FlushBatchPhase;
  reservations: number;
  unsettledDeliveries: number;
  dropDiagnosed: boolean;
}

interface FlushCycle {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
  readonly reject: (cause: unknown) => void;
  readonly first: FlushBatch;
  phase: FlushCyclePhase;
  trailing: FlushBatch | undefined;
}

const makeFlushBatch = (): FlushBatch => {
  let resolveResult = (): void => undefined;
  let rejectResult = (_cause: unknown): void => undefined;
  const result = new Promise<void>((resolve, reject) => {
    resolveResult = resolve;
    rejectResult = reject;
  });
  let resolveReady = (): void => undefined;
  const ready = new Promise<void>((resolve) => {
    resolveReady = resolve;
  });
  return {
    result,
    background: fulfillCloudflareTelemetryBackground(result),
    resolveResult,
    rejectResult,
    ready,
    resolveReady,
    phase: "pending",
    reservations: 0,
    unsettledDeliveries: 0,
    dropDiagnosed: false,
  };
};

const makeFlushCycle = (): FlushCycle => {
  let resolveCycle = (): void => undefined;
  let rejectCycle = (_cause: unknown): void => undefined;
  const promise = new Promise<void>((resolve, reject) => {
    resolveCycle = resolve;
    rejectCycle = reject;
  });
  // Runtime delivery does not consume the raw cycle result. Attach a bounded rejection observer
  // immediately while retaining the original Promise for deterministic tests/host diagnostics.
  void fulfillCloudflareTelemetryBackground(promise);
  return {
    promise,
    resolve: resolveCycle,
    reject: rejectCycle,
    first: makeFlushBatch(),
    phase: "pending",
    trailing: undefined,
  };
};

/**
 * @internal Coordinate exporter attempts for one Conversation Object incarnation. Each cycle is
 * capped at a first attempt plus one trailing attempt. Each batch is reserved synchronously and
 * waits for every delivery assigned to it before exporting. Only the first owner of the pending,
 * trailing, or queued batch registers that batch's shared background Promise, so a stalled
 * exporter cannot accumulate per-delivery waitUntil registrations. Each batch retains at most
 * `MAX_CLOUDFLARE_TELEMETRY_BATCH_DELIVERIES` settlement Promises; excess deliveries are lossy-
 * coalesced into the already-requested export without settlement ownership and diagnosed once for
 * that batch. Failures remain rejected by the raw cycle after every requested attempt runs; each
 * shared waitUntil bridge always fulfills.
 */
export const makeCloudflareTelemetryFlushCoordinator = (
  flush: () => Promise<void>,
  options: {
    /** @internal Synchronous test hook for the exact final-settlement boundary. */
    readonly onSettling?: () => void;
    /** Bounded synchronous diagnostic invoked once for each batch that reaches its hard cap. */
    readonly onReservationDropped?: () => void;
  } = {},
): CloudflareTelemetryFlushCoordinator => {
  let active: FlushCycle | undefined;
  let queued: FlushCycle | undefined;

  const closeBatch = (batch: FlushBatch): void => {
    if (batch.phase !== "pending") return;
    batch.phase = "waiting";
    if (batch.unsettledDeliveries === 0) batch.resolveReady();
  };

  const cancelBatch = (batch: FlushBatch): void => {
    if (batch.phase === "settled" || batch.phase === "cancelled") return;
    batch.phase = "cancelled";
    batch.resolveReady();
    batch.resolveResult();
  };

  const reserveDelivery = <A>(
    batch: FlushBatch,
    delivery: Promise<A>,
  ): { readonly owner: boolean; readonly dropped: boolean } => {
    if (batch.reservations >= MAX_CLOUDFLARE_TELEMETRY_BATCH_DELIVERIES) {
      if (!batch.dropDiagnosed) {
        batch.dropDiagnosed = true;
        try {
          options.onReservationDropped?.();
        } catch {
          // A derivative diagnostic sink cannot change native delivery or exporter coordination.
        }
      }
      return { owner: false, dropped: true };
    }
    const owner = batch.reservations === 0;
    batch.reservations += 1;
    batch.unsettledDeliveries += 1;
    const settled = (): void => {
      batch.unsettledDeliveries -= 1;
      if (batch.phase === "waiting" && batch.unsettledDeliveries === 0) {
        batch.resolveReady();
      }
    };
    void delivery.then(settled, settled);
    return { owner, dropped: false };
  };

  const runCycle = async (cycle: FlushCycle): Promise<void> => {
    const failures: Array<unknown> = [];

    const runAttempt = async (): Promise<void> => {
      try {
        await flush();
      } catch (cause) {
        // Each cycle is capped at two attempts, so retaining every cause is bounded.
        failures.push(cause);
      }
    };

    const runBatch = async (batch: FlushBatch): Promise<void> => {
      closeBatch(batch);
      await batch.ready;
      if (batch.phase === "cancelled") return;
      batch.phase = "running";
      const failureStart = failures.length;
      await runAttempt();
      batch.phase = "settled";
      if (failures.length === failureStart) {
        batch.resolveResult();
      } else {
        batch.rejectResult(failures[failures.length - 1]);
      }
    };

    try {
      // Requests reserved before this first exporter microtask starts share the first batch.
      cycle.phase = "first";
      await runBatch(cycle.first);
      const trailing = cycle.trailing;
      if (trailing !== undefined) {
        cycle.phase = "trailing";
        await runBatch(trailing);
      }
      // Once no more attempts can join this capped cycle, later requests must route to the queued
      // cycle. The synchronous hook lets tests re-enter `reserve` at this exact boundary without
      // relying on Promise reaction ordering.
      cycle.phase = "settling";
      options.onSettling?.();
    } finally {
      // Advance in the same continuation that completes the capped cycle. A request queued on the
      // just-completed exporter Promise therefore either joined the next cycle while this one was
      // trailing or sees the promoted pending cycle; it can never attach to a settling old Promise.
      const next = queued;
      queued = undefined;
      active = next;
      if (next !== undefined) startCycle(next);
    }

    if (failures.length === 0) return;
    if (failures.length === 1) throw failures[0];
    throw new AggregateError(
      failures,
      `${failures.length} Cloudflare telemetry flush attempts failed`,
    );
  };

  const startCycle = (cycle: FlushCycle): void => {
    // Start in a microtask after active is assigned, including when flush throws synchronously.
    void Promise.resolve()
      .then(() => runCycle(cycle))
      .then(cycle.resolve, cycle.reject);
  };

  const reserve = <A>(delivery: Promise<A>): CloudflareTelemetryFlushReservation => {
    let cycle: FlushCycle;
    let batch: FlushBatch;
    let shouldStart = false;

    if (active === undefined) {
      cycle = makeFlushCycle();
      active = cycle;
      batch = cycle.first;
      shouldStart = true;
    } else if (active.phase === "pending" && active.first.phase !== "cancelled") {
      cycle = active;
      batch = active.first;
    } else if (active.phase === "first") {
      cycle = active;
      batch = active.trailing ??= makeFlushBatch();
    } else {
      // The current cycle has spent its trailing attempt, entered settlement, or had its pending
      // registration cancelled. Route later traffic to one pending next cycle.
      if (queued === undefined) queued = makeFlushCycle();
      cycle = queued;
      batch = queued.first;
    }

    const { dropped, owner } = reserveDelivery(batch, delivery);
    if (shouldStart) startCycle(cycle);

    return {
      background: batch.background,
      cycle: cycle.promise,
      owner,
      dropped,
      cancel: () => {
        if (!owner) return;
        cancelBatch(batch);
        if (cycle.trailing === batch && cycle.phase === "first") {
          cycle.trailing = undefined;
        } else if (queued === cycle && active !== cycle) {
          queued = undefined;
          cycle.resolve();
        }
      },
    };
  };

  return { reserve };
};

/**
 * @internal Reserve telemetry before registration without allowing a synchronous platform failure
 * to replace the already-running native delivery Promise. Only a batch owner calls waitUntil; its
 * shared bridge is already always fulfilled, and registration failure cancels export for that
 * unowned batch.
 */
export const registerCloudflareTelemetryAfterNativeSettlement = <A>(
  waitUntil: (promise: Promise<void>) => void,
  delivery: Promise<A>,
  reserve: (delivery: Promise<A>) => CloudflareTelemetryFlushReservation,
  diagnoseRegistrationFailure: () => void,
): Promise<A> => {
  const reservation = reserve(delivery);
  if (!reservation.owner) return delivery;
  try {
    waitUntil(reservation.background);
  } catch {
    reservation.cancel();
    try {
      diagnoseRegistrationFailure();
    } catch {
      // Even a broken diagnostic sink is derivative and cannot create an uncertain native outcome.
    }
  }
  return delivery;
};
