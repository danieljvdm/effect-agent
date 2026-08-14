import { Context, Effect, Layer, Ref, Schema } from "effect";

/**
 * Coordinator-owned failpoint locations (plan §Failpoints). Every location sits immediately
 * AFTER one durable mutation of the six-step coordinator flow, so a fault injected there
 * simulates a crash between two durable steps; the matching BEFORE boundary of each mutation is
 * owned by the storage adapters (`ledger:*:before`, `append:before`, ...), giving every durable
 * mutation a failpoint on both sides (change discipline).
 *
 * The `subagent:*` family covers every durable mutation of the S2 establishment/join protocol
 * (spec/subagents.md §12/§14): reservation, request append, child admission, child readiness,
 * start-link append, sibling per-call settles at the suspension seam, the `waitingForChild`
 * suspension, child abort propagation, the atomic join batch, and both reservation-release
 * transitions.
 */
export const DurableRuntimeFailpointLocation = Schema.Literals([
  "submit:after-admit",
  "submit:after-materialize",
  "claim:after-claim",
  "input:after-canonical-append",
  "turn:after-canonical-append",
  "turn:after-response-append",
  "turn:after-results-append",
  "tools:after-prepared-append",
  "step:after-step-append",
  "approval:after-request-append",
  "approval:after-suspend",
  "join:after-claim",
  "join:after-canonical-append",
  "resolve:after-intent",
  "terminalize:after-reserve",
  "terminalize:after-canonical-append",
  "abort:after-intent",
  "subagent:after-reserve",
  "subagent:after-request-append",
  "subagent:after-admit",
  "subagent:after-child-ready",
  "subagent:after-start-append",
  "subagent:after-sibling-settle",
  "subagent:after-suspend",
  "subagent:after-child-abort-intent",
  "subagent:after-join-append",
  "subagent:after-release-pending",
  "subagent:after-release",
]);
export type DurableRuntimeFailpointLocation = typeof DurableRuntimeFailpointLocation.Type;

/** Injected coordinator fault. Reaching the caller means the preceding durable step committed. */
export class DurableRuntimeFailpointError extends Schema.TaggedErrorClass<DurableRuntimeFailpointError>()(
  "DurableRuntimeFailpointError",
  {
    location: DurableRuntimeFailpointLocation,
  },
) {}

export type DurableRuntimeFailpointHandler = (
  location: DurableRuntimeFailpointLocation,
) => Effect.Effect<void, DurableRuntimeFailpointError>;

const noFailpoint: DurableRuntimeFailpointHandler = () => Effect.void;

/** Test-only control for replacing the active coordinator failpoint handler. */
export class DurableRuntimeFailpointTestControl extends Context.Service<
  DurableRuntimeFailpointTestControl,
  {
    readonly clear: Effect.Effect<void>;
    readonly setHandler: (handler: DurableRuntimeFailpointHandler) => Effect.Effect<void>;
  }
>()("@effect-agent/session/DurableRuntimeFailpointTestControl") {}

/**
 * Explicit fault-injection authority used at durable coordinator step boundaries. The process
 * crash harness (WP5) maps a hit to `process.exit`; in-process tests fail the step with the
 * typed `DurableRuntimeFailpointError` instead.
 */
export class DurableRuntimeFailpoint extends Context.Service<
  DurableRuntimeFailpoint,
  {
    readonly hit: DurableRuntimeFailpointHandler;
  }
>()("@effect-agent/session/DurableRuntimeFailpoint") {
  /** Production default: no fault injection. */
  static readonly layer = Layer.succeed(this)({ hit: noFailpoint });

  /** Reusable test Layer with a control service backed by the same handler Ref. */
  static readonly layerTest = Layer.effectContext(
    Effect.gen(function* () {
      const handler = yield* Ref.make<DurableRuntimeFailpointHandler>(noFailpoint);
      return Context.make(
        DurableRuntimeFailpoint,
        DurableRuntimeFailpoint.of({
          hit: (location) => Ref.get(handler).pipe(Effect.flatMap((current) => current(location))),
        }),
      ).pipe(
        Context.add(
          DurableRuntimeFailpointTestControl,
          DurableRuntimeFailpointTestControl.of({
            clear: Ref.set(handler, noFailpoint),
            setHandler: (next) => Ref.set(handler, next),
          }),
        ),
      );
    }),
  );
}
