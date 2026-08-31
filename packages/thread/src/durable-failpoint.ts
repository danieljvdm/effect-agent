import { Context, Effect, Layer, Schema } from "effect";

/**
 * Coordinator-owned failpoint locations (plan §Failpoints). Locations normally sit immediately
 * AFTER one durable mutation of the six-step coordinator flow, so a fault injected there
 * simulates a crash between two durable steps; the matching BEFORE boundary of each mutation is
 * owned by the storage adapters (`ledger:*:before`, `append:before`, ...), giving every durable
 * mutation a failpoint on both sides (change discipline). Run-start append also exposes its
 * BEFORE boundary here to distinguish a fresh clock from a committed clock in crash tests.
 *
 * The `subagent:*` family covers every durable mutation of the S2 establishment/join protocol
 * including reservation, request append, child admission, child readiness,
 * start-link append, sibling per-call settles at the suspension seam, the `waitingForChild`
 * suspension, child abort propagation, the atomic join batch, and both reservation-release
 * transitions.
 */
export const DurableRuntimeFailpointLocation = Schema.Literals([
  "submit:after-admit",
  "submit:after-materialize",
  "claim:after-claim",
  "input:after-canonical-append",
  "run:before-start-append",
  "run:after-start-append",
  "turn:after-canonical-append",
  "turn:after-response-append",
  "turn:after-results-append",
  "compaction:after-canonical-append",
  "tools:after-prepared-append",
  "tools:before-prepared-append",
  "policy:before-reservation-append",
  "policy:after-reservation-append",
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
export class DurableRuntimeFailpointError extends Schema.TaggedError<DurableRuntimeFailpointError>()(
  "DurableRuntimeFailpointError",
  {
    location: DurableRuntimeFailpointLocation,
  },
) {}

export type DurableRuntimeFailpointHandler = (
  location: DurableRuntimeFailpointLocation,
) => Effect.Effect<void, DurableRuntimeFailpointError>;

const noFailpoint: DurableRuntimeFailpointHandler = () => Effect.void;

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
>()("@effect-agent/thread/DurableRuntimeFailpoint") {
  /** Production default: no fault injection. */
  static readonly layer = Layer.succeed(this)({ hit: noFailpoint });
}
