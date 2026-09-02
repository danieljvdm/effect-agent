import type { Crypto } from "effect";
import { Cause, Effect, Exit, Option, Schema } from "effect";

import { threadStoreConformanceCases } from "./conformance.ts";
import { DurableRuntimeFailpointLocation } from "./durable-failpoint.ts";
import { inspectForeignDiagnostic, safeUnknownString } from "./foreign-diagnostic.ts";
import { submissionLedgerConformanceCases } from "./ledger-conformance.ts";
import type { SubmissionLedger } from "./ledger.ts";
import { LedgerCapabilities } from "./ledger.ts";
import type { ThreadStore } from "./store.ts";

// ---------------------------------------------------------------------------
// Certification report vocabulary (WP2, plan §1)
// ---------------------------------------------------------------------------

const BoundedCertificationDetail = Schema.String.check(Schema.isMaxLength(4_096));

/**
 * Adapter self-description carried by a certificate. `durability` is read from the candidate
 * ledger's `capabilities` — the adapter's honest claim (persistence.md), never inferred by the
 * certification runner.
 */
export class CertifiedAdapterIdentity extends Schema.Class<CertifiedAdapterIdentity>(
  "@effect-agent/thread/CertifiedAdapterIdentity",
)({
  /** Workspace/package name of the adapter pair under certification. */
  name: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  version: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(256))),
  durability: LedgerCapabilities.fields.durability,
}) {}

/** Which shared suite one Tier-1 (or lever-driven Tier-3) case belongs to. */
export const CertificationSuite = Schema.Literals([
  "submission-ledger",
  "thread-store",
  "real-loss",
]);

export type CertificationSuite = typeof CertificationSuite.Type;

/** One executed certification case: a conformance contract case or a Tier-3 lever row. */
export class CertificationCaseResult extends Schema.Class<CertificationCaseResult>(
  "@effect-agent/thread/CertificationCaseResult",
)({
  suite: CertificationSuite,
  name: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
  status: Schema.Literals(["passed", "failed"]),
  detail: Schema.optionalKey(BoundedCertificationDetail),
}) {}

/**
 * The six Tier-2 scenario shapes (plan §1): every `DurableRuntimeFailpointLocation` is armed
 * once per shape, so each coordinator fault boundary is exercised in every protocol family it
 * can appear in.
 */
export const CertificationScenario = Schema.Literals([
  "plain",
  "uncertain-tool",
  "durable-steps",
  "approval",
  "join",
  "delegation",
]);

export type CertificationScenario = typeof CertificationScenario.Type;

/**
 * Outcome of one Tier-2 sweep cell:
 *
 * - `converged` — the armed fault fired once, recovery classified the state, and the re-drive
 *   converged to verified invariants;
 * - `not-triggered` — the armed location is not on this scenario's coordinator path (recorded
 *   honestly, never claimed as fault coverage); the clean run still converged and verified;
 * - `failed` — anything else.
 */
export const CertificationSweepStatus = Schema.Literals(["converged", "not-triggered", "failed"]);
export type CertificationSweepStatus = typeof CertificationSweepStatus.Type;

/** One Tier-2 cell: one scenario shape with one armed coordinator failpoint location. */
export class CertificationSweepResult extends Schema.Class<CertificationSweepResult>(
  "@effect-agent/thread/CertificationSweepResult",
)({
  scenario: CertificationScenario,
  location: DurableRuntimeFailpointLocation,
  /** The armed fault actually fired (one-shot) somewhere in the cell's drive or re-drive. */
  failpointFired: Schema.Boolean,
  status: CertificationSweepStatus,
  /**
   * Every lane's digest chain recomputed from `EMPTY_TAIL_DIGEST` and matched the exported
   * tail — fully discharged here because the runner captures per-batch producer identity at
   * append time (the port-only admin `verify` reports this check `skipped` instead).
   */
  digestChainVerified: Schema.Boolean,
  detail: Schema.optionalKey(BoundedCertificationDetail),
}) {}

/**
 * How the real-loss tier was discharged. Tier 3 is never silently claimed: an adapter either
 * exercised a real loss lever in this run (`exercised`), cites committed real-loss evidence
 * (`recorded-evidence`), honestly records that nothing exercised it (`not-exercised`), or is a
 * non-durable reference adapter with no real loss to exercise (`not-applicable`).
 */
export const CertificationTierThreeStatus = Schema.Literals([
  "exercised",
  "recorded-evidence",
  "not-exercised",
  "not-applicable",
]);

export type CertificationTierThreeStatus = typeof CertificationTierThreeStatus.Type;

/** The Tier-3 (real loss lever) section of a certificate. */
export class CertificationTierThreeReport extends Schema.Class<CertificationTierThreeReport>(
  "@effect-agent/thread/CertificationTierThreeReport",
)({
  status: CertificationTierThreeStatus,
  /** Cited real-loss suites. The runner does not execute or verify these references. */
  evidence: Schema.Array(BoundedCertificationDetail),
  /** Rows executed by a supplied crash lever in THIS run (`suite: "real-loss"`). */
  cases: Schema.Array(CertificationCaseResult),
  detail: Schema.optionalKey(BoundedCertificationDetail),
}) {}

/**
 * The Schema-encoded certification report (plan §1): Tier 1 = the two shared port contract
 * suites verbatim; Tier 2 = the coordinator failpoint convergence sweep; Tier 3 = the real
 * loss lever record. `ok` is true exactly when every EXECUTED check passed — honest scope
 * statements (`not-triggered`, `recorded-evidence`, `not-exercised`, `not-applicable`) never
 * count as failures and never count as silent coverage. `fullyCertified` additionally requires
 * a durable adapter and passing real-loss cases executed in this run. Citations alone do not
 * certify recovery. A non-durable adapter can pass conformance but is never fully certified.
 * The semantic assertions live in the runner tests.
 */
export class CertificationReport extends Schema.Class<CertificationReport>(
  "@effect-agent/thread/CertificationReport",
)({
  format: Schema.Literal("effect-agent/certification@2"),
  adapter: CertifiedAdapterIdentity,
  generatedAt: Schema.DateTimeUtcFromString,
  tier1: Schema.Array(CertificationCaseResult),
  tier2: Schema.Array(CertificationSweepResult),
  tier3: CertificationTierThreeReport,
  ok: Schema.Boolean,
  fullyCertified: Schema.Boolean,
}) {}

/** Bounded operator-readable rendering of one failure Cause for a certification detail. */
const causeDetail = (cause: Cause.Cause<unknown>): string => {
  const failure = Cause.findErrorOption(cause);

  if (Option.isSome(failure)) {
    const diagnostic = inspectForeignDiagnostic(failure.value);

    if (diagnostic.tag !== undefined || diagnostic.message !== undefined) {
      const tag = diagnostic.tag ?? "Error";

      return `${tag}${diagnostic.message === undefined ? "" : `: ${diagnostic.message}`}`.slice(
        0,
        4_096,
      );
    }

    return safeUnknownString(failure.value, "Unknown foreign failure").slice(0, 4_096);
  }

  return `defect: ${safeUnknownString(cause, "Unknown defect")}`.slice(0, 4_096);
};

const caseResult = <A, E>(
  suite: CertificationSuite,
  name: string,
  exit: Exit.Exit<A, E>,
): CertificationCaseResult =>
  Exit.isSuccess(exit)
    ? CertificationCaseResult.make({ suite, name, status: "passed" })
    : CertificationCaseResult.make({
        suite,
        name,
        status: "failed",
        detail: causeDetail(exit.cause),
      });

/**
 * Tier 1 — port contract (plan §1): run BOTH shared conformance case arrays verbatim against
 * the provided candidate adapters and fold every case into a typed result. Case failures are
 * captured per-case (never short-circuiting the sweep), so a certificate always reports the
 * complete contract surface. Requires the candidate `SubmissionLedger` and `ThreadStore`
 * plus `Crypto.Crypto`, and must run under a TestClock (the ledger cases drive lease expiry
 * through virtual time).
 */
export const certifyPorts = Effect.fn("Thread.certifyPorts")(function* (): Effect.fn.Return<
  ReadonlyArray<CertificationCaseResult>,
  never,
  SubmissionLedger | ThreadStore | Crypto.Crypto
> {
  const results: Array<CertificationCaseResult> = [];

  for (const contractCase of submissionLedgerConformanceCases) {
    const exit = yield* Effect.exit(contractCase.run);

    results.push(caseResult("submission-ledger", contractCase.name, exit));
  }
  for (const contractCase of threadStoreConformanceCases) {
    const exit = yield* Effect.exit(contractCase.run);

    results.push(caseResult("thread-store", contractCase.name, exit));
  }

  return results;
});
