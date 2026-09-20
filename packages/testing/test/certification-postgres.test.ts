import * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import {
  CERTIFICATION_SCENARIOS,
  TIER2_UNREACHED_LOCATIONS,
  certifyDurableAdapters,
  tier2NeverFiredLocations,
} from "@effect-agent/testing/certification";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Schema } from "effect";
import { DurableRuntimeFailpointLocation } from "effect-agent/durable-failpoint";
import { CertificationCaseResult, CertificationReport } from "effect-agent/testing/certification";
import { submissionLedgerConformanceCases } from "effect-agent/testing/submission-ledger-conformance";
import { threadStoreConformanceCases } from "effect-agent/testing/thread-store-conformance";

import { maybeWriteReport } from "./certification-report-io.ts";

/**
 * Lives here for the same reason the SQLite runner does: only the Cloudflare packages may
 * dev-depend on `testing`, and vp's task graph rejects the storage-* -> testing cycle. Both
 * ports share one client over one temporary database. Tier 3 is reported as not exercised: no
 * committed process-kill suite drives this adapter yet.
 */
const adminUrl =
  process.env.EFFECT_AGENT_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:55432/effect_agent";

let databaseCounter = 0;

const admin = (statement: string) =>
  Effect.flatMap(PgClient.PgClient, (sql) => sql.unsafe(statement)).pipe(
    Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })),
    Effect.orDie,
  );

/** A database per run; `WITH (FORCE)` ends any pooled connection the run left open. */
const withTemporaryDatabase = <A, E>(
  use: (url: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      databaseCounter = databaseCounter + 1;

      return `effect_agent_certification_${process.pid}_${databaseCounter}`;
    }).pipe(Effect.tap((database) => admin(`CREATE DATABASE ${database}`))),
    (database) => {
      const url = new URL(adminUrl);

      url.pathname = `/${database}`;

      return use(url.toString());
    },
    (database) => admin(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`).pipe(Effect.ignore),
  );

const combinedAdapters = (url: string) => {
  const storage = PostgresStorage.make({
    client: { url: Redacted.make(url) },
    observationPollInterval: 1,
  });

  return Layer.mergeAll(
    storage.threadStore,
    storage.submissionLedger,
    storage.clientLayer,
    NodeCrypto.layer,
  );
};

let cached: CertificationReport | undefined;

const certified = Effect.gen(function* () {
  if (cached !== undefined) return cached;

  const report = yield* withTemporaryDatabase((url) => {
    // One Layer instance for both ports: memoization builds it once, so the ledger and the
    // store share one client over one database.
    const adapters = combinedAdapters(url);

    return certifyDurableAdapters({
      adapter: { name: "@effect-agent/storage-postgres" },
      submissionLedger: adapters,
      threadStore: adapters,
    }).pipe(Effect.provide(NodeCrypto.layer));
  });

  yield* maybeWriteReport("storage-postgres", report);
  cached = report;

  return report;
});

describe("adapter certification — storage-postgres", () => {
  // Verdict combinations belong to certification-verdict.test.ts. Keep one negative full
  // run as well as the shared successful certificate: bypassing report aggregation must fail.
  it.effect(
    "propagates a failed supplied crash lever into the final certification verdict",
    () =>
      withTemporaryDatabase((url) =>
        Effect.gen(function* () {
          const failedCase = CertificationCaseResult.make({
            suite: "real-loss",
            name: "injected lever failure",
            status: "failed",
          });

          let leverRuns = 0;
          const adapters = combinedAdapters(url);

          const report = yield* certifyDurableAdapters({
            adapter: { name: "failed-lever" },
            submissionLedger: adapters,
            threadStore: adapters,
            crashLever: Effect.sync(() => {
              leverRuns++;

              return [failedCase];
            }),
          });

          expect(leverRuns).toBe(1);
          expect(report.tier1.filter((row) => row.status === "failed")).toEqual([]);
          expect(report.tier2.filter((row) => row.status === "failed")).toEqual([]);
          expect(report.tier3).toMatchObject({ status: "exercised", cases: [failedCase] });
          expect(report.ok).toBe(false);
          expect(report.fullyCertified).toBe(false);
        }).pipe(Effect.provide(NodeCrypto.layer)),
      ),
    300_000,
  );

  it.effect(
    "TIER1: all SubmissionLedger and ThreadStore contract cases pass",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        const ledgerCases = report.tier1.filter((result) => result.suite === "submission-ledger");
        const storeCases = report.tier1.filter((result) => result.suite === "thread-store");

        expect(ledgerCases).toHaveLength(submissionLedgerConformanceCases.length);
        expect(storeCases).toHaveLength(threadStoreConformanceCases.length);
        expect(report.tier1.filter((result) => result.status !== "passed")).toEqual([]);
      }),
    300_000,
  );

  it.effect(
    "TIER2: every coordinator failpoint leaves a classifiable state and re-drive converges",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;

        expect(report.tier2.map(({ scenario, location }) => [scenario, location])).toEqual(
          CERTIFICATION_SCENARIOS.flatMap((scenario) =>
            DurableRuntimeFailpointLocation.literals.map((location) => [scenario, location]),
          ),
        );
        expect(report.tier2.filter((row) => row.status === "failed")).toEqual([]);
        expect(report.tier2.every((row) => row.digestChainVerified)).toBe(true);
        for (const scenario of CERTIFICATION_SCENARIOS) {
          expect(report.tier2.some((row) => row.scenario === scenario && row.failpointFired)).toBe(
            true,
          );
        }
        expect(tier2NeverFiredLocations(report.tier2)).toEqual(
          [...TIER2_UNREACHED_LOCATIONS].sort(),
        );
      }),
    300_000,
  );

  it.effect(
    "the certification report round-trips its Schema and names the adapter identity and durability claim",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        const encoded = yield* Schema.encodeEffect(CertificationReport)(report);
        const decoded = yield* Schema.decodeEffect(CertificationReport)(encoded);

        expect(decoded.format).toBe("effect-agent/certification@2");
        expect(decoded.adapter.name).toBe("@effect-agent/storage-postgres");
        expect(decoded.adapter.durability).toBe("durable-node");
        expect(decoded.ok).toBe(true);
      }),
    300_000,
  );

  it.effect(
    "TIER3: reports the process-kill loss lever as not yet exercised",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;

        expect(report.tier3).toMatchObject({ status: "not-exercised", evidence: [], cases: [] });
        expect(report.fullyCertified).toBe(false);
      }),
    300_000,
  );
});
