import * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import {
  CERTIFICATION_SCENARIOS,
  TIER2_UNREACHED_LOCATIONS,
  certifyDurableAdapters,
  tier2NeverFiredLocations,
} from "@effect-agent/testing/certification";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted } from "effect";
import { DurableRuntimeFailpointLocation } from "effect-agent/durable-failpoint";
import { submissionLedgerConformanceCases } from "effect-agent/testing/submission-ledger-conformance";
import { threadStoreConformanceCases } from "effect-agent/testing/thread-store-conformance";

/**
 * Keep certification outside the adapter package to avoid a storage-postgres -> testing cycle.
 * Both ports share one client over one temporary database. No process-kill suite drives this
 * adapter yet, so Tier 3 must remain not exercised.
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

const combinedAdapters = (url: string) =>
  PostgresStorage.layerWith({ observationPollInterval: 1 }).pipe(
    Layer.provideMerge(NodeCrypto.layer),
    Layer.provideMerge(PgClient.layer({ url: Redacted.make(url) })),
  );

it.effect(
  "certifies PostgreSQL contracts and recovery without claiming process-kill coverage",
  () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const adapters = combinedAdapters(url);

        const report = yield* certifyDurableAdapters({
          adapter: { name: "@effect-agent/storage-postgres" },
          submissionLedger: adapters,
          threadStore: adapters,
        });

        const ledgerCases = report.tier1.filter((result) => result.suite === "submission-ledger");
        const storeCases = report.tier1.filter((result) => result.suite === "thread-store");

        expect(ledgerCases).toHaveLength(submissionLedgerConformanceCases.length);
        expect(storeCases).toHaveLength(threadStoreConformanceCases.length);
        expect(report.tier1.filter((result) => result.status !== "passed")).toEqual([]);
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
        expect(report.adapter.name).toBe("@effect-agent/storage-postgres");
        expect(report.adapter.durability).toBe("durable-node");
        expect(report.ok).toBe(true);
        expect(report.tier3).toMatchObject({ status: "not-exercised", evidence: [], cases: [] });
        expect(report.fullyCertified).toBe(false);
      }).pipe(Effect.provide(NodeCrypto.layer)),
    ),
  300_000,
);
