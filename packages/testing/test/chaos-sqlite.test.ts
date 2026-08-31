import {
  SqliteStorageFailpointError,
  SqliteStorageFailpointLocation,
  layer as sqliteThreadStoreLayer,
  ledgerLayer as sqliteLedgerLayer,
} from "@effect-agent/storage-sqlite";
import {
  chaosSeedFromEnv,
  generateChaosPlans,
  runChaosPlan,
  type ChaosAdapterFailpoints,
  type ChaosPlan,
} from "@effect-agent/testing/chaos";
import {
  DeploymentId,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  ProducerId,
  ToolReconciler,
  WakeScheduler,
} from "@effect-agent/thread";
import { DurableRuntimeFailpointTestControl } from "@effect-agent/thread/testing";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Duration, Effect, Exit, FileSystem, Layer, Option, Schema } from "effect";
import { TestClock } from "effect/testing";

/**
 * P7 WP4 SQLite chaos lane (plan §5): a reduced seeded-plan sweep over the REAL SQLite adapter
 * pair, adding the adapter-owned failpoint arms (`ledger:*` / `append:*`) to the coordinator
 * arms the memory lane already exercises. One fresh database file per plan; the ledger and the
 * store open their own connections to it, as the DN production assembly's substrate does.
 *
 * This suite lives in `packages/testing` (not `packages/storage-sqlite/test`) because vp's task
 * graph rejects dev-dependency cycles and `@effect-agent/testing` already depends on
 * `@effect-agent/storage-sqlite` — the same constraint that moved the adapter-backed durable
 * runtime suites here (see the c106b53 precedent).
 */

const ROOT_SEED = chaosSeedFromEnv(process.env);
const PLAN_COUNT = 24;

/** Adapter arms offered to the generator; every name decodes as a real SQLite location. */
const SQLITE_ARM_POOL: ReadonlyArray<SqliteStorageFailpointLocation> = [
  "append:before",
  "append:after-record-insert",
  "ledger:admit:after",
  "ledger:claim:after",
  "ledger:mark-ready:after",
  "ledger:reserve-settlement:after",
  "ledger:finalize-settlement:before",
];

const decodeSqliteLocation = Schema.decodeUnknownOption(SqliteStorageFailpointLocation);

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-chaos-sqlite"),
  producerId: Schema.decodeSync(ProducerId)("producer-chaos-sqlite"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

/**
 * A per-plan mutable arm shared by BOTH adapter layers' failpoint handlers plus the
 * runner-facing `ChaosAdapterFailpoints` control that flips it.
 */
const makeSqliteArm = (): {
  readonly handler: (
    location: SqliteStorageFailpointLocation,
  ) => Effect.Effect<void, SqliteStorageFailpointError>;
  readonly failpoints: ChaosAdapterFailpoints;
} => {
  let active: SqliteStorageFailpointLocation | undefined;
  return {
    handler: (location) =>
      Effect.suspend(() =>
        location === active
          ? Effect.fail(SqliteStorageFailpointError.make({ location }))
          : Effect.void,
      ),
    failpoints: {
      arm: (location) =>
        Effect.sync(() => {
          active = Option.getOrUndefined(decodeSqliteLocation(location));
        }),
      clear: Effect.sync(() => {
        active = undefined;
      }),
    },
  };
};

/** One FRESH SQLite adapter pair + coordinator per plan over its own database file. */
const freshLayer = (
  filename: string,
  handler: (
    location: SqliteStorageFailpointLocation,
  ) => Effect.Effect<void, SqliteStorageFailpointError>,
) =>
  DurableAgentRuntime.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        sqliteLedgerLayer({ filename, observationPollInterval: 1, failpoint: handler }),
        sqliteThreadStoreLayer({
          filename,
          observationPollInterval: 1,
          failpoint: handler,
        }),
        WakeScheduler.layerNoop,
        DurableRuntimeFailpointTestControl.layer,
        ToolReconciler.uncertain,
        configLayer,
      ).pipe(Layer.provideMerge(NodeCrypto.layer)),
    ),
  );

const replayHint = (planIndex: number, plan: ChaosPlan): string =>
  `replay: CHAOS_SEED=${ROOT_SEED} (plan #${planIndex}, plan seed ${plan.seed}, ` +
  `arms [${[...plan.failpointArms, ...plan.adapterArms].join(", ")}])`;

describe("DUR-002/DUR-004/DUR-017 P7 chaos (SQLite adapters)", () => {
  it.effect(
    `CHAOS: ${PLAN_COUNT} seeded interleavings over SQLite converge (reduced count, adapter arms included)`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const directory = yield* fileSystem.makeTempDirectoryScoped({
            prefix: "effect-agent-chaos-sqlite-",
          });
          const plans = generateChaosPlans({
            seed: ROOT_SEED,
            count: PLAN_COUNT,
            adapterArms: SQLITE_ARM_POOL,
          });
          let adapterArmedPlans = 0;
          for (const [planIndex, plan] of plans.entries()) {
            if (plan.adapterArms.length > 0) adapterArmedPlans += 1;
            const arm = makeSqliteArm();
            const exit = yield* Effect.exit(
              runChaosPlan(plan, {
                adapterFailpoints: arm.failpoints,
                // The SQLite ledger blocks every new claim until the lease expires (D5:
                // expiry revokes liveness only; epochs stay the correctness fence), so a
                // dead Attempt's lane reopens deterministically one TestClock minute later.
                betweenRounds: TestClock.adjust(Duration.minutes(1)),
              }).pipe(
                Effect.provide(freshLayer(`${directory}/plan-${plan.seed}.sqlite`, arm.handler)),
              ),
            );
            if (Exit.isFailure(exit)) {
              throw new Error(
                `sqlite chaos plan failed — ${replayHint(planIndex, plan)}\n${Cause.pretty(exit.cause)}`,
              );
            }
            expect(exit.value.openObligations).toBe(0);
            for (const lane of exit.value.lanes) {
              expect(lane.verified, `${replayHint(planIndex, plan)} lane ${lane.threadId}`).toBe(
                true,
              );
            }
          }
          // The reduced sweep still exercises adapter-owned failpoint arms.
          expect(adapterArmedPlans).toBeGreaterThan(0);
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    240_000,
  );
});
