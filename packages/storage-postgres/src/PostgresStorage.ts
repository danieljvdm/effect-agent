import { makeSqlActivityStore } from "@effect-agent/storage-sql/sql-activity-store";
import { makeSqlMessageDeliveryStore } from "@effect-agent/storage-sql/sql-message-delivery-store";
import { makeSqlScheduleStore } from "@effect-agent/storage-sql/sql-schedule-store";
import { makeSqlSubmissionLedger } from "@effect-agent/storage-sql/sql-submission-ledger";
import { makeSqlSubscriptionStore } from "@effect-agent/storage-sql/sql-subscription-store";
import { makeSqlThreadStore } from "@effect-agent/storage-sql/sql-thread-store";
import { Duration, Effect, Layer, Schema } from "effect";
import {
  ActivityMutationFailpoint,
  ActivityProcessorStore,
  ActivityStoreError,
} from "effect-agent/activity-store";
import {
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "effect-agent/message-delivery";
import { ScheduleStore } from "effect-agent/schedule";
import { DEFAULT_OWNERSHIP_LEASE_DURATION, LedgerError } from "effect-agent/submission-ledger";
import { SubscriptionError, SubscriptionStore, SourcePartition } from "effect-agent/subscription";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  classifyWriteFailure,
  ensurePostgresSchema,
  initializePostgresStorage,
  makePostgresJournal,
  postgresStorageErrors,
  withWriterLockTransaction,
} from "./internal/postgres-storage.ts";
import {
  PostgresStorageError,
  type PostgresStorageFailpointError,
  type PostgresStorageFailpointLocation,
} from "./PostgresStorageError.ts";

export { CurrentPostgresStorageVersion } from "./internal/postgres-storage.ts";

export type PostgresStorageFailpointHandler = (
  location: PostgresStorageFailpointLocation,
) => Effect.Effect<void, PostgresStorageFailpointError>;

const Settings = Schema.Struct({
  observationPollInterval: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  lockTimeout: Schema.Int.check(Schema.isGreaterThan(0)),
  ownershipLeaseDuration: Schema.Int.check(Schema.isGreaterThan(0)),
  verifyOnOpen: Schema.Boolean,
  schema: Schema.NonEmptyString.check(
    Schema.isMaxLength(63),
    Schema.isPattern(/^[a-z_][a-z0-9_]*$/),
  ),
});

export interface PostgresStorageOptions {
  /** Namespace for qualified storage tables, created under the writer lock when absent. Defaults to public. */
  readonly schema?: string | undefined;
  /** Journal observation polling interval in milliseconds. Defaults to 25. */
  readonly observationPollInterval?: number | undefined;
  /** Positive writer-lock timeout in milliseconds. Defaults to 5,000. */
  readonly lockTimeout?: number | undefined;
  /** Ownership lease in milliseconds. Defaults to the SubmissionLedger's lease duration. */
  readonly ownershipLeaseDuration?: number | undefined;
  /** Audit every stored payload and digest chain on open. Defaults to false. */
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: PostgresStorageFailpointHandler | undefined;
  readonly activityFailpoint?: ActivityMutationFailpoint["Service"]["hit"] | undefined;
}

/**
 * Compose selected storage ports over the application's native PostgreSQL SqlClient.
 * Provide SqlClient and Crypto at the composition root; reuse this result to share format
 * initialization. Activity progress remains independent and does not initialize the Thread journal.
 */
export const make = (options: PostgresStorageOptions = {}) => {
  const settings = Schema.decodeEffect(Settings)({
    observationPollInterval: options.observationPollInterval ?? 25,
    lockTimeout: options.lockTimeout ?? 5_000,
    ownershipLeaseDuration:
      options.ownershipLeaseDuration ?? Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
    verifyOnOpen: options.verifyOnOpen ?? false,
    schema: options.schema ?? "public",
  }).pipe(
    Effect.mapError((cause) =>
      PostgresStorageError.make({
        cause,
        operation: "configure Postgres storage",
        message: cause.message,
      }),
    ),
  );

  const hitFailpoint: PostgresStorageFailpointHandler = options.failpoint ?? (() => Effect.void);

  const initialized = Layer.effectDiscard(Effect.flatMap(settings, initializePostgresStorage));

  const journal = Effect.flatMap(settings, (config) =>
    makePostgresJournal(config.lockTimeout, hitFailpoint, config.schema),
  );

  const threadStore = Layer.effectContext(
    Effect.gen(function* () {
      const config = yield* settings;

      return yield* makeSqlThreadStore(yield* journal, {
        ...config,
        namespace: config.schema,
        errors: postgresStorageErrors,
        hitFailpoint,
        offsetPrefix: "effect-agent-postgres@1:",
      });
    }),
  ).pipe(Layer.provide(initialized));

  const submissionLedger = Layer.effectContext(
    Effect.gen(function* () {
      const config = yield* settings;

      return yield* makeSqlSubmissionLedger(yield* journal, {
        namespace: config.schema,
        errors: postgresStorageErrors,
        hitFailpoint,
        ownershipLeaseDuration: config.ownershipLeaseDuration,
        sqlFailure: (operation) => (cause) => {
          const internal = classifyWriteFailure(operation)(cause);

          return LedgerError.make({ operation, message: internal.message, cause: internal });
        },
      });
    }),
  ).pipe(Layer.provide(initialized));

  const scheduleStore = Layer.effect(
    ScheduleStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* settings;

      return yield* makeSqlScheduleStore(
        withWriterLockTransaction(sql, config.lockTimeout),
        config.schema,
      );
    }),
  ).pipe(Layer.provide(initialized));

  const messageDeliveryStore = (limits?: MessageDeliveryStoreLimits) =>
    Layer.effect(
      MessageDeliveryStore,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const config = yield* settings;

        return yield* makeSqlMessageDeliveryStore(limits, {
          namespace: config.schema,
          transaction: withWriterLockTransaction(sql, config.lockTimeout),
        });
      }),
    ).pipe(Layer.provide(initialized));

  const subscriptionStore = (owned: SourcePartition) =>
    Layer.unwrap(
      Schema.decodeEffect(SourcePartition)(owned).pipe(
        Effect.mapError(() => SubscriptionError.make({ reason: "validation", code: "partition" })),
        Effect.map((partition) =>
          Layer.effect(
            SubscriptionStore,
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              const config = yield* settings;
              const transaction = withWriterLockTransaction(sql, config.lockTimeout);

              // Retention DDL must share the writer lock with format initialization.
              return yield* transaction(
                makeSqlSubscriptionStore(partition, {
                  namespace: config.schema,
                  transaction,
                  maxStoredJsonLength: 16 * 1024 * 1024,
                }),
              ).pipe(
                Effect.catchTag("SqlError", () =>
                  SubscriptionError.make({ reason: "storage", code: "initialize" }),
                ),
              );
            }),
          ).pipe(Layer.provide(initialized)),
        ),
      ),
    );

  const activityStore = Layer.effect(
    ActivityProcessorStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* settings;

      return yield* makeSqlActivityStore(
        withWriterLockTransaction(sql, config.lockTimeout),
        ensurePostgresSchema(config.schema).pipe(
          Effect.mapError(() =>
            ActivityStoreError.make({
              operation: "initialize activity schema",
              reason: "unavailable",
            }),
          ),
        ),
        config.schema,
      );
    }),
  ).pipe(
    Layer.provide(
      options.activityFailpoint === undefined
        ? ActivityMutationFailpoint.layer
        : Layer.succeed(ActivityMutationFailpoint)({ hit: options.activityFailpoint }),
    ),
  );

  return {
    threadStore,
    submissionLedger,
    scheduleStore,
    messageDeliveryStore,
    subscriptionStore,
    activityStore,
  } as const;
};
