import { makeSqlActivityStore } from "@effect-agent/storage-sql/sql-activity-store";
import { makeSqlMessageDeliveryStore } from "@effect-agent/storage-sql/sql-message-delivery-store";
import { makeSqlScheduleStore } from "@effect-agent/storage-sql/sql-schedule-store";
import { makeSqlSubmissionLedger } from "@effect-agent/storage-sql/sql-submission-ledger";
import { makeSqlSubscriptionStore } from "@effect-agent/storage-sql/sql-subscription-store";
import { makeSqlThreadStore } from "@effect-agent/storage-sql/sql-thread-store";
import { Context, Duration, Effect, Layer, Schema } from "effect";
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
import {
  DEFAULT_OWNERSHIP_LEASE_DURATION,
  LedgerError,
  SubmissionLedger,
} from "effect-agent/submission-ledger";
import { SubscriptionError, SubscriptionStore, SourcePartition } from "effect-agent/subscription";
import { ThreadStore } from "effect-agent/thread-store";
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

const settings = (options: PostgresStorageOptions) =>
  Schema.decodeEffect(Settings)({
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

const initialize = Effect.fnUntraced(function* (options: PostgresStorageOptions) {
  const config = yield* settings(options);

  yield* initializePostgresStorage(config);

  return config;
});

const openJournal = Effect.fnUntraced(function* (options: PostgresStorageOptions) {
  const config = yield* initialize(options);
  const hitFailpoint: PostgresStorageFailpointHandler = options.failpoint ?? (() => Effect.void);
  const journal = yield* makePostgresJournal(config.lockTimeout, hitFailpoint, config.schema);

  return { config, journal, hitFailpoint };
});

type Journal = Effect.Success<ReturnType<typeof openJournal>>;

const makeThreadStore = ({ config, journal, hitFailpoint }: Journal) =>
  makeSqlThreadStore(journal, {
    ...config,
    namespace: config.schema,
    errors: postgresStorageErrors,
    hitFailpoint,
    offsetPrefix: "effect-agent-postgres@1:",
  });

const makeSubmissionLedger = ({ config, journal, hitFailpoint }: Journal) =>
  makeSqlSubmissionLedger(journal, {
    namespace: config.schema,
    errors: postgresStorageErrors,
    hitFailpoint,
    ownershipLeaseDuration: config.ownershipLeaseDuration,
    sqlFailure: (operation) => (cause) => {
      const internal = classifyWriteFailure(operation)(cause);

      return LedgerError.make({ operation, message: internal.message, cause: internal });
    },
  });

/** Thread history and submissions sharing one initialized journal. Requires SqlClient and Crypto. */
export const layerWith = (options: PostgresStorageOptions) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const journal = yield* openJournal(options);
      const threadStore = yield* makeThreadStore(journal);
      const submissionLedger = yield* makeSubmissionLedger(journal);

      return Context.make(ThreadStore, threadStore).pipe(
        Context.add(SubmissionLedger, submissionLedger),
      );
    }),
  );

/** Thread history and submissions with default settings. Requires SqlClient and Crypto. */
export const layer = layerWith({});

/** Standalone thread history over the application's SqlClient and Crypto. */
export const threadStoreLayer = (options: PostgresStorageOptions = {}) =>
  Layer.effect(ThreadStore, Effect.flatMap(openJournal(options), makeThreadStore));

/** Standalone submissions over the application's SqlClient and Crypto. */
export const submissionLedgerLayer = (options: PostgresStorageOptions = {}) =>
  Layer.effect(SubmissionLedger, Effect.flatMap(openJournal(options), makeSubmissionLedger));

/** Schedules over the application's SqlClient. */
export const scheduleStoreLayer = (options: PostgresStorageOptions = {}) =>
  Layer.effect(
    ScheduleStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* initialize(options);

      return yield* makeSqlScheduleStore(
        withWriterLockTransaction(sql, config.lockTimeout),
        config.schema,
      );
    }),
  );

/** Message delivery over the application's SqlClient, with optional retention limits. */
export const messageDeliveryStoreLayer = (
  options: PostgresStorageOptions & {
    readonly limits?: MessageDeliveryStoreLimits | undefined;
  } = {},
) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* initialize(options);

      return yield* makeSqlMessageDeliveryStore(options.limits, {
        namespace: config.schema,
        transaction: withWriterLockTransaction(sql, config.lockTimeout),
      });
    }),
  );

/** Subscriptions owned by an explicit partition. Invalid partitions fail before accessing SQL. */
export const subscriptionStoreLayer = (
  owned: SourcePartition,
  options: PostgresStorageOptions = {},
) =>
  Layer.effect(
    SubscriptionStore,
    Effect.gen(function* () {
      const partition = yield* Schema.decodeEffect(SourcePartition)(owned).pipe(
        Effect.mapError(() => SubscriptionError.make({ reason: "validation", code: "partition" })),
      );

      const config = yield* initialize(options);
      const sql = yield* SqlClient.SqlClient;
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
  );

/** Independent activity progress over SqlClient; does not initialize the Thread journal. */
export const activityStoreLayer = (options: PostgresStorageOptions = {}) =>
  Layer.effect(
    ActivityProcessorStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* settings(options);

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
