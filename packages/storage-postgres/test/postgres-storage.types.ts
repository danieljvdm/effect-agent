import type * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import type {
  PostgresStorageError,
  PostgresStorageInitializationError,
} from "@effect-agent/storage-postgres/postgres-storage-error";
import { expectTypeOf } from "@effect/vitest";
import type { Crypto, Layer } from "effect";
import type { ActivityMutationFailure, ActivityStoreError } from "effect-agent/activity-store";
import type { SubmissionLedger } from "effect-agent/submission-ledger";
import type { ThreadStore } from "effect-agent/thread-store";
import type { SqlClient } from "effect/unstable/sql/SqlClient";

type Persistence = typeof PostgresStorage.layer;
type Ledger = ReturnType<typeof PostgresStorage.submissionLedgerLayer>;

expectTypeOf<Layer.Services<Persistence>>().toEqualTypeOf<SqlClient | Crypto.Crypto>();
expectTypeOf<Layer.Success<Persistence>>().toEqualTypeOf<ThreadStore | SubmissionLedger>();
expectTypeOf<Layer.Error<Persistence>>().toEqualTypeOf<PostgresStorageInitializationError>();
expectTypeOf<Layer.Services<Ledger>>().toEqualTypeOf<SqlClient | Crypto.Crypto>();
expectTypeOf<Layer.Success<Ledger>>().toEqualTypeOf<SubmissionLedger>();
expectTypeOf<Layer.Error<Ledger>>().toEqualTypeOf<PostgresStorageInitializationError>();
expectTypeOf<
  Layer.Services<ReturnType<typeof PostgresStorage.scheduleStoreLayer>>
>().toEqualTypeOf<SqlClient>();
expectTypeOf<
  Layer.Services<ReturnType<typeof PostgresStorage.activityStoreLayer>>
>().toEqualTypeOf<SqlClient>();
expectTypeOf<Layer.Error<ReturnType<typeof PostgresStorage.activityStoreLayer>>>().toEqualTypeOf<
  PostgresStorageError | ActivityStoreError | ActivityMutationFailure
>();
expectTypeOf<
  Layer.Services<ReturnType<typeof PostgresStorage.subscriptionStoreLayer>>
>().toEqualTypeOf<SqlClient>();
expectTypeOf<
  Layer.Services<ReturnType<typeof PostgresStorage.messageDeliveryStoreLayer>>
>().toEqualTypeOf<SqlClient>();
