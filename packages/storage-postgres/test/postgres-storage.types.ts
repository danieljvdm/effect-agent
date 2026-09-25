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

type Storage = ReturnType<typeof PostgresStorage.make>;

expectTypeOf<Layer.Services<Storage["threadStore"]>>().toEqualTypeOf<SqlClient | Crypto.Crypto>();
expectTypeOf<Layer.Success<Storage["threadStore"]>>().toEqualTypeOf<ThreadStore>();
expectTypeOf<
  Layer.Error<Storage["threadStore"]>
>().toEqualTypeOf<PostgresStorageInitializationError>();
expectTypeOf<Layer.Services<Storage["submissionLedger"]>>().toEqualTypeOf<
  SqlClient | Crypto.Crypto
>();
expectTypeOf<Layer.Success<Storage["submissionLedger"]>>().toEqualTypeOf<SubmissionLedger>();
expectTypeOf<
  Layer.Error<Storage["submissionLedger"]>
>().toEqualTypeOf<PostgresStorageInitializationError>();
expectTypeOf<Layer.Services<Storage["scheduleStore"]>>().toEqualTypeOf<SqlClient>();
expectTypeOf<Layer.Services<Storage["activityStore"]>>().toEqualTypeOf<SqlClient>();
expectTypeOf<Layer.Error<Storage["activityStore"]>>().toEqualTypeOf<
  PostgresStorageError | ActivityStoreError | ActivityMutationFailure
>();
expectTypeOf<Layer.Services<ReturnType<Storage["subscriptionStore"]>>>().toEqualTypeOf<SqlClient>();
expectTypeOf<
  Layer.Services<ReturnType<Storage["messageDeliveryStore"]>>
>().toEqualTypeOf<SqlClient>();
