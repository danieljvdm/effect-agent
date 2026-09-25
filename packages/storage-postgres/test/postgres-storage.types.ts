import type * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import type * as PostgresStorageClient from "@effect-agent/storage-postgres/postgres-storage-client";
import type {
  PostgresStorageError,
  PostgresStorageInitializationError,
} from "@effect-agent/storage-postgres/postgres-storage-error";
import { expectTypeOf } from "@effect/vitest";
import type { Layer } from "effect";
import type { ActivityMutationFailure, ActivityStoreError } from "effect-agent/activity-store";
import type { SubmissionLedger } from "effect-agent/submission-ledger";
import type { ThreadStore } from "effect-agent/thread-store";

type Storage = ReturnType<typeof PostgresStorage.make>;

expectTypeOf<Layer.Services<Storage["threadStore"]>>().toEqualTypeOf<never>();
expectTypeOf<Layer.Success<Storage["threadStore"]>>().toEqualTypeOf<ThreadStore>();
expectTypeOf<
  Layer.Error<Storage["threadStore"]>
>().toEqualTypeOf<PostgresStorageInitializationError>();
expectTypeOf<Layer.Services<Storage["submissionLedger"]>>().toEqualTypeOf<never>();
expectTypeOf<Layer.Success<Storage["submissionLedger"]>>().toEqualTypeOf<SubmissionLedger>();
expectTypeOf<
  Layer.Error<Storage["submissionLedger"]>
>().toEqualTypeOf<PostgresStorageInitializationError>();
expectTypeOf<Layer.Services<Storage["scheduleStore"]>>().toEqualTypeOf<never>();
expectTypeOf<Layer.Services<Storage["activityStore"]>>().toEqualTypeOf<never>();
expectTypeOf<Layer.Error<Storage["activityStore"]>>().toEqualTypeOf<
  PostgresStorageError | ActivityStoreError | ActivityMutationFailure
>();
expectTypeOf<Layer.Services<ReturnType<Storage["subscriptionStore"]>>>().toEqualTypeOf<never>();
expectTypeOf<Layer.Services<ReturnType<Storage["messageDeliveryStore"]>>>().toEqualTypeOf<never>();
expectTypeOf<
  Layer.Error<ReturnType<typeof PostgresStorageClient.layer>>
>().toEqualTypeOf<PostgresStorageError>();
