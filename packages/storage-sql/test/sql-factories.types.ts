import { expectTypeOf } from "@effect/vitest";
import { Context, Effect, Schema, type Crypto } from "effect";
import type { SubmissionLedger } from "effect-agent/submission-ledger";
import type { AppendConflict, FenceRejected, ThreadStore } from "effect-agent/thread-store";
import type { SqlClient } from "effect/unstable/sql/SqlClient";

import type { makeSqlJournal, SqlJournal } from "../src/SqlJournal.ts";
import type { makeSqlSubmissionLedger } from "../src/SqlSubmissionLedger.ts";
import type { makeSqlThreadStore } from "../src/SqlThreadStore.ts";

class StorageFailure extends Schema.TaggedError<StorageFailure>()("StorageFailure", {}) {}
class Corruption extends Schema.TaggedError<Corruption>()("Corruption", {}) {}
class Contention extends Schema.TaggedError<Contention>()("Contention", {}) {}
class InjectedFailure extends Schema.TaggedError<InjectedFailure>()("InjectedFailure", {}) {}
class BodyFailure extends Schema.TaggedError<BodyFailure>()("BodyFailure", {}) {}
class RequiredService extends Context.Service<RequiredService, {}>()("RequiredService") {}

type Journal = SqlJournal<StorageFailure, Corruption, Contention, InjectedFailure>;
type JournalConstruction = ReturnType<
  typeof makeSqlJournal<StorageFailure, Corruption, Contention, InjectedFailure>
>;
type ThreadConstruction = ReturnType<
  typeof makeSqlThreadStore<StorageFailure, Corruption, Contention, InjectedFailure>
>;
type LedgerConstruction = ReturnType<
  typeof makeSqlSubmissionLedger<StorageFailure, Corruption, Contention, InjectedFailure>
>;

expectTypeOf<Effect.Error<JournalConstruction>>().toEqualTypeOf<never>();
expectTypeOf<Effect.Services<JournalConstruction>>().toEqualTypeOf<SqlClient>();
expectTypeOf<Effect.Error<ReturnType<Journal["append"]>>>().toEqualTypeOf<
  StorageFailure | Corruption | Contention | InjectedFailure | AppendConflict | FenceRejected
>();
expectTypeOf<Effect.Services<ThreadConstruction>>().toEqualTypeOf<SqlClient | Crypto.Crypto>();
expectTypeOf<Effect.Error<ThreadConstruction>>().toEqualTypeOf<StorageFailure | Corruption>();
expectTypeOf<Effect.Services<LedgerConstruction>>().toEqualTypeOf<SqlClient | Crypto.Crypto>();
expectTypeOf<Effect.Error<LedgerConstruction>>().toEqualTypeOf<never>();
expectTypeOf<
  Effect.Services<ReturnType<ThreadStore["Service"]["append"]>>
>().toEqualTypeOf<never>();
expectTypeOf<
  Effect.Services<ReturnType<SubmissionLedger["Service"]["claim"]>>
>().toEqualTypeOf<never>();

export const preservesTransactionChannels = (journal: Journal) => {
  const transaction = journal.withWriteTransaction("typed body")(
    Effect.andThen(RequiredService, Effect.fail(BodyFailure.make({}))),
  );

  expectTypeOf<Effect.Error<typeof transaction>>().toEqualTypeOf<
    BodyFailure | StorageFailure | Contention
  >();
  expectTypeOf<Effect.Services<typeof transaction>>().toEqualTypeOf<RequiredService>();
};
