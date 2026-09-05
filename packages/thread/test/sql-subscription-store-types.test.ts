import type {
  makeSqlSubscriptionStore,
  SqlSubscriptionTransaction,
} from "@effect-agent/thread/SqlSubscriptionStore";
import type { SubscriptionError } from "@effect-agent/thread/Subscription";
import { expectTypeOf, it } from "@effect/vitest";
import type { Effect } from "effect";
import type { SqlClient } from "effect/unstable/sql/SqlClient";

type Construction = ReturnType<typeof makeSqlSubscriptionStore>;
type Store = Effect.Success<Construction>;

it("requires SQL and transaction services at construction and captures them for store operations", () => {
  expectTypeOf<Effect.Services<Construction>>().toEqualTypeOf<
    SqlClient | SqlSubscriptionTransaction
  >();
  expectTypeOf<Effect.Error<Construction>>().toEqualTypeOf<SubscriptionError>();
  expectTypeOf<Effect.Services<ReturnType<Store["register"]>>>().toEqualTypeOf<never>();
});
