import {
  type AdmissionPolicyError,
  type AdmissionConflict,
  type LedgerError,
  SubmissionLedger,
  type AdmissionRequest,
} from "@effect-agent/thread/SubmissionLedger";
import {
  type SubscriptionScope,
  type SubscriptionStoreFailure,
  type SubscriptionError,
  type SubscriptionKey,
  type SubscriptionDeliveryKey,
} from "@effect-agent/thread/Subscription";
import {
  Subscriptions,
  type SubscribeOptions,
  type SubscriptionFailure,
} from "@effect-agent/thread/Subscriptions";
import { expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";

/** A host can compose management without losing either errors or service requirements. */
const revise = (scope: SubscriptionScope, options: SubscribeOptions) =>
  Effect.gen(function* () {
    const subscriptions = yield* Subscriptions;
    const created = yield* subscriptions.subscribe(scope, { ...options, expiresAtMillis: null });
    const inspected = yield* subscriptions.getSubscription(scope, created.key);

    const paused = yield* subscriptions.pauseSubscription(
      scope,
      created.key,
      inspected.configurationRevision,
    );

    return yield* subscriptions.updateSubscription(
      scope,
      created.key,
      paused.configurationRevision,
      options,
    );
  });

const recover = (scope: SubscriptionScope, key: SubscriptionDeliveryKey, generation: number) =>
  Effect.flatMap(Subscriptions, (subscriptions) =>
    subscriptions.recoverDelivery(scope, key, generation),
  );

const inspect = (scope: SubscriptionScope, key: SubscriptionKey) =>
  Effect.flatMap(Subscriptions, (subscriptions) => subscriptions.getSubscription(scope, key));

const admit = (request: AdmissionRequest) =>
  Effect.flatMap(SubmissionLedger, (ledger) => ledger.admit(request));

it("preserves public management and admission E/R contracts", () => {
  expectTypeOf<Effect.Services<ReturnType<typeof revise>>>().toEqualTypeOf<Subscriptions>();
  expectTypeOf<Effect.Error<ReturnType<typeof revise>>>().toEqualTypeOf<SubscriptionFailure>();
  expectTypeOf<
    Effect.Error<ReturnType<typeof recover>>
  >().toEqualTypeOf<SubscriptionStoreFailure>();
  expectTypeOf<Effect.Error<ReturnType<typeof inspect>>>().toEqualTypeOf<SubscriptionError>();
  expectTypeOf<Effect.Services<ReturnType<typeof admit>>>().toEqualTypeOf<SubmissionLedger>();
  expectTypeOf<Effect.Error<ReturnType<typeof admit>>>().toEqualTypeOf<
    AdmissionConflict | AdmissionPolicyError | LedgerError
  >();
});
