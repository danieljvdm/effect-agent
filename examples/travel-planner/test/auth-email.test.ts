import { it } from "@effect/vitest";
import { LoginIdentifier } from "@yielded/auth/Identity";
import {
  EmailProofDelivery,
  ProofDeliveryId,
  ProofPurpose,
  ProofReference,
  ProofId,
} from "@yielded/auth/Proofs";
import { DateTime, Deferred, Effect, Fiber, Redacted } from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vite-plus/test";

import { emailDeliveryLayer } from "../src/auth/email-delivery";

const send = Effect.gen(function* () {
  const delivery = yield* EmailProofDelivery;

  return yield* delivery.send({
    deliveryId: ProofDeliveryId.make("fixture-send"),
    purpose: ProofPurpose.make("email-signin"),
    reference: ProofReference.make({
      proofId: ProofId.make("fixture-proof"),
      purpose: ProofPurpose.make("email-signin"),
      keyId: "v1",
    }),
    recipient: LoginIdentifier.make({ namespace: "email", value: "traveler@example.invalid" }),
    secret: Redacted.make("123456"),
    format: "numeric-code",
    expiresAtMillis: DateTime.toEpochMillis(yield* DateTime.now) + 300_000,
    template: "elsewhere-code",
    locale: "en",
  });
});

it.effect("sends a bounded Cloudflare email with the code only in the message bodies", () =>
  Effect.gen(function* () {
    const sent: unknown[] = [];

    const result = yield* send.pipe(
      Effect.provide(
        emailDeliveryLayer(
          {
            send: async (message) => {
              sent.push(message);

              return { messageId: "fixture" };
            },
          },
          "signin@example.invalid",
        ),
      ),
    );

    expect(result).toEqual({ _tag: "Accepted" });
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      from: { name: "Elsewhere", email: "signin@example.invalid" },
      to: "traveler@example.invalid",
      subject: "Your Elsewhere sign-in code",
      text: expect.stringContaining("123456"),
      html: expect.stringContaining("123456"),
    });
  }),
);

it.effect("does not retry rejected or ambiguous email sends and bounds waiting", () =>
  Effect.gen(function* () {
    let calls = 0;

    const failed = yield* send.pipe(
      Effect.provide(
        emailDeliveryLayer(
          {
            send: async () => {
              calls++;
              throw new Error("private provider response");
            },
          },
          "signin@example.invalid",
        ),
      ),
    );

    expect(failed).toEqual({ _tag: "Ambiguous" });
    expect(calls).toBe(1);
    const started = yield* Deferred.make<void>();

    const pending = yield* send.pipe(
      Effect.provide(
        emailDeliveryLayer(
          {
            send: () => {
              calls++;
              Deferred.doneUnsafe(started, Effect.void);

              return new Promise<EmailSendResult>(() => {});
            },
          },
          "signin@example.invalid",
        ),
      ),
      Effect.forkChild,
    );

    yield* Deferred.await(started);
    yield* TestClock.adjust("11 seconds");
    expect(yield* Fiber.join(pending)).toEqual({ _tag: "Ambiguous" });
    expect(calls).toBe(2);
  }),
);

it.effect("releases the caller on interruption without starting a second external send", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    let calls = 0;
    let finalized = false;

    const fiber = yield* send.pipe(
      Effect.provide(
        emailDeliveryLayer(
          {
            send: () => {
              calls++;
              Deferred.doneUnsafe(started, Effect.void);

              return new Promise<EmailSendResult>(() => {});
            },
          },
          "signin@example.invalid",
        ),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          finalized = true;
        }),
      ),
      Effect.forkChild,
    );

    yield* Deferred.await(started);
    yield* Fiber.interrupt(fiber);
    expect(finalized).toBe(true);
    expect(calls).toBe(1);
  }),
);
