import { Context, Effect, Encoding, FileSystem, Layer, Result } from "effect";

import { DeliveryUnauthentic, type IngressPolicy, type PlatformDelivery } from "./contracts.ts";

export interface TrustedActionsIdentity {
  readonly repository: string;
  readonly eventName: string;
  readonly eventPayload: string;
  readonly deliveryId: string;
}

export class ObservedActionsIdentity extends Context.Service<
  ObservedActionsIdentity,
  {
    readonly read: Effect.Effect<TrustedActionsIdentity | void>;
  }
>()("@effect-agent/example-pr-work-order-ingress/ObservedActionsIdentity") {
  static readonly layerFromEnvironment = Layer.effect(
    ObservedActionsIdentity,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      return ObservedActionsIdentity.of({
        read: Effect.gen(function* () {
          if (process.env.GITHUB_ACTIONS !== "true") return undefined;
          const repository = process.env.GITHUB_REPOSITORY;
          const eventName = process.env.GITHUB_EVENT_NAME;
          const eventPath = process.env.GITHUB_EVENT_PATH;
          const runId = process.env.GITHUB_RUN_ID;
          const runAttempt = process.env.GITHUB_RUN_ATTEMPT ?? "1";
          if (
            repository === undefined ||
            eventName === undefined ||
            eventPath === undefined ||
            runId === undefined
          ) {
            return undefined;
          }
          const eventPayload = yield* fs
            .readFileString(eventPath)
            .pipe(Effect.orElseSucceed((): string | undefined => undefined));
          if (eventPayload === undefined) return undefined;
          return {
            repository,
            eventName,
            eventPayload,
            deliveryId: `${runId}:${runAttempt}`,
          };
        }),
      });
    }),
  );

  static readonly layerAbsent = Layer.succeed(
    ObservedActionsIdentity,
    ObservedActionsIdentity.of({
      read: Effect.void,
    }),
  );
}

const hexToBytes = (hex: string): Uint8Array | undefined => {
  const decoded = Encoding.decodeHex(hex);
  return Result.isSuccess(decoded) ? decoded.success : undefined;
};

const hmacKey = (secret: string, usage: ReadonlyArray<"sign" | "verify">) =>
  Effect.tryPromise({
    try: () =>
      globalThis.crypto.subtle.importKey(
        "raw",
        new TextEncoder().encode(secret),
        { name: "HMAC", hash: "SHA-256" },
        false,
        [...usage],
      ),
    catch: (cause) =>
      DeliveryUnauthentic.make({
        reason: `HMAC key import failed: ${String(cause).slice(0, 1_000)}`,
      }),
  });

export const signGitHubDelivery = Effect.fn("signGitHubDelivery")(function* (
  secret: string,
  rawBody: string,
) {
  const key = yield* hmacKey(secret, ["sign"]);
  const signature = yield* Effect.tryPromise({
    try: () => globalThis.crypto.subtle.sign("HMAC", key, new TextEncoder().encode(rawBody)),
    catch: (cause) =>
      DeliveryUnauthentic.make({
        reason: `HMAC sign failed: ${String(cause).slice(0, 1_000)}`,
      }),
  });
  return `sha256=${Encoding.encodeHex(new Uint8Array(signature))}`;
});

const verifySignature = Effect.fn("verifySignature")(function* (
  secret: string,
  rawBody: string,
  header: string,
) {
  if (!header.startsWith("sha256=")) return false;
  const bytes = hexToBytes(header.slice("sha256=".length));
  if (bytes === undefined) return false;
  const key = yield* hmacKey(secret, ["verify"]);
  const signature = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(signature).set(bytes);
  return yield* Effect.tryPromise({
    try: () =>
      globalThis.crypto.subtle.verify("HMAC", key, signature, new TextEncoder().encode(rawBody)),
    catch: (cause) =>
      DeliveryUnauthentic.make({
        reason: `HMAC verify failed: ${String(cause).slice(0, 1_000)}`,
      }),
  });
});

export const authenticateDelivery = Effect.fn("authenticateDelivery")(function* (
  delivery: PlatformDelivery,
  policy: IngressPolicy["Service"],
): Effect.fn.Return<void, DeliveryUnauthentic, ObservedActionsIdentity> {
  if (delivery.signature !== undefined) {
    const valid = yield* verifySignature(
      policy.webhookSecret,
      delivery.rawBody,
      delivery.signature,
    );
    if (!valid) {
      return yield* DeliveryUnauthentic.make({
        reason: "delivery signature is not authentic",
      });
    }
    return;
  }
  const actions = yield* (yield* ObservedActionsIdentity).read;
  if (
    actions &&
    actions.repository === policy.repository &&
    actions.eventName === delivery.eventName &&
    actions.eventPayload === delivery.rawBody &&
    actions.deliveryId === delivery.deliveryId
  ) {
    return;
  }
  return yield* DeliveryUnauthentic.make({
    reason: "delivery has neither a valid signature nor a trusted Actions identity",
  });
});
