import { Context, Effect, Encoding, Layer, Option, Redacted, Result, Schema } from "effect";

import {
  JournalClaimed,
  JournalCompleted,
  type WorkOrderJournalState,
  WorkOrderJournalState as WorkOrderJournalStateSchema,
} from "./action-contracts.ts";
import { IngressStoreFailure } from "./contracts.ts";

const MARKER_PREFIX = "<!-- effect-agent-work-order:v1:";
const MARKER_SUFFIX = " -->";
const MARKER_PATTERN = /<!-- effect-agent-work-order:v1:([A-Za-z0-9+/=]+)\.([0-9a-f]{64}) -->/g;
const SIGNATURE_DOMAIN = "effect-agent/pr-work-order-journal/v1\0";

const failure = (operation: string, cause: unknown) =>
  IngressStoreFailure.make({
    operation,
    reason: String(cause).slice(0, 4_096),
  });

const signatureBuffer = (hex: string): ArrayBuffer | undefined => {
  const decoded = Encoding.decodeHex(hex);
  if (Result.isFailure(decoded)) return undefined;
  const buffer = new ArrayBuffer(decoded.success.byteLength);
  new Uint8Array(buffer).set(decoded.success);
  return buffer;
};

const hmacKey = (secret: Redacted.Redacted<string>, operation: string) =>
  Effect.tryPromise({
    try: () =>
      globalThis.crypto.subtle.importKey(
        "raw",
        new TextEncoder().encode(Redacted.value(secret)),
        { name: "HMAC", hash: "SHA-256" },
        false,
        ["sign", "verify"],
      ),
    catch: (cause) => failure(operation, cause),
  });

export class WorkOrderJournalAuthenticator extends Context.Service<
  WorkOrderJournalAuthenticator,
  {
    readonly render: (
      state: WorkOrderJournalState,
      visible: string,
    ) => Effect.Effect<string, IngressStoreFailure>;
    readonly extract: (
      body: string,
    ) => Effect.Effect<Option.Option<WorkOrderJournalState>, IngressStoreFailure>;
  }
>()("@effect-agent/example-pr-work-order-ingress/WorkOrderJournalAuthenticator") {
  static readonly layer = (secret: Redacted.Redacted<string>) =>
    Layer.succeed(
      WorkOrderJournalAuthenticator,
      WorkOrderJournalAuthenticator.of({
        render: (state, visible) =>
          Effect.gen(function* () {
            const json = yield* Schema.encodeEffect(
              Schema.fromJsonString(WorkOrderJournalStateSchema),
            )(state).pipe(Effect.mapError((cause) => failure("encode work-order journal", cause)));
            const payload = Encoding.encodeBase64(json);
            const key = yield* hmacKey(secret, "sign work-order journal");
            const signature = yield* Effect.tryPromise({
              try: () =>
                globalThis.crypto.subtle.sign(
                  "HMAC",
                  key,
                  new TextEncoder().encode(`${SIGNATURE_DOMAIN}${payload}`),
                ),
              catch: (cause) => failure("sign work-order journal", cause),
            });
            const hex = Encoding.encodeHex(new Uint8Array(signature));
            return `${visible.slice(0, 1_200)}\n\n${MARKER_PREFIX}${payload}.${hex}${MARKER_SUFFIX}`;
          }),
        extract: (body) => {
          if (body.length > 60_000) return Effect.succeed(Option.none());
          return Effect.gen(function* () {
            const key = yield* hmacKey(secret, "verify work-order journal");
            const candidates = [...body.matchAll(MARKER_PATTERN)].slice(-32).reverse();
            for (const match of candidates) {
              const payload = match[1];
              const signature = match[2];
              if (payload === undefined || signature === undefined) continue;
              const json = Result.getOrUndefined(Encoding.decodeBase64String(payload));
              const bytes = signatureBuffer(signature);
              if (json === undefined || bytes === undefined) continue;
              const state = Schema.decodeUnknownOption(
                Schema.fromJsonString(WorkOrderJournalStateSchema),
              )(json);
              if (Option.isNone(state)) continue;
              const valid = yield* Effect.tryPromise({
                try: () =>
                  globalThis.crypto.subtle.verify(
                    "HMAC",
                    key,
                    bytes,
                    new TextEncoder().encode(`${SIGNATURE_DOMAIN}${payload}`),
                  ),
                catch: (cause) => failure("verify work-order journal", cause),
              });
              if (valid) return state;
            }
            return Option.none();
          });
        },
      }),
    );
}

export const claimedState = (input: {
  readonly eventId: string;
  readonly repository: string;
  readonly pullRequestNumber: number;
  readonly sourceCommentId: string;
  readonly workOrderId: string;
  readonly workOrderDigest: JournalClaimed["workOrderDigest"];
  readonly expectedHeadSha: JournalClaimed["expectedHeadSha"];
  readonly runId: string;
}): JournalClaimed => JournalClaimed.make({ _tag: "claimed", version: 1, ...input });

export const completedState = (
  claimed: JournalClaimed,
  terminal: JournalCompleted["terminal"],
): JournalCompleted =>
  JournalCompleted.make({
    _tag: "completed",
    version: 1,
    eventId: claimed.eventId,
    repository: claimed.repository,
    pullRequestNumber: claimed.pullRequestNumber,
    sourceCommentId: claimed.sourceCommentId,
    workOrderId: claimed.workOrderId,
    workOrderDigest: claimed.workOrderDigest,
    expectedHeadSha: claimed.expectedHeadSha,
    runId: claimed.runId,
    terminal,
  });
