import { it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { FetchHttpClient } from "effect/unstable/http";
import { expect } from "vite-plus/test";

import { adminEmail } from "../src/access-domain.ts";
import { credentialForOwner } from "../src/server/credentials.ts";
import { storageOwner as ownerThread } from "../src/server/tenancy.ts";
import { createVoiceSession } from "../src/server/voice-http.ts";

const environment = Effect.gen(function* () {
  const bytes = new Uint8Array(32);
  const iv = new Uint8Array(12);

  const key = yield* Effect.promise(() =>
    crypto.subtle.importKey("raw", bytes, "AES-GCM", false, ["encrypt"]),
  );

  const cipher = yield* Effect.promise(() =>
    crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv,
        additionalData: new TextEncoder().encode(`travel-planner:openai:v1:${ownerThread}`),
      },
      key,
      new TextEncoder().encode("sk-voice-PRIVATE"),
    ),
  );

  return {
    BYOK_ENCRYPTION_KEY: btoa(String.fromCharCode(...bytes)),
    THREADS: {
      getByName: (owner: string) => ({
        demoAccessAllowed: async () => false,
        modelCredential: async () => {
          expect(owner).toBe(ownerThread);

          return JSON.stringify({
            version: 1,
            iv: btoa(String.fromCharCode(...iv)),
            ciphertext: btoa(String.fromCharCode(...new Uint8Array(cipher))),
            lastFour: "VATE",
            updatedAt: "2026-09-10T00:00:00Z",
          });
        },
      }),
    },
  };
});

const session = { email: adminEmail, isAdmin: true };
const offer = { sdp: "offer", history: [{ role: "user" as const, text: "Plan Lisbon" }] };

it.effect(
  "uses the verified owner's key for client delegation and returns only the SDP/session identity",
  () =>
    Effect.gen(function* () {
      const env = yield* environment;
      const signals: AbortSignal[] = [];

      for (const status of [201, 302, 401, 429]) {
        const result = yield* createVoiceSession(offer, env, session).pipe(
          Effect.provide(FetchHttpClient.layer),
          Effect.provideService(FetchHttpClient.Fetch, async (url, init) => {
            expect(String(url)).toBe("https://api.openai.com/v1/live/sessions");
            expect(new Headers(init?.headers).get("authorization")).toBe("Bearer sk-voice-PRIVATE");
            expect(init?.redirect).toBe("manual");
            const body = await new Response(init?.body).json();

            expect(body).toMatchObject({
              session: {
                model: "gpt-live-1",
                delegation: { type: "client" },
                input: [{ role: "user", content: [{ type: "input_text", text: "Plan Lisbon" }] }],
              },
              transport: { type: "webrtc", sdp: "offer" },
            });
            if (init?.signal) signals.push(init.signal);

            return Response.json(
              {
                session: { id: "live-test", secret: "PRIVATE" },
                transport: { type: "webrtc", sdp: "answer" },
                secret: "PRIVATE",
              },
              { status },
            );
          }),
          Effect.result,
        );

        expect(result._tag).toBe(status === 201 ? "Success" : "Failure");
        expect(JSON.stringify(result)).not.toContain("PRIVATE");
      }
      expect(signals.every((signal) => signal.aborted)).toBe(true);
    }),
);

it.effect("times out session creation, aborts transport, and retains no provider error body", () =>
  Effect.gen(function* () {
    const env = yield* environment;
    const entered = yield* Deferred.make<void>();
    let signal: AbortSignal | undefined;

    const fiber = yield* createVoiceSession(offer, env, session).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, async (_url, init) => {
        signal = init?.signal ?? undefined;
        Deferred.doneUnsafe(entered, Effect.void);

        return new Promise<Response>((_resolve, reject) =>
          signal?.addEventListener("abort", () => reject(new Error("PRIVATE"))),
        );
      }),
      Effect.result,
      Effect.forkChild,
    );

    yield* Deferred.await(entered);
    yield* TestClock.adjust("26 seconds");
    const result = yield* Fiber.join(fiber);

    expect(result._tag).toBe("Failure");
    expect(JSON.stringify(result)).not.toContain("PRIVATE");
    expect(signal?.aborted).toBe(true);
  }),
);

it.effect(
  "uses sponsored voice only for the verified account and rechecks new-call revocation",
  () =>
    Effect.gen(function* () {
      let allowed = true;
      let calls = 0;

      const env = {
        DEMO_OPENAI_API_KEY: "sk-demo-voice-PRIVATE",
        THREADS: {
          getByName: (owner: string) => ({
            modelCredential: async () => {
              expect(owner).toBe(ownerThread);

              return "null";
            },
            demoAccessAllowed: async (account: string) => {
              expect(owner).toBe(ownerThread);
              expect(account).toBe(ownerThread);

              return allowed;
            },
          }),
        },
      };

      const fetch: typeof globalThis.fetch = async (_url, init) => {
        calls++;
        expect(new Headers(init?.headers).get("authorization")).toBe(
          "Bearer sk-demo-voice-PRIVATE",
        );

        return Response.json(
          { session: { id: "sponsored-voice" }, transport: { type: "webrtc", sdp: "answer" } },
          { status: 201 },
        );
      };

      const run = createVoiceSession(offer, env, session).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
        Effect.result,
      );

      const accepted = yield* run;

      expect(accepted._tag).toBe("Success");
      expect(JSON.stringify(accepted)).not.toContain("PRIVATE");
      allowed = false;
      expect((yield* run)._tag).toBe("Failure");
      expect(calls).toBe(1);
    }),
);

it.effect("bounds a stalled funding lookup and never grants access after timeout", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();

    const env = {
      DEMO_OPENAI_API_KEY: "sk-stalled-demo-PRIVATE",
      THREADS: {
        getByName: () => ({
          modelCredential: async () => "null",
          demoAccessAllowed: () => {
            Deferred.doneUnsafe(entered, Effect.void);

            return new Promise<boolean>(() => {});
          },
        }),
      },
    };

    const fiber = yield* credentialForOwner(env, ownerThread).pipe(Effect.result, Effect.forkChild);

    yield* Deferred.await(entered);
    yield* TestClock.adjust("11 seconds");
    const result = yield* Fiber.join(fiber);

    expect(result._tag).toBe("Failure");
    expect(JSON.stringify(result)).not.toContain("PRIVATE");
  }),
);
