import { it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Schema } from "effect";
import { TestClock } from "effect/testing";
import {
  CompactSign,
  createLocalJWKSet,
  exportJWK,
  generateKeyPair,
  SignJWT,
  type JWTPayload,
} from "jose";
import { beforeAll, expect, expectTypeOf } from "vite-plus/test";

import type { AccessError } from "../src/access-domain.ts";
import { AccessSession, adminEmail } from "../src/access-domain.ts";
import type { authenticate } from "../src/server/access-auth.ts";
import { makeAuthenticate } from "../src/server/access-auth.ts";

const env = {
  ACCESS_TEAM_DOMAIN: "https://travel-test.cloudflareaccess.com",
  ACCESS_AUD: "travel-planner-application",
};

const now = 1_800_000_000;

const validClaims = {
  iss: env.ACCESS_TEAM_DOMAIN,
  aud: [env.ACCESS_AUD],
  sub: "test-access-user",
  iat: now - 60,
  exp: now + 60,
  email: adminEmail,
};

let keys: Awaited<ReturnType<typeof generateKeyPair>>;
let forgedKeys: Awaited<ReturnType<typeof generateKeyPair>>;
let verify: ReturnType<typeof makeAuthenticate>;

beforeAll(async () => {
  keys = await generateKeyPair("RS256");
  forgedKeys = await generateKeyPair("RS256");
  const publicKey = await exportJWK(keys.publicKey);

  verify = makeAuthenticate(() => createLocalJWKSet({ keys: [{ ...publicKey, kid: "access" }] }));
});

const sign = (payload: JWTPayload) =>
  Effect.promise(() =>
    new SignJWT(payload).setProtectedHeader({ alg: "RS256", kid: "access" }).sign(keys.privateKey),
  );

const request = (token: string) =>
  new Request("https://planner.example/api/rpc", {
    headers: { "Cf-Access-Jwt-Assertion": token },
  });

const reject = Effect.fn(function* (payload: JWTPayload) {
  const token = yield* sign(payload);
  const failure = yield* verify(request(token), env).pipe(Effect.flip);

  expect(failure).toMatchObject({ _tag: "AccessError", code: "unauthorized" });
  expect(failure.message).not.toContain(token);
});

it.effect(
  "verifies signed sessions, normalizes email, and derives admin status only from identity",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(now * 1_000);
      for (const [email, expectedEmail, isAdmin] of [
        ["DANIELJMERWE@GMAIL.COM", adminEmail, true],
        ["Friend@Example.com", "friend@example.com", false],
        ["danieljmerwe+other@gmail.com", "danieljmerwe+other@gmail.com", false],
      ] as const) {
        const token = yield* sign({ ...validClaims, email, isAdmin: true });

        const session = yield* verify(request(token), {
          ...env,
          ACCESS_TEAM_DOMAIN: `${env.ACCESS_TEAM_DOMAIN}/`,
        });

        expect(session).toEqual({ email: expectedEmail, isAdmin });
        expect(Schema.is(AccessSession)(session)).toBe(true);
      }
    }),
);

it.effect(
  "requires the assertion and ignores bearer credentials and unsigned identity headers",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(now * 1_000);
      const token = yield* sign(validClaims);

      const missing = new Request("https://planner.example/api/rpc", {
        headers: {
          Authorization: `Bearer ${token}`,
          "Cf-Access-Authenticated-User-Email": adminEmail,
        },
      });

      expect(yield* verify(missing, env).pipe(Effect.flip)).toMatchObject({ code: "unauthorized" });
      for (const malformed of ["not-a-jwt", "", "x".repeat(16_385)])
        expect(yield* verify(request(malformed), env).pipe(Effect.flip)).toMatchObject({
          code: "unauthorized",
        });

      const forged = yield* Effect.promise(() =>
        new SignJWT(validClaims)
          .setProtectedHeader({ alg: "RS256", kid: "access" })
          .sign(forgedKeys.privateKey),
      );

      expect(yield* verify(request(forged), env).pipe(Effect.flip)).toMatchObject({
        code: "unauthorized",
      });

      const wrongAlgorithm = yield* Effect.promise(() =>
        new SignJWT(validClaims)
          .setProtectedHeader({ alg: "HS256", kid: "access" })
          .sign(new Uint8Array(32)),
      );

      expect(yield* verify(request(wrongAlgorithm), env).pipe(Effect.flip)).toMatchObject({
        code: "unauthorized",
      });
    }),
);

it.effect("rejects wrong scope, expired or future tokens, and invalid signed claims", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(now * 1_000);
    for (const replacement of [
      { aud: "another-application" },
      { aud: [env.ACCESS_AUD, 123] },
      { iss: "https://other.cloudflareaccess.com" },
      { exp: now },
      { iat: now + 1 },
      { nbf: now + 1 },
      { sub: "" },
      { sub: 123 },
      { email: "not-an-email" },
      { email: 123 },
      { iat: "yesterday" },
      { exp: "tomorrow" },
    ]) {
      // Sign arbitrary JSON so the verifier, not the signing helper's typed setters, owns rejection.
      const token = yield* Effect.promise(() =>
        new CompactSign(
          new TextEncoder().encode(JSON.stringify({ ...validClaims, ...replacement })),
        )
          .setProtectedHeader({ alg: "RS256", kid: "access" })
          .sign(keys.privateKey),
      );

      expect(yield* verify(request(token), env).pipe(Effect.flip)).toMatchObject({
        code: "unauthorized",
      });
    }
    for (const required of ["email", "sub", "iat", "exp", "iss", "aud"]) {
      const payload = Object.fromEntries(
        Object.entries(validClaims).filter(([name]) => name !== required),
      );

      yield* reject(payload);
    }

    const nonobject = yield* Effect.promise(() =>
      new CompactSign(new TextEncoder().encode("[]"))
        .setProtectedHeader({ alg: "RS256", kid: "access" })
        .sign(keys.privateKey),
    );

    expect(yield* verify(request(nonobject), env).pipe(Effect.flip)).toMatchObject({
      code: "unauthorized",
    });
  }),
);

it.effect("uses Effect time for expiry and revalidates each request", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(now * 1_000);
    const token = yield* sign(validClaims);

    expect((yield* verify(request(token), env)).isAdmin).toBe(true);
    yield* TestClock.adjust("60 seconds");
    expect(yield* verify(request(token), env).pipe(Effect.flip)).toMatchObject({
      code: "unauthorized",
    });
  }),
);

it.effect("rejects missing and unsafe host configuration before resolving any signing key", () =>
  Effect.gen(function* () {
    const configured = makeAuthenticate(() => {
      throw new Error("Invalid configuration must never resolve a key");
    });

    for (const candidate of [
      {},
      { ...env, ACCESS_AUD: "" },
      { ...env, ACCESS_AUD: " " },
      ...[
        "http://travel-test.cloudflareaccess.com",
        "https://cloudflareaccess.com",
        "https://travel-test.cloudflareaccess.com.evil.example",
        "https://user@travel-test.cloudflareaccess.com",
        "https://travel-test.cloudflareaccess.com:8443",
        "https://travel-test.cloudflareaccess.com/path",
        "https://travel-test.cloudflareaccess.com?key=1",
        "https://travel-test.cloudflareaccess.com#fragment",
        "https://127.0.0.1",
      ].map((ACCESS_TEAM_DOMAIN) => ({ ...env, ACCESS_TEAM_DOMAIN })),
    ])
      expect(yield* configured(request("irrelevant"), candidate).pipe(Effect.flip)).toMatchObject({
        code: "unavailable",
      });
  }),
);

it.effect("redacts resolver failures and cancels request-local signing-key retrieval", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(now * 1_000);
    const token = yield* sign(validClaims);

    const unavailable = makeAuthenticate(() => async () => {
      throw new Error("private-provider-body");
    });

    const failure = yield* unavailable(request(token), env).pipe(Effect.flip);

    expect(failure.code).toBe("unauthorized");
    expect(failure.message).not.toContain("private-provider-body");
    const started = yield* Deferred.make<void>();
    const canceled = yield* Deferred.make<void>();

    const pending = makeAuthenticate((url, signal) => () => {
      expect(url.href).toBe(`${env.ACCESS_TEAM_DOMAIN}/cdn-cgi/access/certs`);

      return new Promise((_, fail) => {
        Deferred.doneUnsafe(started, Effect.void);
        signal.addEventListener(
          "abort",
          () => {
            Deferred.doneUnsafe(canceled, Effect.void);
            fail(new Error("canceled"));
          },
          { once: true },
        );
      });
    });

    const fiber = yield* pending(request(token), env).pipe(Effect.forkChild);

    yield* Deferred.await(started);
    yield* Fiber.interrupt(fiber);
    yield* Deferred.await(canceled);
  }),
);

it("exposes only a typed Access session or AccessError without runtime dependencies", () => {
  expectTypeOf<Effect.Success<ReturnType<typeof authenticate>>>().toEqualTypeOf<AccessSession>();
  expectTypeOf<Effect.Error<ReturnType<typeof authenticate>>>().toEqualTypeOf<AccessError>();
  expectTypeOf<Effect.Services<ReturnType<typeof authenticate>>>().toEqualTypeOf<never>();
});
