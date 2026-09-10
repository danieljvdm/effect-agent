import { it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/unstable/http";
import { expect, expectTypeOf } from "vite-plus/test";

import { AccessError, adminEmail } from "../src/access-domain.ts";
import {
  AccessCommand,
  AccessFailpoint,
  manageAccess,
  type AccessAdminEnvironment,
} from "../src/server/access-admin.ts";

const env = {
  ACCESS_ACCOUNT_ID: "a".repeat(32),
  ACCESS_GROUP_ID: "11111111-2222-3333-4444-555555555555",
  ACCESS_API_TOKEN: "private-test-api-token",
};

const groupUrl = `https://api.cloudflare.com/client/v4/accounts/${env.ACCESS_ACCOUNT_ID}/access/groups/${env.ACCESS_GROUP_ID}`;

const group = (emails: ReadonlyArray<string> = [adminEmail]) => ({
  id: env.ACCESS_GROUP_ID,
  name: "effect-agent-travel-planner-invited",
  include: emails.map((email) => ({ email: { email } })),
  exclude: [],
  require: [],
});

const invite = Schema.decodeUnknownSync(AccessCommand)({
  _tag: "Invite",
  email: "Friend@Example.com",
});

const list = Schema.decodeUnknownSync(AccessCommand)({ _tag: "List" });

it.effect("refuses redirects at the credentialed fetch boundary", () =>
  Effect.gen(function* () {
    let calls = 0;

    const fetch: typeof globalThis.fetch = async (input, options) => {
      calls++;
      expect(input).toEqual(new URL(groupUrl));
      expect(options?.redirect).toBe("manual");

      return new Response(null, {
        status: 302,
        headers: { location: "https://untrusted.example/receive-credentials" },
      });
    };

    const error = yield* manageAccess(list, env).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
      Effect.flip,
    );

    expect(error.code).toBe("unavailable");
    expect(calls).toBe(1);
  }),
);

/** A mutable remote group, observed only through the actual HTTP request/response boundary. */
const makeApi = Effect.fn(function* (initial: Schema.Json = group()) {
  const remote = yield* Ref.make(initial);
  const methods = yield* Ref.make<Array<string>>([]);

  const client = HttpClient.make((request, url) =>
    Effect.gen(function* () {
      expect(url.href).toBe(groupUrl);
      expect(request.headers.authorization).toBe(`Bearer ${env.ACCESS_API_TOKEN}`);
      yield* Ref.update(methods, (seen) => [...seen, request.method]);
      if (request.method === "PUT") {
        if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON update");

        const next = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json))(
          new TextDecoder().decode(request.body.body),
        ).pipe(Effect.orDie);

        yield* Ref.set(remote, next);
      } else expect(request.method).toBe("GET");

      return HttpClientResponse.fromWeb(
        request,
        Response.json({ success: true, result: yield* Ref.get(remote) }),
      );
    }),
  );

  const run = (command: typeof AccessCommand.Type, environment: AccessAdminEnvironment = env) =>
    manageAccess(command, environment).pipe(Effect.provideService(HttpClient.HttpClient, client));

  return { run, methods, remote };
});

it.effect(
  "lists, invites, and removes normalized members at the configured group without removing the administrator",
  () =>
    Effect.gen(function* () {
      const api = yield* makeApi(
        group([adminEmail.toUpperCase(), "Zed@Example.com", "zed@example.com"]),
      );

      expect(yield* api.run(list)).toEqual({ emails: [adminEmail, "zed@example.com"], adminEmail });
      expect(yield* api.run(invite)).toEqual({
        emails: [adminEmail, "friend@example.com", "zed@example.com"],
        adminEmail,
      });
      expect(yield* api.run(invite)).toEqual({
        emails: [adminEmail, "friend@example.com", "zed@example.com"],
        adminEmail,
      });
      expect(yield* api.run({ _tag: "Remove", email: "FRIEND@EXAMPLE.COM" })).toEqual({
        emails: [adminEmail, "zed@example.com"],
        adminEmail,
      });
      expect(yield* api.run({ _tag: "Remove", email: "absent@example.com" })).toEqual({
        emails: [adminEmail, "zed@example.com"],
        adminEmail,
      });
      expect(
        yield* api.run({ _tag: "Remove", email: adminEmail.toUpperCase() }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
      expect((yield* Ref.get(api.methods)).filter((method) => method === "PUT")).toHaveLength(2);
      expect(yield* api.run(list)).toEqual({ emails: [adminEmail, "zed@example.com"], adminEmail });

      expectTypeOf<Effect.Error<ReturnType<typeof manageAccess>>>().toEqualTypeOf<AccessError>();
      expectTypeOf<
        Effect.Services<ReturnType<typeof manageAccess>>
      >().toEqualTypeOf<HttpClient.HttpClient>();
    }),
);

it.effect("refuses unexpected or malformed remote policies before any update", () =>
  Effect.gen(function* () {
    for (const malformed of [
      null,
      { ...group(), id: "other-group" },
      { ...group(), name: "unrelated-policy" },
      group(["other@example.com"]),
      { ...group(), include: [] },
      { ...group(), include: [{ everyone: {} }] },
      { ...group(), include: [{ email: { email: adminEmail }, everyone: {} }] },
      { ...group(), include: [{ email: { email: adminEmail, unsupported: true } }] },
      { ...group(), include: [{ email: { email: "invalid" } }] },
      { ...group(), exclude: [{ email: { email: "excluded@example.com" } }] },
      { ...group(), require: [{ email_domain: { domain: "example.com" } }] },
    ]) {
      const api = yield* makeApi(malformed);

      expect(yield* api.run(invite).pipe(Effect.flip)).toMatchObject({ code: "unavailable" });
      expect(yield* Ref.get(api.methods)).toEqual(["GET"]);
      expect(yield* Ref.get(api.remote)).toEqual(malformed);
    }
  }),
);

it.effect(
  "fails closed on missing configuration and API errors without exposing credentials or error bodies",
  () =>
    Effect.gen(function* () {
      const api = yield* makeApi();

      for (const invalid of [
        {},
        { ...env, ACCESS_API_TOKEN: "" },
        { ...env, ACCESS_ACCOUNT_ID: "../other-account" },
        { ...env, ACCESS_GROUP_ID: "bad/group" },
      ])
        expect(yield* api.run(list, invalid).pipe(Effect.flip)).toMatchObject({
          code: "unavailable",
          message: "Invitations are not configured for this deployment. No access was changed.",
        });
      expect(yield* Ref.get(api.methods)).toEqual([]);

      for (const status of [200, 403, 500]) {
        const methods = yield* Ref.make<Array<string>>([]);

        const client = HttpClient.make((request) =>
          Ref.update(methods, (seen) => [...seen, request.method]).pipe(
            Effect.as(
              HttpClientResponse.fromWeb(
                request,
                Response.json(
                  {
                    success: false,
                    errors: [{ message: `private-response-body ${env.ACCESS_API_TOKEN}` }],
                  },
                  { status },
                ),
              ),
            ),
          ),
        );

        const error = yield* manageAccess(invite, env).pipe(
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.flip,
        );

        expect(error.code).toBe("unavailable");
        expect(JSON.stringify(error)).not.toContain(env.ACCESS_API_TOKEN);
        expect(JSON.stringify(error)).not.toContain("private-response-body");
        expect(yield* Ref.get(methods)).toEqual(["GET"]);
      }
    }),
);

it.effect(
  "bounds stalled requests and preserves caller interruption while finalizing HTTP work",
  () =>
    Effect.gen(function* () {
      for (const mode of ["timeout", "interrupt"] as const) {
        const started = yield* Deferred.make<void>();
        const finalized = yield* Ref.make(false);

        const client = HttpClient.make(() =>
          Effect.acquireUseRelease(
            Deferred.succeed(started, undefined),
            () => Effect.never,
            () => Ref.set(finalized, true),
          ),
        );

        const fiber = yield* manageAccess(invite, env).pipe(
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.forkChild,
        );

        yield* Deferred.await(started);
        if (mode === "timeout") {
          yield* TestClock.adjust("16 seconds");
          expect(yield* Fiber.join(fiber).pipe(Effect.flip)).toMatchObject({ code: "unavailable" });
        } else {
          yield* Fiber.interrupt(fiber);
          expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
        }
        expect(yield* Ref.get(finalized)).toBe(true);
      }
    }),
);

it.effect(
  "refreshes authoritative membership after an uncertain update and does not duplicate a retry",
  () =>
    Effect.gen(function* () {
      for (const point of ["before-update", "after-update"] as const) {
        const api = yield* makeApi();

        const failure = new AccessError({
          code: "unavailable",
          message: "Injected interruption around update",
        });

        const interrupted = yield* api.run(invite).pipe(
          Effect.provideService(AccessFailpoint, {
            hit: (current) => (current === point ? Effect.fail(failure) : Effect.void),
          }),
          Effect.flip,
        );

        expect(interrupted).toBe(failure);
        expect(yield* Ref.get(api.methods)).toEqual(
          point === "before-update" ? ["GET"] : ["GET", "PUT"],
        );
        expect((yield* api.run(list)).emails).toEqual(
          point === "before-update" ? [adminEmail] : [adminEmail, "friend@example.com"],
        );
        expect((yield* api.run(invite)).emails).toEqual([adminEmail, "friend@example.com"]);
        expect((yield* Ref.get(api.methods)).filter((method) => method === "PUT")).toHaveLength(1);
        expect((yield* api.run(list)).emails).toEqual([adminEmail, "friend@example.com"]);
      }
    }),
);

it.effect("enforces the membership limit and rejects unexpected confirmed update responses", () =>
  Effect.gen(function* () {
    const api = yield* makeApi(
      group([
        adminEmail,
        ...Array.from({ length: 199 }, (_, index) => `member${index}@example.com`),
      ]),
    );

    expect(yield* api.run(invite).pipe(Effect.flip)).toMatchObject({ code: "invalid" });
    expect(yield* Ref.get(api.methods)).toEqual(["GET"]);
    for (const saved of [
      { ...group([adminEmail, "friend@example.com"]), id: "other-group" },
      group(["friend@example.com"]),
    ]) {
      const methods = yield* Ref.make<Array<string>>([]);

      const client = HttpClient.make((request) =>
        Ref.update(methods, (seen) => [...seen, request.method]).pipe(
          Effect.as(
            HttpClientResponse.fromWeb(
              request,
              Response.json({ success: true, result: request.method === "GET" ? group() : saved }),
            ),
          ),
        ),
      );

      expect(
        yield* manageAccess(invite, env).pipe(
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.flip,
        ),
      ).toMatchObject({ code: "unavailable" });
      expect(yield* Ref.get(methods)).toEqual(["GET", "PUT"]);
    }
  }),
);
