import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { OAuthSignInAuthorization, OAuthRegistrationRequired } from "@yielded/auth/OAuth";
import { ProofRequestReceipt, ProofContinuation } from "@yielded/auth/Proofs";
import { Redacted, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import type { PlannerSettings } from "../src/domain";
import { defaultPlannerSettings } from "../src/domain";

let mf: Miniflare;
let directory: string;
let githubExchanges = 0;

beforeAll(async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/auth-worker.ts")],
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    alias: { "@tanstack/react-start/server-entry": join(import.meta.dirname, "fixtures/start.ts") },
    banner: {
      js: 'import { createRequire } from "node:module"; const require = createRequire("/fixture.mjs");',
    },
    logLevel: "silent",
  });

  directory = await mkdtemp(join(tmpdir(), "travel-auth-test-"));
  mf = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: bundle.outputFiles[0]!.text,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      r2Buckets: ["APP_BUILDS"],
      serviceBindings: { ASSETS: () => new Response("Fixture asset") },
      durableObjects: {
        THREADS: { className: "TravelPlannerThread", useSQLite: true },
        AUTH: { className: "AuthFixture", useSQLite: true },
        STORAGE: { className: "AuthStorageFixture", useSQLite: true },
      },
      durableObjectsPersist: directory,
      outboundService: async (request) => {
        if (request.url === "https://github.com/login/oauth/access_token") {
          githubExchanges++;

          return Response.json({
            access_token: "fixture-token",
            token_type: "bearer",
            scope: "read:user",
          });
        }
        if (request.url === "https://api.github.com/user")
          return Response.json({
            id: 424242,
            login: "fixture-traveler",
            email: "reader@example.com",
          });

        return new Response("Unexpected provider request", { status: 500 });
      },
    }),
  );
}, 30_000);
afterAll(async () => {
  await mf.dispose();
  await rm(directory, { recursive: true, force: true });
});

const makeClient = () => {
  const cookies = new Map<string, string>();

  const request = async (
    path: string,
    init?: { method?: string; headers?: HeadersInit; body?: string },
  ) => {
    const headers = new Headers(init?.headers);

    headers.set("cookie", [...cookies].map(([k, v]) => `${k}=${v}`).join("; "));

    const response = await mf.dispatchFetch(`https://planner.test${path}`, {
      ...init,
      redirect: "manual",
      headers: Object.fromEntries(headers),
    });

    for (const cookie of response.headers.getSetCookie()) {
      const pair = cookie.split(";", 1)[0]!;
      const split = pair.indexOf("=");

      if (pair.slice(split + 1) === "" || /max-age=0/i.test(cookie))
        cookies.delete(pair.slice(0, split));
      else cookies.set(pair.slice(0, split), pair.slice(split + 1));
    }

    return response;
  };

  const raw = async (name: string, payload?: object) => {
    const response = await request(`/auth/${name}`, {
      method: payload ? "POST" : "GET",
      headers: {
        origin: "https://planner.test",
        "content-type": "application/json",
        "x-effect-auth-csrf": "1",
      },
      ...(payload ? { body: JSON.stringify({ payload }) } : {}),
    });

    return { status: response.status, body: await response.json() };
  };

  const call = async (name: string, payload?: object) => {
    const response = await raw(name, payload);

    expect(response.status, JSON.stringify(response.body)).toBe(200);

    return Schema.decodeUnknownSync(
      Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
    )(response.body).value;
  };

  return {
    call,
    raw,
    request,
    cookie: () => [...cookies].map(([key, value]) => `${key}=${value}`).join("; "),
  };
};

const mail = async () =>
  Schema.decodeUnknownSync(
    Schema.Struct({ code: Schema.String, email: Schema.String, count: Schema.Number }),
  )(await (await mf.dispatchFetch("https://planner.test/_fixture/delivery")).json());

it("registers and signs in new and returning email and GitHub accounts through durable Auth HTTP actions", async () => {
  const client = makeClient();
  const { call } = client;

  expect(await call("getSession")).toBeNull();

  const base = {
    flowId: "email-register",
    email: "reader@example.com",
    registration: { displayName: "Reader" },
  };

  await call("beginEmailRegistration", { flowId: base.flowId });
  const sent = await call("registerEmail", { ...base, requestId: "register-one", locale: "en" });
  const receipt = Schema.decodeUnknownSync(Schema.toEncoded(ProofRequestReceipt))(sent);

  const mail = Schema.decodeUnknownSync(
    Schema.Struct({ code: Schema.String, email: Schema.String }),
  )(await (await mf.dispatchFetch("https://planner.test/_fixture/delivery")).json());

  expect(mail.email).toBe(base.email);

  const proof = Schema.decodeUnknownSync(
    Schema.toEncoded(Schema.Struct({ continuation: ProofContinuation })),
  )(
    await call("verifyEmailRegistration", {
      ...base,
      reference: receipt.reference,
      secret: mail.code,
    }),
  );

  expect(
    await call("completeEmailRegistration", {
      ...base,
      continuationId: proof.continuation.continuationId,
      commandId: "register-complete",
    }),
  ).toMatchObject({ _tag: "RegistrationAccepted" });
  expect(await call("getSession")).toBeNull();
  const login = { flowId: "email-sign-in", email: base.email, returnTarget: "/" };

  await call("beginEmailSignIn", { flowId: login.flowId });

  const code = Schema.decodeUnknownSync(Schema.toEncoded(ProofRequestReceipt))(
    await call("requestEmailCode", { ...login, requestId: "signin-one", locale: "en" }),
  );

  const loginMail = Schema.decodeUnknownSync(Schema.Struct({ code: Schema.String }))(
    await (await mf.dispatchFetch("https://planner.test/_fixture/delivery")).json(),
  );

  const verified = Schema.decodeUnknownSync(
    Schema.toEncoded(Schema.Struct({ continuation: ProofContinuation })),
  )(await call("verifyEmailCode", { ...login, reference: code.reference, secret: loginMail.code }));

  expect(
    await call("completeEmailSignIn", {
      ...login,
      continuationId: verified.continuation.continuationId,
    }),
  ).toMatchObject({
    completion: { _tag: "Authenticated", session: { claims: { displayName: "Reader" } } },
  });
  expect(await call("getSession")).toMatchObject({ claims: { displayName: "Reader" } });

  const emailAccount = Schema.decodeUnknownSync(Schema.Struct({ subjectId: Schema.String }))(
    await call("getSession"),
  );

  const privateRpc = async (subjectId: string, tag: string, payload: object | null = null) => {
    const response = await client.request("/api/rpc", {
      method: "POST",
      headers: {
        origin: "https://planner.test",
        "content-type": "application/ndjson",
        "x-elsewhere-account": subjectId,
      },
      body: JSON.stringify({ _tag: "Request", id: "1", tag, payload, headers: [] }) + "\n",
    });

    const body = await response.text();

    expect(response.status, body).toBe(200);

    return Schema.decodeUnknownSync(
      Schema.fromJsonString(
        Schema.Struct({
          exit: Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
        }),
      ),
    )(body.trim()).exit.value;
  };

  const preferences: PlannerSettings = {
    model: "gpt-6-astra",
    reasoningEffort: "high",
    fast: false,
  };

  expect(await privateRpc(emailAccount.subjectId, "SavePlannerSettings", preferences)).toEqual(
    preferences,
  );
  for (const path of ["/api/rpc", "/api/progress", "/api/voice"]) {
    const stale = await client.request(path, {
      method: "POST",
      headers: { origin: "https://planner.test", "x-elsewhere-account": "another-account" },
      body: "{}",
    });

    expect(stale.status).toBe(409);
    await stale.arrayBuffer();
  }
  expect((await client.request("/api/access")).status).toBe(404);
  await call("signOut", {});

  const githubStart = async (flowId: string) => {
    const start = Schema.decodeUnknownSync(OAuthSignInAuthorization)(
      await call("signIn", {
        flowId,
        commandId: flowId,
        provider: "github",
        callbackId: "github",
        returnTarget: "/",
      }),
    );

    const authorization = new URL(Redacted.value(start.authorizationUrl));

    return {
      flowId,
      provider: "github",
      callbackId: "github",
      response: {
        _tag: "Code",
        state: authorization.searchParams.get("state"),
        code: "fixture-code",
      },
    };
  };

  const github = await githubStart("github-register");

  const registration = Schema.decodeUnknownSync(OAuthRegistrationRequired)(
    await call("completeSignIn", github),
  );

  expect(
    await call("register", {
      flowId: github.flowId,
      commandId: "github-provision",
      reference: registration.reference,
      registration: { displayName: "GitHub traveler" },
    }),
  ).toMatchObject({ _tag: "RegistrationAccepted" });
  expect(await call("getSession")).toBeNull();
  expect(await call("completeSignIn", await githubStart("github-signin"))).toMatchObject({
    completion: { _tag: "Authenticated", session: { claims: { displayName: "GitHub traveler" } } },
  });
  expect(await call("getSession")).toMatchObject({ claims: { displayName: "GitHub traveler" } });

  const githubAccount = Schema.decodeUnknownSync(Schema.Struct({ subjectId: Schema.String }))(
    await call("getSession"),
  );

  expect(githubAccount.subjectId).not.toBe(emailAccount.subjectId);
  expect(await privateRpc(githubAccount.subjectId, "GetPlannerSettings")).toEqual(
    defaultPlannerSettings,
  );
  await call("signOut", {});
  expect(await call("getSession")).toBeNull();
}, 30_000);

const registrationCode = async (client: ReturnType<typeof makeClient>, email: string) => {
  const input = {
    flowId: crypto.randomUUID(),
    email,
    registration: { displayName: "Fixture traveler" },
  };

  await client.call("beginEmailRegistration", { flowId: input.flowId });
  const requestId = crypto.randomUUID();

  const receipt = Schema.decodeUnknownSync(Schema.toEncoded(ProofRequestReceipt))(
    await client.call("registerEmail", { ...input, requestId, locale: "en" }),
  );

  return { input, requestId, receipt, delivered: await mail() };
};

const verifyRegistration = (
  client: ReturnType<typeof makeClient>,
  code: Awaited<ReturnType<typeof registrationCode>>,
  secret = code.delivered.code,
) =>
  client.raw("verifyEmailRegistration", {
    ...code.input,
    reference: code.receipt.reference,
    secret,
  });

it("binds email proofs to the initiating browser and consumes successful codes only once", async () => {
  const client = makeClient();
  const code = await registrationCode(client, "proof@example.com");

  expect((await verifyRegistration(makeClient(), code)).status).toBe(400);
  expect((await verifyRegistration(client, code, "not-a-code")).status).toBe(400);
  expect((await verifyRegistration(client, code)).status).toBe(200);
  expect((await verifyRegistration(client, code)).status).toBe(400);
  expect(await client.call("getSession")).toBeNull();
});

it("enforces expiry, attempt budgets, request deduplication and resend cooldown without leaking account existence", async () => {
  const client = makeClient();
  const code = await registrationCode(client, "limits@example.com");

  await client.call("registerEmail", { ...code.input, requestId: code.requestId, locale: "en" });
  expect((await mail()).count).toBe(code.delivered.count);
  await client.call("registerEmail", {
    ...code.input,
    requestId: crypto.randomUUID(),
    locale: "en",
  });
  expect((await mail()).count).toBe(code.delivered.count);
  const wrong = code.delivered.code === "000000" ? "111111" : "000000";

  for (let i = 0; i < 5; i++)
    expect((await verifyRegistration(client, code, wrong)).status).toBe(400);
  expect((await verifyRegistration(client, code)).status).toBe(400);
  const expired = await registrationCode(client, "expired@example.com");

  await mf.dispatchFetch("https://planner.test/_fixture/expire");
  expect((await verifyRegistration(client, expired)).status).toBe(400);
  expect(await client.call("getSession")).toBeNull();
});

it("does not automatically repeat an ambiguous email delivery or establish a session", async () => {
  const client = makeClient();
  const count = (await mail()).count;

  await mf.dispatchFetch("https://planner.test/_fixture/fail-delivery");
  try {
    const input = {
      flowId: crypto.randomUUID(),
      email: "delivery@example.com",
      registration: { displayName: "Fixture" },
      requestId: crypto.randomUUID(),
      locale: "en",
    };

    await client.call("beginEmailRegistration", { flowId: input.flowId });
    await client.call("registerEmail", input);
    await client.call("registerEmail", input);
    expect((await mail()).count).toBe(count);
    expect(await client.call("getSession")).toBeNull();
  } finally {
    await mf.dispatchFetch("https://planner.test/_fixture/fail-delivery?enabled=false");
  }
});

const githubStart = async (client: ReturnType<typeof makeClient>) => {
  const flowId = crypto.randomUUID();

  const started = Schema.decodeUnknownSync(OAuthSignInAuthorization)(
    await client.call("signIn", {
      flowId,
      commandId: crypto.randomUUID(),
      provider: "github",
      callbackId: "github",
      returnTarget: "/",
    }),
  );

  const url = new URL(Redacted.value(started.authorizationUrl));

  expect(url.searchParams.get("redirect_uri")).toBe("https://planner.test/auth/github/callback");
  expect(url.searchParams.get("scope") ?? "").not.toMatch(/repo|mail/);

  return {
    flowId,
    provider: "github",
    callbackId: "github",
    response: { _tag: "Code", state: url.searchParams.get("state"), code: "fixture-code" },
  };
};

it("rejects invalid and replayed GitHub callbacks, and handles denial without exchanging a token", async () => {
  const client = makeClient();
  const count = githubExchanges;
  const invalid = await githubStart(client);

  expect(
    (
      await client.raw("completeSignIn", {
        ...invalid,
        response: { ...invalid.response, state: "wrong-state" },
      })
    ).status,
  ).toBe(400);
  expect((await makeClient().raw("completeSignIn", invalid)).status).toBe(400);
  expect(githubExchanges).toBe(count);
  const denied = await githubStart(client);

  expect(
    await client.call("completeSignIn", {
      ...denied,
      response: { _tag: "Error", state: denied.response.state, error: "access-denied" },
    }),
  ).toMatchObject({ _tag: "Cancelled" });
  expect(githubExchanges).toBe(count);
  const valid = await githubStart(client);

  await client.call("completeSignIn", valid);
  expect(githubExchanges).toBe(count + 1);
  expect((await client.raw("completeSignIn", valid)).status).toBe(400);
  expect(githubExchanges).toBe(count + 1);
  expect(
    (
      await client.raw("signIn", {
        flowId: crypto.randomUUID(),
        commandId: crypto.randomUUID(),
        provider: "github",
        callbackId: "github",
        returnTarget: "https://attacker.test",
      })
    ).status,
  ).toBe(400);
});

it("keeps callback GET inert, exposes only login assets, and rejects unauthenticated private APIs", async () => {
  const client = makeClient();
  const count = githubExchanges;

  for (const path of ["/api/rpc", "/api/access", "/api/progress", "/api/voice", "/trips/trip/1"]) {
    const response = await client.request(path);

    expect(response.status).toBe(401);
    await response.arrayBuffer();
  }
  const home = await client.request("/");

  expect(home.status).toBe(303);
  expect(home.headers.get("location")).toBe("/login");
  for (const path of [
    "/login",
    "/auth/github/callback?code=PRIVATE_CODE&state=PRIVATE_STATE",
    "/favicon.svg",
    "/assets/login.js",
  ]) {
    const response = await client.request(path);

    expect(response.status).toBe(200);
    expect(await response.text()).not.toMatch(/PRIVATE_CODE|PRIVATE_STATE/);
    expect(response.headers.has("set-cookie")).toBe(false);
  }
  expect(githubExchanges).toBe(count);
  const payload = JSON.stringify({ payload: { flowId: crypto.randomUUID() } });

  for (const headers of [
    new Headers({
      origin: "https://attacker.test",
      "x-effect-auth-csrf": "1",
      "content-type": "application/json",
    }),
    new Headers({ origin: "https://planner.test", "content-type": "application/json" }),
  ]) {
    const response = await client.request("/auth/beginEmailSignIn", {
      method: "POST",
      headers,
      body: payload,
    });

    expect(response.status).toBe(403);
    await response.arrayBuffer();
  }

  const large = await client.request("/auth/beginEmailSignIn", {
    method: "POST",
    headers: {
      origin: "https://planner.test",
      "x-effect-auth-csrf": "1",
      "content-type": "application/json",
    },
    body: "x".repeat(33 * 1024),
  });

  expect(large.status).toBe(413);
  await large.arrayBuffer();
});

it("initializes auth storage atomically, survives lost replies, and fails closed on unsupported formats", async () => {
  const inspect = async (id: string, mode = "") =>
    Schema.decodeUnknownSync(
      Schema.Struct({
        outcome: Schema.Literals(["Success", "Failure"]),
        tables: Schema.Array(Schema.Struct({ name: Schema.String })),
      }),
    )(
      await (
        await mf.dispatchFetch(`https://planner.test/_fixture/storage?id=${id}&mode=${mode}`)
      ).json(),
    );

  for (const mode of ["schema:before", "defect", "interrupt", "timeout"]) {
    const failed = await inspect(mode, mode);

    expect(failed).toEqual({ outcome: "Failure", tables: [] });
    expect((await inspect(mode)).outcome).toBe("Success");
  }
  const lost = await inspect("lost", "schema:after");

  expect(lost.outcome).toBe("Failure");
  expect(lost.tables).toHaveLength(21);
  expect((await inspect("lost")).tables).toEqual(lost.tables);
  const rejected = await inspect("lost", "unsupported");

  expect(rejected.outcome).toBe("Failure");
  expect(rejected.tables).toEqual(lost.tables);
  expect((await inspect("lost")).outcome).toBe("Failure");
});
