import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { OAuthSignInAuthorization, OAuthRegistrationRequired } from "@yielded/auth/OAuth";
import { ProofRequestReceipt } from "@yielded/auth/Proofs";
import { Redacted, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

let mf: Miniflare;
let directory: string;
let githubExchanges = 0;
const githubName = "River Traveler";
let githubUserId = 424242;
let githubLogin = "fixture-traveler";
const githubIssuer = "https://github.com/login/oauth";

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
        ACCOUNT_THREADS: { className: "TravelPlannerThread", useSQLite: true },
        AUTH: { className: "AuthFixture", useSQLite: true },
      },
      durableObjectsPersist: directory,
      outboundService: async (request) => {
        if (request.url === "https://github.com/login/oauth/access_token") {
          githubExchanges++;

          if (new URLSearchParams(await request.text()).get("code") === "fixture-rejected-code")
            return Response.json({ error: "bad_verification_code" });

          return Response.json({
            access_token: "fixture-token",
            token_type: "bearer",
            scope: "read:user",
          });
        }
        if (request.url === "https://api.github.com/user")
          return Response.json({
            id: githubUserId,
            login: githubLogin,
            name: githubName,
            email: "reader@example.com",
            site_admin: true,
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
  expect((await verifyRegistration(client, code)).status).toBe(200);
  expect((await verifyRegistration(client, code)).status).toBe(400);
  expect(await client.call("getSession")).toBeNull();
});

const githubStart = async (client: ReturnType<typeof makeClient>) => {
  const started = Schema.decodeUnknownSync(OAuthSignInAuthorization)(
    await client.call("signIn", {
      provider: "github",
      returnTarget: "/",
    }),
  );

  const url = new URL(Redacted.value(started.authorizationUrl));

  return {
    flowId: started.flowId,
    provider: "github",
    callbackId: "github",
    response: {
      _tag: "Code",
      state: url.searchParams.get("state"),
      code: "fixture-code",
      issuer: githubIssuer,
    },
  };
};

it("rejects invalid and replayed GitHub callbacks before exchanging another token", async () => {
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
  const valid = await githubStart(client);

  await client.call("completeSignIn", valid);
  expect(githubExchanges).toBe(count + 1);
  expect((await client.raw("completeSignIn", valid)).status).toBe(400);
  expect(githubExchanges).toBe(count + 1);
});

it("keeps credential-bearing callback GET inert and rejects unauthorized requests", async () => {
  const client = makeClient();
  const count = githubExchanges;

  for (const path of ["/api/rpc"]) {
    const response = await client.request(path);

    expect(response.status).toBe(401);
    await response.arrayBuffer();
  }

  const callback = await client.request(
    "/auth/github/callback?code=PRIVATE_CODE&state=PRIVATE_STATE",
  );

  const callbackHtml = await callback.text();

  expect(callbackHtml).not.toMatch(/PRIVATE_CODE|PRIVATE_STATE/);
  expect(callback.headers.get("referrer-policy")).toBe("no-referrer");
  expect(callback.headers.get("cache-control")).toBe("no-store");
  expect(callback.headers.has("set-cookie")).toBe(false);
  expect(githubExchanges).toBe(count);
  const payload = JSON.stringify({ payload: { flowId: crypto.randomUUID() } });

  for (const headers of [
    new Headers({
      origin: "https://attacker.test",
      "x-effect-auth-csrf": "1",
      "content-type": "application/json",
    }),
  ]) {
    const response = await client.request("/auth/beginEmailSignIn", {
      method: "POST",
      headers,
      body: payload,
    });

    expect(response.status).toBe(403);
    await response.arrayBuffer();
  }
});

it("serves funding administration only to the verified owner and fences cross-origin and stale-account requests", async () => {
  const signInAs = async (id: number, login: string) => {
    githubUserId = id;
    githubLogin = login;
    const client = makeClient();

    const complete = async () => {
      const start = Schema.decodeUnknownSync(OAuthSignInAuthorization)(
        await client.call("signIn", {
          provider: "github",
          returnTarget: "/",
        }),
      );

      const result = await client.call("completeSignIn", {
        flowId: start.flowId,
        provider: "github",
        callbackId: "github",
        response: {
          _tag: "Code",
          state: new URL(Redacted.value(start.authorizationUrl)).searchParams.get("state"),
          code: "fixture-code",
          issuer: githubIssuer,
        },
      });

      return { flowId: start.flowId, result };
    };

    const first = await complete();

    if (Schema.is(OAuthRegistrationRequired)(first.result)) {
      await client.call("register", {
        flowId: first.flowId,
        commandId: crypto.randomUUID(),
        reference: first.result.reference,
        registration: { displayName: login },
      });
      await complete();
    }

    const session = Schema.decodeUnknownSync(Schema.Struct({ subjectId: Schema.String }))(
      await client.call("getSession"),
    );

    return { client, id: session.subjectId };
  };

  try {
    const owner = await signInAs(3450486, "danieljvdm");
    const reader = await signInAs(424242, "fixture-traveler");

    const request = (
      account: typeof owner,
      path: string,
      body?: object,
      origin = "https://planner.test",
      id = account.id,
    ) =>
      account.client.request(`/api/funding/${path}`, {
        method: body ? "POST" : "GET",
        headers: { origin, "x-elsewhere-account": id, "content-type": "application/json" },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });

    const grant = { kind: "account", value: reader.id };

    expect((await request(reader, "grant", grant)).status).toBe(400);
    expect((await request(owner, "grant", grant, "https://attacker.test")).status).toBe(400);
    expect((await request(owner, "grant", grant, "https://planner.test", reader.id)).status).toBe(
      400,
    );
    expect((await request(owner, "grant", grant)).status).toBe(200);
    expect((await request(owner, "revoke", { kind: "account", target: reader.id })).status).toBe(
      200,
    );
  } finally {
    githubUserId = 424242;
    githubLogin = "fixture-traveler";
  }
});
