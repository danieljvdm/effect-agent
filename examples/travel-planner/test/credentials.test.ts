import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { Effect } from "effect";
import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, expectTypeOf, it } from "vite-plus/test";

import { adminEmail } from "../src/access-domain.ts";
import { OpenAiConnection } from "../src/credential-domain.ts";
import type { PlannerError } from "../src/domain.ts";
import { PlannerSnapshot } from "../src/domain.ts";
import type { CredentialStore } from "../src/server/credentials.ts";

const token = "preference-test-token";
const first = "sk-proj-fixture-private-first-1111";
const second = "sk-proj-fixture-private-second-2222";

const RpcExit = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  exit: Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
    Schema.Struct({ _tag: Schema.Literal("Failure"), cause: Schema.Unknown }),
  ]),
});

let directory: string;
let worker: string;
let runtime: Miniflare;
let demoKey = "sk-fixture-shared-demo-3333";

const makeRuntime = () =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: worker,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      bindings: {
        PLANNER_TOKEN: token,
        ACCESS_OPEN_REGISTRATION: "true",
        DEMO_OPENAI_API_KEY: demoKey,
        BYOK_ENCRYPTION_KEY: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
      },
      r2Buckets: ["APP_BUILDS"],
      durableObjects: { THREADS: { className: "TravelPlannerThread", useSQLite: true } },
      resourcePersistencePath: directory,
      outboundService: async (request) => {
        expect(request.url).toBe("https://api.openai.com/v1/models");
        const key = request.headers.get("authorization") ?? "";

        return new Response("", {
          status: key.includes("invalid")
            ? 401
            : key.includes("busy")
              ? 429
              : key.includes("redirect")
                ? 302
                : 200,
          headers: key.includes("redirect") ? { location: "https://must-not-follow.test" } : {},
        });
      },
    }),
  );

beforeAll(async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/credentials-worker.ts")],
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

  const output = bundle.outputFiles[0];

  if (output === undefined) throw new Error("No worker bundle");
  worker = output.text;
  directory = await mkdtemp(join(tmpdir(), "planner-credentials-test-"));
  runtime = makeRuntime();
});

afterAll(async () => {
  await runtime?.dispose();
  if (directory !== undefined) await rm(directory, { recursive: true, force: true });
});

const headers = (email: string) => ({ authorization: `Bearer ${token}`, "x-test-email": email });

const rpcExit = async (tag: string, payload?: unknown, email = adminEmail) => {
  const response = await runtime.dispatchFetch(
    `http://planner/api/${["GetDemoAccess", "GrantDemoAccess", "RevokeDemoAccess"].includes(tag) ? "access" : "rpc"}`,
    {
      method: "POST",
      headers: { ...headers(email), "content-type": "application/ndjson" },
      body: `${JSON.stringify({ _tag: "Request", id: "1", tag, payload: payload ?? null, headers: [] })}\n`,
    },
  );

  const body = await response.text();

  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");

  return Schema.decodeUnknownSync(Schema.fromJsonString(RpcExit))(body.trim().split("\n")[0]).exit;
};

const rpc = async (tag: string, payload?: unknown, email = adminEmail) => {
  const result = await rpcExit(tag, payload, email);

  if (result._tag === "Failure") throw new Error(JSON.stringify(result.cause));

  return result.value;
};

const get = async (email = adminEmail) =>
  Schema.decodeUnknownSync(OpenAiConnection)(await rpc("GetOpenAiConnection", undefined, email));

const save = async (apiKey: string, email = adminEmail) =>
  Schema.decodeUnknownSync(OpenAiConnection)(await rpc("ConnectOpenAi", { apiKey }, email));

const remove = async (email = adminEmail) =>
  Schema.decodeUnknownSync(OpenAiConnection)(await rpc("DisconnectOpenAi", undefined, email));

const resolve = async (email = adminEmail) => {
  const response = await runtime.dispatchFetch("http://planner/__test/credentials?resolve", {
    headers: headers(email),
  });

  return response.json();
};

const arm = async (point: string, mode = "failure") => {
  const response = await runtime.dispatchFetch(
    `http://planner/__test/credentials?point=${point}&mode=${mode}`,
    { headers: headers(adminEmail) },
  );

  await response.arrayBuffer();
  expect(response.status).toBe(200);
};

const raw = async (email: string, value?: string) => {
  const response = await runtime.dispatchFetch("http://planner/__test/credentials", {
    method: value === undefined ? "GET" : "PUT",
    headers: headers(email),
    ...(value === undefined ? {} : { body: value }),
  });

  return Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ value: Schema.String })))(
    await response.json(),
  );
};

it("encrypts account keys, resolves them after restart, rotates and removes without touching trips", async () => {
  const guest = "friend@example.com";

  expect(await get()).toMatchObject({ connected: false, lastFour: null });
  expect(await raw(adminEmail)).toEqual([]);
  await rpc("SaveTrip", {
    conversationId: "existing-trip",
    tripId: null,
    expectedRevision: null,
    title: "Retained trip",
    destination: "Lisbon",
    summary: "A stored draft",
    startDate: null,
    endDate: null,
    travelers: 1,
    days: [],
    notes: ["Preserve this trip"],
  });

  const before = Schema.decodeUnknownSync(PlannerSnapshot)(
    await rpc("GetPlanner", { conversationId: null }),
  );

  expect(await save(first)).toMatchObject({ connected: true, lastFour: "1111" });
  expect(await get(guest)).toMatchObject({ connected: false });
  expect(await save(second, guest)).toMatchObject({ connected: true, lastFour: "2222" });
  const saved = await raw(adminEmail);

  expect(saved).toHaveLength(1);
  expect(JSON.stringify(saved)).not.toContain(first);
  expect(JSON.stringify(saved)).not.toContain("fixture-private");
  await runtime.dispose();
  runtime = makeRuntime();
  expect(await resolve()).toEqual({ lastFour: "1111" });
  expect(await resolve(guest)).toEqual({ lastFour: "2222" });
  expect(await rpc("GetPlanner", { conversationId: null })).toEqual(before);
  await save(second);
  expect(await resolve()).toEqual({ lastFour: "2222" });
  await remove();
  expect(await raw(adminEmail)).toEqual([]);
  expect(await resolve()).toMatchObject({
    error: "Connect your OpenAI API key in Settings to continue planning.",
  });
  expect(await resolve(guest)).toEqual({ lastFour: "2222" });
  expect(await rpc("GetPlanner", { conversationId: null })).toEqual(before);
  expect(await remove()).toMatchObject({ connected: false });
}, 30_000);

it("preserves the previous key on validation failure and never follows validation redirects", async () => {
  const email = "validation@example.com";

  await save(first, email);
  for (const apiKey of [
    "bad",
    "sk-fixture-invalid-PRIVATE",
    "sk-fixture-busy-PRIVATE",
    "sk-fixture-redirect-PRIVATE",
  ]) {
    const result = await rpcExit("ConnectOpenAi", { apiKey }, email);

    expect(result._tag).toBe("Failure");
    expect(JSON.stringify(result)).not.toContain("PRIVATE");
    expect(await resolve(email)).toEqual({ lastFour: "1111" });
  }
}, 30_000);

it("recovers after interrupted credential schema initialization", async () => {
  for (const point of ["schema:before", "schema:after"]) {
    for (const mode of ["failure", "defect", "interrupt"]) {
      const email = `${point.replace(":", "-")}-${mode}@example.com`;

      await arm(point, mode);

      const response = await runtime.dispatchFetch("http://planner/api/rpc", {
        method: "POST",
        headers: { ...headers(email), "content-type": "application/ndjson" },
        body: `${JSON.stringify({ _tag: "Request", id: "1", tag: "GetOpenAiConnection", payload: null, headers: [] })}\n`,
      });

      const body = await response.text();

      expect(response.ok && body.includes('"_tag":"Success"')).toBe(false);
      await runtime.dispose();
      runtime = makeRuntime();
      expect(await get(email)).toMatchObject({ connected: false });
      expect(await save(first, email)).toMatchObject({ connected: true, lastFour: "1111" });
    }
  }
}, 30_000);

it("handles faults before and after durable writes without exposing key material", async () => {
  const email = "failure@example.com";

  for (const mode of ["failure", "defect", "interrupt"]) {
    await save(first, email);
    await arm("save:before", mode);
    expect((await rpcExit("ConnectOpenAi", { apiKey: second }, email))._tag).toBe("Failure");
    expect(await resolve(email)).toEqual({ lastFour: "1111" });
    await arm("save:after", mode);
    expect((await rpcExit("ConnectOpenAi", { apiKey: second }, email))._tag).toBe("Failure");
    expect(await resolve(email)).toEqual({ lastFour: "2222" });
    await arm("remove:before", mode);
    expect((await rpcExit("DisconnectOpenAi", undefined, email))._tag).toBe("Failure");
    expect(await resolve(email)).toEqual({ lastFour: "2222" });
    await arm("remove:after", mode);
    expect((await rpcExit("DisconnectOpenAi", undefined, email))._tag).toBe("Failure");
    expect(await get(email)).toMatchObject({ connected: false });
  }
}, 30_000);

it("binds encrypted keys to their owner and rejects malformed records without replacing them", async () => {
  const email = "corrupt@example.com";

  await save(first, "source@example.com");
  const copied = (await raw("source@example.com"))[0]?.value;

  if (!copied) throw new Error("Missing fixture row");
  await get(email);
  await raw(email, copied);
  expect(await resolve(email)).toHaveProperty("error");
  for (const value of ["not-json-PRIVATE", JSON.stringify({ version: 99 })]) {
    await raw(email, value);
    for (const result of [
      await rpcExit("GetOpenAiConnection", undefined, email),
      await rpcExit("ConnectOpenAi", { apiKey: second }, email),
      await rpcExit("DisconnectOpenAi", undefined, email),
    ]) {
      expect(result._tag).toBe("Failure");
      expect(JSON.stringify(result)).not.toContain("PRIVATE");
    }
    expect(await raw(email)).toEqual([{ value }]);
  }
}, 30_000);

it("requires authentication and rejects cross-origin requests before reaching credentials", async () => {
  for (const [extra, expected] of [
    [{}, 401],
    [{ ...headers(adminEmail), origin: "https://attacker.test" }, 403],
  ] as const) {
    const response = await runtime.dispatchFetch("http://planner/api/rpc", {
      method: "POST",
      headers: extra,
    });

    await response.arrayBuffer();
    expect(response.status).toBe(expected);
  }
  expectTypeOf<Effect.Error<CredentialStore["Service"]["status"]>>().toEqualTypeOf<PlannerError>();
  expectTypeOf<
    Effect.Services<ReturnType<CredentialStore["Service"]["save"]>>
  >().toEqualTypeOf<never>();
});

it("grants shared funding only to allowlisted accounts, preserves personal keys, and rechecks revocation after restart", async () => {
  const guest = "sponsored@example.com";

  expect(await rpc("GetDemoAccess")).toEqual({ emails: [], configured: true });
  expect(await get(guest)).toMatchObject({ connected: false });
  expect(await resolve(guest)).toHaveProperty("error");
  for (const tag of ["GetDemoAccess", "GrantDemoAccess", "RevokeDemoAccess"])
    expect(
      await rpcExit(tag, tag === "GetDemoAccess" ? undefined : { email: guest }, guest),
    ).toMatchObject({ _tag: "Failure" });

  await rpc("GrantDemoAccess", { email: "SPONSORED@example.com" });
  expect(await rpc("GrantDemoAccess", { email: guest })).toEqual({
    emails: [guest],
    configured: true,
  });
  expect(await get(guest)).toEqual({
    connected: true,
    source: "demo",
    lastFour: null,
    updatedAt: null,
  });
  expect(await resolve(guest)).toEqual({ lastFour: "3333" });
  expect(await get("unlisted@example.com")).toMatchObject({ connected: false });
  expect(await resolve("unlisted@example.com")).toHaveProperty("error");

  await save(first, guest);
  expect(await get(guest)).toMatchObject({ connected: true, lastFour: "1111" });
  expect(await resolve(guest)).toEqual({ lastFour: "1111" });
  expect(await remove(guest)).toMatchObject({ connected: true, source: "demo" });
  await runtime.dispose();
  demoKey = "sk-fixture-shared-demo-4444";
  runtime = makeRuntime();
  expect(await get(guest)).toMatchObject({ source: "demo" });
  expect(await resolve(guest)).toEqual({ lastFour: "4444" });

  await rpc("RevokeDemoAccess", { email: guest });
  expect(await get(guest)).toMatchObject({ connected: false });
  expect(await resolve(guest)).toHaveProperty("error");
  await rpc("GrantDemoAccess", { email: guest });
  await save(first, guest);
  await rpc("RevokeDemoAccess", { email: guest });
  expect(await resolve(guest)).toEqual({ lastFour: "1111" });
  await runtime.dispose();
  demoKey = "";
  runtime = makeRuntime();
  await rpc("GrantDemoAccess", { email: "no-demo-key@example.com" });
  expect(await rpc("GetDemoAccess")).toMatchObject({ configured: false });
  expect(await resolve("no-demo-key@example.com")).toHaveProperty("error");
  await rpc("RevokeDemoAccess", { email: "no-demo-key@example.com" });
  await runtime.dispose();
  demoKey = "sk-fixture-shared-demo-3333";
  runtime = makeRuntime();
}, 30_000);

const armDemo = async (point: string, mode: string) => {
  const response = await runtime.dispatchFetch(
    `http://planner/__test/credentials?demo-point=${point}&mode=${mode}`,
    { headers: headers(adminEmail) },
  );

  await response.arrayBuffer();
  expect(response.status).toBe(200);
};

it("recovers uncertain demo-access writes without widening access or losing revocations", async () => {
  const guest = "demo-failures@example.com";

  for (const mode of ["failure", "defect", "interrupt"]) {
    await rpc("RevokeDemoAccess", { email: guest });
    await armDemo("save:before", mode);
    expect((await rpcExit("GrantDemoAccess", { email: guest }))._tag).toBe("Failure");
    expect(await resolve(guest)).toHaveProperty("error");
    await armDemo("save:after", mode);
    expect((await rpcExit("GrantDemoAccess", { email: guest }))._tag).toBe("Failure");
    await runtime.dispose();
    runtime = makeRuntime();
    expect(await resolve(guest)).toEqual({ lastFour: "3333" });
    await armDemo("save:before", mode);
    expect((await rpcExit("RevokeDemoAccess", { email: guest }))._tag).toBe("Failure");
    expect(await resolve(guest)).toEqual({ lastFour: "3333" });
    await armDemo("save:after", mode);
    expect((await rpcExit("RevokeDemoAccess", { email: guest }))._tag).toBe("Failure");
    await runtime.dispose();
    runtime = makeRuntime();
    expect(await resolve(guest)).toHaveProperty("error");
  }
}, 30_000);

it("fails closed on unreadable grants or personal credentials and preserves unsupported access data", async () => {
  const guest = "demo-corruption@example.com";

  await rpc("GrantDemoAccess", { email: guest });
  await get(guest);
  await raw(guest, "unsupported-private-credential");
  expect(await resolve(guest)).toHaveProperty("error");

  const row = async (value?: string) => {
    const response = await runtime.dispatchFetch("http://planner/__test/credentials?demo-row", {
      method: value === undefined ? "GET" : "PUT",
      headers: headers(adminEmail),
      ...(value === undefined ? {} : { body: value }),
    });

    return Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ value: Schema.String })))(
      await response.json(),
    );
  };

  const before = await row();

  if (!before[0]) throw new Error("Missing access fixture");
  try {
    await row(JSON.stringify({ version: 99, emails: ["unlisted@example.com"] }));
    for (const tag of ["GetDemoAccess", "GrantDemoAccess", "RevokeDemoAccess"])
      expect(
        (await rpcExit(tag, tag === "GetDemoAccess" ? undefined : { email: guest }))._tag,
      ).toBe("Failure");
    expect(await resolve("unlisted@example.com")).toHaveProperty("error");
    expect((await row())[0]?.value).toContain('"version":99');
    for (const point of ["schema:before", "schema:after"]) {
      await row(before[0].value);
      for (const mode of ["failure", "defect", "interrupt"]) {
        await armDemo(point, mode);
        expect((await rpcExit("GetDemoAccess"))._tag).toBe("Failure");
        expect(await rpc("GetDemoAccess")).toMatchObject({ configured: true });
      }
    }
  } finally {
    await row(before[0].value);
    await rpc("RevokeDemoAccess", { email: guest });
  }
}, 30_000);
