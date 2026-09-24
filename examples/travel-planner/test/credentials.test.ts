import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { OpenAiConnection } from "../src/credential-domain.ts";
import { ownerEmail } from "./fixtures/identity.ts";

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
        BYOK_ENCRYPTION_KEY: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
      },
      r2Buckets: ["APP_BUILDS"],
      durableObjects: { ACCOUNT_THREADS: { className: "TravelPlannerThread", useSQLite: true } },
      resourcePersistencePath: directory,
      outboundService: async (request) => {
        expect(request.url).toBe("https://api.openai.com/v1/models");
        const key = request.headers.get("authorization") ?? "";

        return new Response("", {
          status: key.includes("invalid") ? 401 : key.includes("redirect") ? 302 : 200,
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

const rpcExit = async (tag: string, payload?: unknown, email = ownerEmail) => {
  const response = await runtime.dispatchFetch("http://planner/api/rpc", {
    method: "POST",
    headers: { ...headers(email), "content-type": "application/ndjson" },
    body: `${JSON.stringify({ _tag: "Request", id: "1", tag, payload: payload ?? null, headers: [] })}\n`,
  });

  const body = await response.text();

  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");

  return Schema.decodeSync(Schema.fromJsonString(RpcExit))(body.trim().split("\n")[0]).exit;
};

const rpc = async (tag: string, payload?: unknown, email = ownerEmail) => {
  const result = await rpcExit(tag, payload, email);

  if (result._tag === "Failure") throw new Error(JSON.stringify(result.cause));

  return result.value;
};

const get = async (email = ownerEmail) =>
  Schema.decodeUnknownSync(OpenAiConnection)(await rpc("GetOpenAiConnection", undefined, email));

const save = async (apiKey: string, email = ownerEmail) =>
  Schema.decodeUnknownSync(OpenAiConnection)(await rpc("ConnectOpenAi", { apiKey }, email));

const resolve = async (email = ownerEmail) => {
  const response = await runtime.dispatchFetch("http://planner/__test/credentials?resolve", {
    headers: headers(email),
  });

  return response.json();
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

it("encrypts separate account keys and resolves them after restart", async () => {
  const guest = "friend@example.com";

  expect(await save(first)).toMatchObject({ connected: true, lastFour: "1111" });
  expect(await save(second, guest)).toMatchObject({ connected: true, lastFour: "2222" });
  const saved = await raw(ownerEmail);

  expect(saved).toHaveLength(1);
  expect(JSON.stringify(saved)).not.toContain(first);
  expect(JSON.stringify(saved)).not.toContain("fixture-private");
  await runtime.dispose();
  runtime = makeRuntime();
  expect(await resolve()).toEqual({ lastFour: "1111" });
  expect(await resolve(guest)).toEqual({ lastFour: "2222" });
}, 30_000);

it("preserves the previous key on validation failure and never follows validation redirects", async () => {
  const email = "validation@example.com";

  await save(first, email);
  for (const apiKey of ["sk-fixture-invalid-PRIVATE", "sk-fixture-redirect-PRIVATE"]) {
    const result = await rpcExit("ConnectOpenAi", { apiKey }, email);

    expect(result._tag).toBe("Failure");
    expect(JSON.stringify(result)).not.toContain("PRIVATE");
    expect(await resolve(email)).toEqual({ lastFour: "1111" });
  }
}, 30_000);

it("refuses to decrypt an encrypted key copied from another owner", async () => {
  const email = "corrupt@example.com";

  await save(first, "source@example.com");
  const copied = (await raw("source@example.com"))[0]?.value;

  if (!copied) throw new Error("Missing fixture row");
  await get(email);
  await raw(email, copied);
  expect(await resolve(email)).toHaveProperty("error");
}, 30_000);
