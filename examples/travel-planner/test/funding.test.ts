import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

const admin = "00000000-0000-0000-0000-000000000001";
const email = "00000000-0000-0000-0000-000000000002";
let mf: Miniflare;
let directory: string;
let script: string;

const start = () =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { STORE: { className: "FundingFixture", useSQLite: true } },
      resourcePersistencePath: directory,
    }),
  );

beforeAll(async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/funding-worker.ts")],
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    logLevel: "silent",
  });

  script = bundle.outputFiles[0]!.text;
  directory = await mkdtemp(join(tmpdir(), "travel-funding-"));
  mf = start();
});
afterAll(async () => {
  await mf.dispose();
  await rm(directory, { recursive: true, force: true });
});

const request = (store: string, path: string, input?: object, actor = admin) =>
  mf.dispatchFetch(
    `https://fixture${path}${path.includes("?") ? "&" : "?"}store=${store}&actor=${actor}`,
    {
      method: input ? "POST" : "GET",
      ...(input
        ? { headers: { "content-type": "application/json" }, body: JSON.stringify(input) }
        : {}),
    },
  );

const json = async (store: string, path: string, input?: object, actor = admin) =>
  (await request(store, path, input, actor)).json();

it("resolves server credentials only while a current grant applies, including after object restart", async () => {
  await json("credentials", "/seed");
  expect((await request("credentials", "/resolve", undefined, email)).status).toBe(403);
  await json("credentials", "/grant", { kind: "email", value: "reader@gmail.com" });
  expect(await json("credentials", "/resolve", undefined, email)).toEqual({ lastFour: "9876" });
  await mf.dispose();
  mf = start();
  expect(await json("credentials", "/resolve", undefined, email)).toEqual({ lastFour: "9876" });
  await json("credentials", "/revoke", { kind: "email", target: "reader@gmail.com" });
  expect((await request("credentials", "/resolve", undefined, email)).status).toBe(403);
});

it("preserves committed grants and revocations across lost acknowledgements", async () => {
  await json("faults", "/seed");
  expect(
    (await request("faults", "/grant?point=grant:after", { kind: "account", value: email })).status,
  ).toBe(400);
  expect(await json("faults", "/status", undefined, email)).toMatchObject({ allowed: true });
  expect(
    (await request("faults", "/revoke?point=revoke:after", { kind: "account", target: email }))
      .status,
  ).toBe(400);
  expect(await json("faults", "/status", undefined, email)).toMatchObject({ allowed: false });
});
