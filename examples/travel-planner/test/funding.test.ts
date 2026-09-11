import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

const admin = "00000000-0000-0000-0000-000000000001";
const email = "00000000-0000-0000-0000-000000000002";
const github = "00000000-0000-0000-0000-000000000003";
const impostor = "00000000-0000-0000-0000-000000000004";
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
      outboundService: (request) =>
        request.url === "https://api.github.com/users/reader"
          ? Response.json({ id: 424242, login: "reader" })
          : new Response(null, { status: 404 }),
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

it("authorizes only the immutable administrator identity and never display names or ordinary accounts", async () => {
  await json("authority", "/seed");
  expect(await json("authority", "/status")).toEqual({ admin: true, allowed: true });
  expect(await json("authority", "/status", undefined, impostor)).toEqual({
    admin: false,
    allowed: false,
  });
  for (const actor of [email, github, impostor]) {
    expect((await request("authority", "/list", undefined, actor)).status).toBe(400);
    expect(
      (await request("authority", "/grant", { kind: "account", value: actor }, actor)).status,
    ).toBe(400);
    expect(
      (await request("authority", "/revoke", { kind: "account", target: actor }, actor)).status,
    ).toBe(400);
  }
  expect(await json("authority", "/list")).toMatchObject({
    grants: [],
    users: expect.arrayContaining([
      expect.objectContaining({ subjectId: email, emails: ["reader@gmail.com"] }),
    ]),
  });
});

it("matches verified email, resolves GitHub usernames to stable IDs, and applies overlapping grants independently", async () => {
  await json("matching", "/seed");
  expect(
    await json("matching", "/grant", { kind: "email", value: " Reader@GMAIL.com " }),
  ).toMatchObject({ target: "reader@gmail.com" });
  expect(await json("matching", "/status", undefined, email)).toMatchObject({ allowed: true });
  expect(await json("matching", "/grant", { kind: "github", value: "@reader" })).toMatchObject({
    target: "424242",
    label: "reader",
  });
  expect(await json("matching", "/status", undefined, github)).toMatchObject({ allowed: true });
  expect((await request("matching", "/grant", { kind: "github", value: "missing" })).status).toBe(
    400,
  );
  await json("matching", "/grant", { kind: "account", value: email });
  await json("matching", "/revoke", { kind: "email", target: "reader@gmail.com" });
  expect(await json("matching", "/status", undefined, email)).toMatchObject({ allowed: true });
  await json("matching", "/revoke", { kind: "account", target: email });
  expect(await json("matching", "/status", undefined, email)).toMatchObject({ allowed: false });
  await json("matching", "/disable", undefined, github);
  expect(await json("matching", "/status", undefined, github)).toMatchObject({ allowed: false });
});

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

it("preserves grants across lost acknowledgements and rejects unsupported stored records without mutation", async () => {
  await json("faults", "/seed");
  for (const mode of ["failure", "defect", "interrupt", "timeout"]) {
    expect(
      (
        await request("faults", `/grant?point=grant:before&mode=${mode}`, {
          kind: "account",
          value: email,
        })
      ).status,
    ).toBe(400);
    expect(await json("faults", "/status", undefined, email)).toMatchObject({ allowed: false });
  }
  expect(
    (await request("faults", "/grant?point=grant:after", { kind: "account", value: email })).status,
  ).toBe(400);
  expect(await json("faults", "/status", undefined, email)).toMatchObject({ allowed: true });
  expect(
    (await request("faults", "/revoke?point=revoke:before", { kind: "account", target: email }))
      .status,
  ).toBe(400);
  expect(await json("faults", "/status", undefined, email)).toMatchObject({ allowed: true });
  expect(
    (await request("faults", "/revoke?point=revoke:after", { kind: "account", target: email }))
      .status,
  ).toBe(400);
  expect(await json("faults", "/status", undefined, email)).toMatchObject({ allowed: false });
  await json("faults", "/grant", { kind: "account", value: email });
  expect((await request("faults", "/corrupt")).status).toBe(400);
  expect((await request("faults", "/grant", { kind: "account", value: email })).status).toBe(400);
  expect((await request("faults", "/revoke", { kind: "account", target: email })).status).toBe(400);
  expect((await request("faults", "/resolve", undefined, email)).status).toBe(403);
});

it("initializes funding independently without resetting existing identities and fails closed on unknown formats or mismatched records", async () => {
  for (const point of ["schema:before", "schema:after"]) {
    const store = `initialize-${point}`;

    expect((await request(store, `/seed?point=${point}`)).status).toBe(400);
    expect(await json(store, "/status")).toEqual({ admin: true, allowed: true });
    expect((await request(store, "/unsupported")).status).toBe(400);
    expect((await request(store, "/status")).status).toBe(400);
    expect((await request(store, "/grant", { kind: "account", value: email })).status).toBe(400);
  }
  await json("mismatch", "/seed");
  await json("mismatch", "/grant", { kind: "account", value: email });
  expect((await request("mismatch", "/mismatch")).status).toBe(400);
  expect((await request("mismatch", "/status", undefined, email)).status).toBe(400);
  expect((await request("mismatch", "/grant", { kind: "account", value: email })).status).toBe(400);
});
