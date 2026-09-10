import { spawn } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { it as effectIt } from "@effect/vitest";
import { Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { type AppFile, PlannerError } from "../src/domain.ts";
import { TripFailpoint } from "../src/server/trips.ts";
import { AppSourceStore, appSourceLayer } from "../src/trip-app/source.ts";

// The native Artifacts control plane is deterministic; source transfer and Git
// history use the real smart-HTTP backend and production isomorphic-git client.
const command = (args: string[], input = new Uint8Array(), env = process.env) =>
  new Promise<Buffer>((resolve, reject) => {
    const child = spawn("git", args, { env, stdio: ["pipe", "pipe", "pipe"] });
    const chunks: Buffer[] = [];

    child.stdout.on("data", (chunk: Buffer) => chunks.push(chunk));
    child.stderr.resume();
    child.on("error", reject);
    child.on("exit", (code) =>
      code === 0 ? resolve(Buffer.concat(chunks)) : reject(new Error("Fixture Git command failed")),
    );
    child.stdin.end(input);
  });

const initial: AppFile[] = [
  { path: "src/main.ts", content: "export const title = 'Trip';\n" },
  { path: "index.html", content: "<main>Trip</main>" },
];

const names = new Set<string>();
const forks: string[] = [];
const tokens = new Set<string>();
let released = 0;
let created = 0;
let point = "";
let directory: string;
let mode: "git" | "redirect" | "oversized" | "wait" = "git";
let started: (() => void) | undefined;
let aborted = false;

const metadata = (name: string): ArtifactsRepoInfo => ({
  id: name,
  name,
  description: null,
  defaultBranch: "main",
  createdAt: "2026-09-09T00:00:00Z",
  updatedAt: "2026-09-09T00:00:00Z",
  lastPushAt: null,
  source: null,
  readOnly: false,
  remote: `https://git.example/${name}.git`,
});

const result = (name: string): ArtifactsCreateRepoResult => {
  const token = `creation-${name}`;

  tokens.add(token);

  return {
    id: name,
    name,
    description: null,
    defaultBranch: "main",
    remote: metadata(name).remote,
    token,
    tokenExpiresAt: "2026-09-09T00:01:00Z",
  };
};

const repository = (name: string): ArtifactsRepo & Disposable => ({
  ...metadata(name),
  createToken: async (scope = "write", ttl) => {
    expect(ttl).toBe(60);
    const id = `token-${++created}`;

    tokens.add(id);

    return { id, plaintext: id, scope, expiresAt: "2026-09-09T00:01:00Z" };
  },
  revokeToken: async (id) => tokens.delete(id),
  listTokens: async () => ({ tokens: [], total: 0 }),
  fork: async (target, options) => {
    expect(name).toBe("trip-app-template-v1");
    expect(options).toEqual({ defaultBranchOnly: true });
    if (names.has(target)) throw { code: "ALREADY_EXISTS" };
    forks.push(target);
    await cp(join(directory, `${name}.git`), join(directory, `${target}.git`), {
      recursive: true,
      force: false,
      errorOnExist: true,
    });
    names.add(target);

    return result(target);
  },
  [Symbol.dispose]: () => {
    released++;
  },
});

const artifacts: Artifacts = {
  get: async (name) => {
    if (!names.has(name)) throw { code: "NOT_FOUND" };

    return repository(name);
  },
  create: async (name) => {
    if (names.has(name)) throw { code: "ALREADY_EXISTS" };
    await command(["init", "--bare", "--initial-branch=main", join(directory, `${name}.git`)]);
    await command([
      "--git-dir",
      join(directory, `${name}.git`),
      "config",
      "http.receivepack",
      "true",
    ]);
    names.add(name);

    return result(name);
  },
  import: async () => {
    throw new Error("No remote imports allowed");
  },
  list: async () => ({ repos: [], total: 0 }),
  delete: async () => {
    throw new Error("No deletions allowed");
  },
};

const run = <A, E>(effect: Effect.Effect<A, E, AppSourceStore>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(appSourceLayer(artifacts, "https://git.example")),
      Effect.provideService(TripFailpoint, {
        hit: (candidate) =>
          Effect.suspend(() => {
            if (candidate !== point) return Effect.void;
            point = "";

            return Effect.fail(
              new PlannerError({ code: "storage", message: "Injected lost acknowledgement" }),
            );
          }),
      }),
    ),
  );

const fork = (repoName = "trip-one", files = initial) =>
  run(Effect.flatMap(AppSourceStore, (store) => store.fork({ repoName, files })));

const read = (repoName: string, commitId: string) =>
  run(Effect.flatMap(AppSourceStore, (store) => store.read({ repoName, commitId })));

const commit = (parentCommit: string, files: AppFile[], repoName = "trip-one") =>
  run(
    Effect.flatMap(AppSourceStore, (store) =>
      store.commit({ repoName, parentCommit, files, message: "Update trip source" }),
    ),
  );

beforeEach(async () => {
  directory = await mkdtemp(join(tmpdir(), "trip-app-git-"));
  names.clear();
  forks.length = 0;
  tokens.clear();
  created = 0;
  released = 0;
  point = "";
  mode = "git";
  aborted = false;
  vi.stubGlobal("fetch", async (url: string, init: RequestInit) => {
    const request = new Request(url, init);
    const parsed = new URL(url);

    expect(parsed.origin).toBe("https://git.example");
    expect(init.redirect).toBe("manual");
    expect(request.headers.get("authorization")).toMatch(/^Bearer token-/);
    if (mode === "redirect")
      return new Response(null, {
        status: 302,
        headers: { location: "https://untrusted.example" },
      });
    if (mode === "oversized") return new Response(new Uint8Array(8 * 1024 * 1024 + 1));
    if (mode === "wait")
      return await new Promise<Response>((_resolve, reject) => {
        request.signal.addEventListener(
          "abort",
          () => {
            aborted = true;
            reject(new Error("aborted"));
          },
          { once: true },
        );
        started?.();
      });
    const body = new Uint8Array(await request.arrayBuffer());

    const raw = await command(["http-backend"], body, {
      ...process.env,
      GIT_PROJECT_ROOT: directory,
      GIT_HTTP_EXPORT_ALL: "1",
      PATH_INFO: parsed.pathname,
      REQUEST_METHOD: request.method,
      QUERY_STRING: parsed.search.slice(1),
      CONTENT_TYPE: request.headers.get("content-type") ?? "",
      CONTENT_LENGTH: String(body.length),
      HTTP_GIT_PROTOCOL: request.headers.get("git-protocol") ?? "",
      REMOTE_USER: "fixture",
    });

    let divider = -1;

    for (let index = 0; index < raw.length - 3; index++)
      if (
        raw[index] === 13 &&
        raw[index + 1] === 10 &&
        raw[index + 2] === 13 &&
        raw[index + 3] === 10
      ) {
        divider = index;
        break;
      }
    if (divider < 0) throw new Error("Invalid Git CGI response");
    const headers = new Headers();
    let status = 200;

    for (const header of raw.subarray(0, divider).toString().split("\r\n")) {
      const colon = header.indexOf(":");
      const key = header.slice(0, colon);
      const value = header.slice(colon + 1).trim();

      if (key === "Status") status = Number(value.slice(0, 3));
      else headers.set(key, value);
    }

    return new Response(new Uint8Array(raw.subarray(divider + 4)), { status, headers });
  });
});
afterEach(async () => {
  vi.unstubAllGlobals();
  await rm(directory, { recursive: true, force: true });
});

it("uses native forks, preserves parent history, replaces the complete tree, and reads old commits", async () => {
  const seed = await fork();

  expect(await fork()).toEqual(seed);
  expect(forks).toEqual(["trip-one"]);
  expect(await read("trip-one", seed.commitId)).toEqual(
    [...initial].sort((a, b) => a.path.localeCompare(b.path)),
  );

  const desired = [
    { path: "src/main.ts", content: "export const title = 'Revised';\n" },
    { path: "assets/style.css", content: "body { color: blue }" },
  ];

  const updated = await commit(seed.commitId, desired);

  expect(updated.commitId).not.toBe(seed.commitId);

  const parents = await command([
    "--git-dir",
    join(directory, "trip-one.git"),
    "rev-list",
    "--parents",
    "-n",
    "1",
    updated.commitId,
  ]);

  expect(parents.toString().trim()).toBe(`${updated.commitId} ${seed.commitId}`);
  expect(await read("trip-one", updated.commitId)).toEqual(
    [...desired].sort((a, b) => a.path.localeCompare(b.path)),
  );
  expect(await read("trip-one", seed.commitId)).toHaveLength(2);
  expect(await commit(seed.commitId, desired)).toEqual(updated);
  await expect(commit(seed.commitId, initial)).rejects.toMatchObject({ code: "conflict" });
  await expect(
    fork("other", [{ path: "different.ts", content: "different" }]),
  ).rejects.toMatchObject({ code: "conflict" });
  expect(names.has("other")).toBe(false);
  expect(tokens.size).toBe(0);
  expect(released).toBeGreaterThan(0);
}, 30_000);

it("recovers a committed fork and push after lost acknowledgements without another revision", async () => {
  point = "app-source:fork:after";
  await expect(fork()).rejects.toMatchObject({ code: "storage" });
  const seed = await fork();

  expect(forks).toEqual(["trip-one"]);
  point = "app-source:push:before";
  const desired = [{ path: "index.html", content: "Revised" }];

  await expect(commit(seed.commitId, desired)).rejects.toMatchObject({ code: "storage" });
  expect(await read("trip-one", seed.commitId)).toHaveLength(2);
  point = "app-source:push:after";
  const updated = await commit(seed.commitId, desired);

  expect(await commit(seed.commitId, desired)).toEqual(updated);

  const count = await command([
    "--git-dir",
    join(directory, "trip-one.git"),
    "rev-list",
    "--count",
    "main",
  ]);

  expect(count.toString().trim()).toBe("2");
  expect(tokens.size).toBe(0);
}, 30_000);

it("rejects unsafe, duplicate, and oversized sources before any binding call", async () => {
  for (const path of [
    "/absolute",
    "../escape",
    "src/../escape",
    "src/.git/config",
    "NODE_MODULES/pkg",
    "dist/index.js",
    "src\\file",
    "src//file",
    "src/\u0000file",
  ])
    await expect(fork("trip-one", [{ path, content: "unsafe" }])).rejects.toMatchObject({
      code: "invalid",
    });
  for (const files of [
    Array.from({ length: 101 }, (_, index) => ({ path: `f${index}`, content: "" })),
    [{ path: "file", content: "😀".repeat(40_000) }],
    [
      { path: "file", content: "a" },
      { path: "file", content: "b" },
    ],
    [
      { path: "src", content: "a" },
      { path: "src/main.ts", content: "b" },
    ],
    Array.from({ length: 17 }, (_, index) => ({
      path: `f${index}`,
      content: "x".repeat(128 * 1024),
    })),
  ])
    await expect(fork("trip-one", files)).rejects.toMatchObject({ code: "invalid" });
  expect(names.size).toBe(0);
  expect(created).toBe(0);
});

it("blocks credential redirects and excessive Git transfers, and cancels in-flight HTTP on interruption", async () => {
  const seed = await fork();

  for (const selected of ["redirect", "oversized"] satisfies Array<typeof mode>) {
    mode = selected;
    await expect(read("trip-one", seed.commitId)).rejects.toMatchObject({ code: "storage" });
    expect(tokens.size).toBe(0);
  }
  mode = "wait";

  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });

  const fiber = Effect.runFork(
    Effect.flatMap(AppSourceStore, (store) =>
      store.read({ repoName: "trip-one", commitId: seed.commitId }),
    ).pipe(Effect.provide(appSourceLayer(artifacts, "https://git.example"))),
  );

  await ready;
  await Effect.runPromise(Fiber.interrupt(fiber));
  expect(aborted).toBe(true);
  expect(tokens.size).toBe(0);
}, 30_000);

it("allows only one competing main update and never force-overwrites its winner", async () => {
  const seed = await fork();

  const writes = await Promise.allSettled([
    commit(seed.commitId, [{ path: "index.html", content: "First" }]),
    commit(seed.commitId, [{ path: "index.html", content: "Second" }]),
  ]);

  expect(writes.filter((write) => write.status === "fulfilled")).toHaveLength(1);
  expect(writes.filter((write) => write.status === "rejected")).toMatchObject([
    { reason: { code: "conflict" } },
  ]);

  const history = await command([
    "--git-dir",
    join(directory, "trip-one.git"),
    "rev-list",
    "--count",
    "main",
  ]);

  expect(history.toString().trim()).toBe("2");
  expect(tokens.size).toBe(0);
}, 30_000);

effectIt.effect("applies the operation deadline and releases HTTP and token resources", () =>
  Effect.gen(function* () {
    const seed = yield* Effect.promise(() => fork());

    mode = "wait";

    const ready = new Promise<void>((resolve) => {
      started = resolve;
    });

    const fiber = yield* Effect.forkChild(
      Effect.flatMap(AppSourceStore, (store) =>
        store.read({ repoName: "trip-one", commitId: seed.commitId }),
      ).pipe(Effect.provide(appSourceLayer(artifacts, "https://git.example")), Effect.result),
    );

    yield* Effect.promise(() => ready);
    yield* TestClock.adjust("45 seconds");
    const timedOut = yield* Fiber.join(fiber);

    expect(timedOut).toMatchObject({ _tag: "Failure", failure: { code: "storage" } });
    expect(aborted).toBe(true);
    expect(tokens.size).toBe(0);
  }),
);
