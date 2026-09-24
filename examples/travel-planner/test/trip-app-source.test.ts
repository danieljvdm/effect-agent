import { spawn } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect } from "effect";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { type AppFile, PlannerError } from "../src/domain.ts";
import { TripFailpoint } from "../src/server/trips.ts";
import { AppSourceStore, appSourceLayer } from "../src/trip-app/source.ts";

// The native Artifacts control plane is deterministic; source transfer and Git
// history use the real smart-HTTP backend and production isomorphic-git client.
const command = (args: string[], input = new Uint8Array(), env = process.env) =>
  new Promise<Buffer>((resolve, reject) => {
    const child = spawn("git", args, {
      env,
      stdio: [input.byteLength === 0 ? "ignore" : "pipe", "pipe", "pipe"],
    });

    const chunks: Buffer[] = [];

    child.stdout?.on("data", (chunk: Buffer) => chunks.push(chunk));
    child.stderr?.resume();
    child.on("error", reject);
    child.on("close", (code) =>
      code === 0 ? resolve(Buffer.concat(chunks)) : reject(new Error("Fixture Git command failed")),
    );
    child.stdin?.on("error", reject);
    child.stdin?.end(input);
  });

const initial: AppFile[] = [
  { path: "src/main.ts", content: "export const title = 'Trip';\n" },
  { path: "index.html", content: "<main>Trip</main>" },
];

const names = new Set<string>();
const forks: string[] = [];
const tokens = new Set<string>();
let created = 0;
let point = "";
let directory: string;
let mode: "git" | "redirect" = "git";

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
  createToken: async (scope = "write") => {
    const id = `token-${++created}`;

    tokens.add(id);

    return { id, plaintext: id, scope, expiresAt: "2026-09-09T00:01:00Z" };
  },
  revokeToken: async (id) => tokens.delete(id),
  listTokens: async () => ({ tokens: [], total: 0 }),
  fork: async (target) => {
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
  [Symbol.dispose]: () => {},
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
  point = "";
  mode = "git";
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

it("recovers a committed fork and push after lost acknowledgements without another revision", async () => {
  point = "app-source:fork:after";
  await expect(fork()).rejects.toMatchObject({ code: "storage" });
  const seed = await fork();

  expect(forks).toEqual(["trip-one"]);
  const desired = [{ path: "index.html", content: "Revised" }];

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

it("blocks credential redirects and releases Git tokens", async () => {
  const seed = await fork();

  mode = "redirect";
  await expect(read("trip-one", seed.commitId)).rejects.toMatchObject({ code: "storage" });
  expect(tokens.size).toBe(0);
}, 30_000);

// d210027c changed the starter's dependencies while every fork still shared template-v1.
// A fresh deployment must create new apps without rewriting previously saved source.
it("forks an updated starter while preserving existing apps and retry conflicts", async () => {
  const original = await fork();
  const updated = [{ path: "index.html", content: "Updated starter" }];
  const next = await fork("trip-two", updated);

  expect(await read("trip-two", next.commitId)).toEqual(updated);
  expect(await fork("trip-two", updated)).toEqual(next);
  await expect(fork("trip-one", updated)).rejects.toMatchObject({ code: "conflict" });
  expect(await fork()).toEqual(original);
  expect(await read("trip-one", original.commitId)).toEqual(
    [...initial].sort((a, b) => a.path.localeCompare(b.path)),
  );
  expect(forks).toEqual(["trip-one", "trip-two"]);
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
