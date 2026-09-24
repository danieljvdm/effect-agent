import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { type TripApp } from "../src/domain.ts";

const app: TripApp = {
  id: "a".repeat(32),
  tripId: "lisbon",
  revision: 1,
  url: "https://app.example",
  repoName: "app-lisbon",
  sourceCommit: "b".repeat(40),
  activeCommit: null,
  pendingCommit: "b".repeat(40),
  status: "building",
  error: null,
  updatedAt: "2026-09-09T00:00:00Z",
  versions: [],
};

let directory: string;
let script: string;
let runtime: Miniflare;

const start = () =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { APPS: { className: "Apps", useSQLite: true } },
      resourcePersistencePath: directory,
    }),
  );

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "trip-app-repository-"));

  const bundle = await build({
    stdin: {
      resolveDir: import.meta.dirname,
      loader: "ts",
      contents: `
    import { DurableObject } from "cloudflare:workers";
    import { SqliteClient } from "@effect/sql-sqlite-do";
    import { Effect, Layer, Schema, Cause } from "effect";
    import { SqlClient } from "effect/unstable/sql/SqlClient";
    import { AppRepository, AppRepositoryLive } from "../src/trip-app/repository.ts";
    import { TripApp } from "../src/domain.ts";
    const Command = Schema.Struct({ kind: Schema.String, app: Schema.optionalKey(TripApp), expected: Schema.optionalKey(Schema.NullOr(Schema.Number)) });
    export class Apps extends DurableObject {
      async fetch(request) {
        const input = Schema.decodeUnknownSync(Command)(await request.json());
        const sql = SqliteClient.layer({ storage: this.ctx.storage });
        const layers = AppRepositoryLive.pipe(Layer.provideMerge(sql));
        const result = await Effect.runPromise(Effect.gen(function* () {
          const repository = yield* AppRepository;
          const client = yield* SqlClient;
          if (input.kind === "save" && input.app) return yield* repository.save(input.app, input.expected ?? null);
          if (input.kind === "get") return yield* repository.get("lisbon");
          if (input.kind === "id") return yield* repository.getById("${app.id}");
          return yield* client\`SELECT value FROM travel_app_revisions ORDER BY revision\`;
        }).pipe(Effect.provide(layers), Effect.exit));
        return Response.json(result._tag === "Success" ? { _tag: "Success", value: result.value } : { _tag: "Failure", error: Cause.pretty(result.cause) });
      }
    }
    export default { async fetch(request, env) {
      const response = await env.APPS.get(env.APPS.idFromName(new URL(request.url).pathname)).fetch(request);
      return new Response(await response.arrayBuffer(), response);
    }};
  `,
    },
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    logLevel: "silent",
  });

  const output = bundle.outputFiles[0];

  if (!output) throw new Error("Missing fixture bundle");
  script = output.text;
  runtime = start();
});
afterAll(async () => {
  await runtime?.dispose();
  if (directory) await rm(directory, { recursive: true, force: true });
});

const Result = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
  Schema.Struct({ _tag: Schema.Literal("Failure"), error: Schema.String }),
]);

const call = async (owner: string, input: unknown) => {
  const response = await runtime.dispatchFetch(`http://fixture/${owner}`, {
    method: "POST",
    body: JSON.stringify(input),
  });

  return Schema.decodeUnknownSync(Result)(await response.json());
};

const value = async (owner: string, input: unknown) => {
  const result = await call(owner, input);

  if (result._tag === "Failure") throw new Error(result.error);

  return result.value;
};

it("keeps the latest revision across an old retry and object restart", async () => {
  expect(await value("owner", { kind: "save", app, expected: null })).toEqual(app);

  const ready: TripApp = {
    ...app,
    revision: 2,
    status: "ready",
    activeCommit: app.sourceCommit,
    pendingCommit: null,
  };

  expect(await value("owner", { kind: "save", app: ready, expected: 1 })).toEqual(ready);
  expect(await value("owner", { kind: "save", app, expected: null })).toEqual(app);
  expect(await value("owner", { kind: "get" })).toEqual(ready);
  await runtime.dispose();
  runtime = start();
  expect(await value("owner", { kind: "id" })).toEqual(ready);
  expect(await value("owner", { kind: "rows" })).toHaveLength(2);
}, 30_000);
