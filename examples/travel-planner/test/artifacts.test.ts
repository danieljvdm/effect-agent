import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect, it } from "vite-plus/test";

import type { Trip } from "../src/domain.ts";

// Captured from the empty sample repository's real Artifacts Git discovery.
const discovery =
  "001e# service=git-upload-pack\n0000000eversion 2\n0014agent=gitty/1.0\n0013ls-refs=unborn\n0012fetch=shallow\n0012server-option\n0017object-format=sha1\n0000";

const revisionRef = `${"a".repeat(40)} refs/heads/revision-1\n`;
const advertisedRevision = `${(revisionRef.length + 4).toString(16).padStart(4, "0")}${revisionRef}0000`;

const token: ArtifactsCreateTokenResult = {
  id: "fixture-token",
  plaintext: "art_v1_fixture?expires=9999999999",
  scope: "read",
  expiresAt: "2286-11-20T17:46:39Z",
};

const trip: Trip = {
  id: "fixture",
  revision: 1,
  published: null,
  title: "Lisbon weekend",
  destination: "Lisbon",
  summary: "A quiet weekend",
  startDate: null,
  endDate: null,
  travelers: 1,
  days: [{ title: "Arrive", activities: ["Walk by the river"] }],
  notes: [],
};

it("uses native repository handles for Git discovery, publication, clone negotiation, and safe redirects", async () => {
  const bundle = await build({
    stdin: {
      resolveDir: import.meta.dirname,
      loader: "ts",
      contents: `
        import { Effect } from "effect";
        import { artifactsLayer, ArtifactsFailpoint } from "../src/artifacts.ts";
        import { PlannerError, TripSiteStore } from "../src/domain.ts";
        export default { async fetch(request, env: { ARTIFACTS: Artifacts }) {
          const mode = new URL(request.url).pathname;
          const base = "https://git.example" + (["/redirect", "/existing"].includes(mode) ? mode : "");
          const result = await Effect.runPromise(Effect.gen(function* () {
            const sites = yield* TripSiteStore;
            return mode === "/publish"
              ? yield* sites.publish({ trip: ${JSON.stringify(trip)} })
              : yield* sites.load({ tripId: "fixture", revision: 1 });
          }).pipe(
            Effect.provide(artifactsLayer(env.ARTIFACTS, base)),
            Effect.provideService(ArtifactsFailpoint, {
              hit: (point) => point === "push:before"
                ? Effect.fail(new PlannerError({ code: "storage", message: "Reached push boundary" }))
                : Effect.void,
            }),
            Effect.result,
          ));
          return Response.json(result);
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
    banner: {
      js: 'import { createRequire } from "node:module"; const require = createRequire("/fixture.mjs");',
    },
    logLevel: "silent",
  });

  const output = bundle.outputFiles[0];

  if (!output) throw new Error("Missing fixture bundle");

  const requests: Array<{
    readonly url: string;
    readonly method: string;
    readonly authorization: string | null;
    readonly protocol: string | null;
  }> = [];

  let existingDiscoveries = 0;

  const runtime = new Miniflare(
    convertV4MiniflareOptions({
      workers: [
        {
          name: "planner",
          modules: true,
          script: output.text,
          modulesRoot: "/",
          compatibilityDate: "2026-07-01",
          compatibilityFlags: ["nodejs_compat"],
          serviceBindings: { ARTIFACTS: "artifacts" },
          outboundService: async (request) => {
            requests.push({
              url: request.url,
              method: request.method,
              authorization: request.headers.get("authorization"),
              protocol: request.headers.get("git-protocol"),
            });
            const url = new URL(request.url);

            if (url.hostname !== "git.example")
              return new Response("Unexpected credential destination", { status: 500 });
            if (url.pathname.startsWith("/redirect/"))
              return new Response(null, {
                status: 302,
                headers: { location: "https://untrusted.example/receive-credentials" },
              });
            if (request.method === "GET") {
              // Stop after clone discovery: no packfile server is needed to
              // verify that v2 ref lookup did not contaminate clone's headers.
              if (url.pathname.startsWith("/existing/") && ++existingDiscoveries > 1)
                return new Response("Fixture stops at clone discovery", { status: 503 });

              return new Response(discovery, {
                headers: { "content-type": "application/x-git-upload-pack-advertisement" },
              });
            }
            expect(url.pathname).toMatch(/\/trip-fixture\.git\/git-upload-pack$/);
            expect(await request.text()).toContain("command=ls-refs");

            return new Response(
              url.pathname.startsWith("/existing/") ? advertisedRevision : "0000",
              {
                headers: { "content-type": "application/x-git-upload-pack-result" },
              },
            );
          },
        },
        {
          name: "artifacts",
          modules: true,
          compatibilityDate: "2026-07-01",
          // Native repository handles expose methods, not REST metadata fields.
          script: `
          import { RpcTarget, WorkerEntrypoint } from "cloudflare:workers";
          class Repository extends RpcTarget {
            async createToken(scope, ttl) {
              if (!["read", "write"].includes(scope) || ttl !== 60)
                throw new Error("Unexpected token request");
              return { ...${JSON.stringify(token)}, scope };
            }
          }
          export default class extends WorkerEntrypoint {
            async get(name) {
              if (name !== "trip-fixture") throw new Error("Unexpected repository");
              return new Repository();
            }
          }
        `,
        },
      ],
    }),
  );

  try {
    const loaded = await runtime.dispatchFetch("http://planner/load");

    expect(await loaded.json()).toMatchObject({ _tag: "Success", value: null });
    expect(requests.map((request) => request.method)).toEqual(["GET", "POST"]);
    const published = await runtime.dispatchFetch("http://planner/publish");

    expect(await published.json()).toMatchObject({
      _tag: "Failure",
      failure: { code: "storage", message: "Reached push boundary" },
    });
    expect(requests).toHaveLength(4);
    const redirected = await runtime.dispatchFetch("http://planner/redirect");

    expect(await redirected.json()).toMatchObject({
      _tag: "Failure",
      failure: { code: "publication" },
    });
    expect(requests).toHaveLength(5);
    expect(requests.map((request) => new URL(request.url).pathname)).toEqual([
      "/trip-fixture.git/info/refs",
      "/trip-fixture.git/git-upload-pack",
      "/trip-fixture.git/info/refs",
      "/trip-fixture.git/git-upload-pack",
      "/redirect/trip-fixture.git/info/refs",
    ]);
    expect(requests.every((request) => new URL(request.url).hostname === "git.example")).toBe(true);
    expect(requests.every((request) => request.authorization === `Bearer ${token.plaintext}`)).toBe(
      true,
    );
    const existing = await runtime.dispatchFetch("http://planner/existing");

    expect(await existing.json()).toMatchObject({
      _tag: "Failure",
      failure: { code: "publication" },
    });
    expect(requests.slice(5).map(({ method, protocol }) => ({ method, protocol }))).toEqual([
      { method: "GET", protocol: "version=2" },
      { method: "POST", protocol: "version=2" },
      { method: "GET", protocol: null },
    ]);
  } finally {
    await runtime.dispose();
  }
}, 30_000);
