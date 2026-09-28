import { fileURLToPath } from "node:url";

import { assert, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

import { Report, Snapshot } from "../src/contract.ts";

it.live(
  "connects without a token and refuses cross-origin and malformed requests before browser acquisition",
  () =>
    Effect.gen(function* () {
      const bundle = yield* Effect.promise(() =>
        build({
          entryPoints: [fileURLToPath(new URL("../src/worker.ts", import.meta.url))],
          bundle: true,
          write: false,
          format: "esm",
          platform: "browser",
          target: "es2022",
          external: ["cloudflare:*", "node:*"],
          alias: { crypto: "node:crypto" },
          conditions: ["workerd", "worker", "browser"],
        }),
      );

      const script = bundle.outputFiles[0]?.text;

      if (script === undefined) return yield* Effect.die("No Worker bundle");
      let browserCalls = 0;
      const upgrades: Array<{ url: string; method: string; upgrade: string | null }> = [];

      const runtime = yield* Effect.acquireRelease(
        Effect.sync(
          () =>
            new Miniflare(
              convertV4MiniflareOptions({
                modules: true,
                script,
                modulesRoot: "/",
                compatibilityDate: "2026-07-01",
                compatibilityFlags: ["nodejs_compat"],
                durableObjects: { LAB: { className: "BrowserLab", useSQLite: true } },
                bindings: {
                  CLOUDFLARE_ACCOUNT_ID: "a".repeat(32),
                  BROWSER_RENDERING_API_TOKEN: "unused",
                },
                serviceBindings: {
                  BROWSER: () => {
                    browserCalls++;

                    return new Response("unexpected browser call", { status: 503 });
                  },
                  ASSETS: () => new Response("fixture assets"),
                },
                outboundService: (request) => {
                  upgrades.push({
                    url: request.url,
                    method: request.method,
                    upgrade: request.headers.get("upgrade"),
                  });

                  return new Response("private provider refusal", { status: 503 });
                },
              }),
            ),
        ),
        (value) => Effect.promise(() => value.dispose()),
      );

      const headers = { "x-lab-session": crypto.randomUUID() };

      const status = (path: string, init?: Parameters<Miniflare["dispatchFetch"]>[1]) =>
        Effect.promise(() => runtime.dispatchFetch(`https://lab.test${path}`, init));

      const snapshot = yield* status("/api/snapshot", { headers });

      const body = yield* Effect.promise(() => snapshot.json()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Snapshot)),
      );

      assert.strictEqual(snapshot.status, 200);
      assert.isFalse(body.ready);
      assert.isTrue(body.browserConfigured);
      assert.isFalse(body.agentConfigured);
      assert.isFalse(body.jevConfigured);
      assert.strictEqual(body.models.length, 3);
      assert.isTrue(body.models.every((model) => !model.configured));
      assert.isFalse(body.busy);
      assert.isNull(body.liveViewUrl);
      assert.isNull(body.report);
      assert.strictEqual((yield* status("/api/snapshot")).status, 400);
      assert.strictEqual(
        (yield* status("/api/snapshot", { headers: { ...headers, "x-lab-session": "../other" } }))
          .status,
        400,
      );
      assert.strictEqual(
        (yield* status("/api/run", {
          method: "POST",
          headers: { ...headers, origin: "https://other.test", "content-type": "application/json" },
          body: "{}",
        })).status,
        400,
      );
      assert.strictEqual(
        (yield* status("/api/run", {
          method: "POST",
          headers: { ...headers, origin: "https://lab.test", "content-type": "application/json" },
          body: "{}",
        })).status,
        400,
      );
      assert.strictEqual(browserCalls, 0);
      for (const selection of [
        { engine: "unapproved-browser" },
        { model: "unapproved-model", grounding: "direct" },
        { model: "gpt-6-luna", grounding: "direct" },
        { model: "gpt-6-luna", grounding: "jev" },
      ]) {
        const rejected = yield* status("/api/run", {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: JSON.stringify({
            id: crypto.randomUUID(),
            scenario: "create",
            mode: "agent",
            temperature: "cold",
            prompt: "",
            screenshots: false,
            liveView: false,
            ...selection,
          }),
        });

        assert.strictEqual(rejected.status, 400);
      }
      assert.strictEqual(
        browserCalls,
        0,
        "Missing or unapproved models must fail before browser acquisition",
      );

      const rejectedKitesurf = yield* status("/api/run", {
        method: "POST",
        headers: { ...headers, "content-type": "application/json" },
        body: JSON.stringify({
          id: crypto.randomUUID(),
          engine: "kitesurf",
          scenario: "create",
          mode: "scripted",
          prompt: "",
          screenshots: false,
          liveView: false,
        }),
      });

      const report = yield* Effect.promise(() => rejectedKitesurf.json()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Report)),
      );

      assert.strictEqual(report.status, "failed");
      assert.strictEqual(report.cleanup, "closed");
      assert.isNull(report.readyAt);
      assert.strictEqual(report.message, "Kitesurf CDP connection failed.");
      assert.strictEqual(upgrades.length, 2, "Only preparation may retry");
      assert.isTrue(
        upgrades.every(
          (request) =>
            request.method === "GET" &&
            request.upgrade === "websocket" &&
            new URL(request.url).search === "?browser=kitesurf",
        ),
      );
      assert.strictEqual(
        browserCalls,
        0,
        "A refused Kitesurf upgrade must never fall back to Chromium",
      );
    }),
  { timeout: 30_000 },
);
