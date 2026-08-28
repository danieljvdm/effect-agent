import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect, Layer } from "effect";

// Deploys run against the account-wide Cloudflare state store so CI runs
// share one state history; ALCHEMY_LOCAL_STATE=true keeps dry runs and local
// experiments out of it. Alchemy's `state` option requires an infallible
// Layer (`Layer<State, never, StackServices>`), so a malformed flag cannot
// stay typed past this boundary: it fails closed as a defect that names the
// flag and the accepted values instead of silently picking a state store.
const state = Layer.unwrap(
  Effect.gen(function* () {
    const useLocalState = yield* Config.boolean("ALCHEMY_LOCAL_STATE").pipe(
      Config.withDefault(false),
    );

    return useLocalState ? Alchemy.localState() : Cloudflare.state();
  }).pipe(
    Effect.mapError(
      (error) =>
        new Error(
          `ALCHEMY_LOCAL_STATE must be a boolean (true/false); refusing to guess a state store: ${String(error)}`,
        ),
    ),
    Effect.orDie,
  ),
);

const stack = Effect.gen(function* () {
  const docs = yield* Cloudflare.Website.StaticSite("Docs", {
    name: "effect-agent-docs",
    command: "bun run docs:build",
    outdir: "docs/.vitepress/dist",
    domain: "effect-agent.com",
    workersDev: false,
    dev: { command: "bun run docs:dev" },
    // VitePress emits 404.html; its cleanUrls links match the default
    // auto-trailing-slash HTML handling.
    assets: { notFoundHandling: "404-page" },
    // The dist and cache directories are gitignored, so hashing docs/**
    // rebuilds exactly when a source page or the site config changes;
    // package.json is included because it owns the docs:build script.
    memo: { include: ["docs/**", "package.json"], lockfile: true },
  });

  return { url: docs.url };
});

export default Alchemy.Stack("effect-agent", { providers: Cloudflare.providers(), state }, stack);
