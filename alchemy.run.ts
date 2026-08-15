import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect, Layer } from "effect";

// Deploys run against the account-wide Cloudflare state store so CI runs
// share one state history; ALCHEMY_LOCAL_STATE=true keeps dry runs and local
// experiments out of it.
const state = Layer.unwrap(
  Effect.gen(function* () {
    const useLocalState = yield* Config.boolean("ALCHEMY_LOCAL_STATE").pipe(
      Config.withDefault(false),
    );

    return useLocalState ? Alchemy.localState() : Cloudflare.state();
  }).pipe(Effect.orDie),
);

const stack = Effect.gen(function* () {
  const docs = yield* Cloudflare.Website.StaticSite("Docs", {
    name: "effect-agent-docs",
    command: "bun run docs:build",
    outdir: "docs/.vitepress/dist",
    domain: "effect-agent.danvdm.com",
    workersDev: false,
    dev: { command: "bun run docs:dev" },
    // VitePress emits 404.html; its cleanUrls links match the default
    // auto-trailing-slash HTML handling.
    assets: { notFoundHandling: "404-page" },
    // The dist and cache directories are gitignored, so hashing docs/**
    // rebuilds exactly when a source page or the site config changes.
    memo: { include: ["docs/**"], lockfile: true },
  });

  return { url: docs.url };
});

export default Alchemy.Stack("effect-agent", { providers: Cloudflare.providers(), state }, stack);
