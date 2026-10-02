import * as Alchemy from "alchemy";
import { adopt } from "alchemy/AdoptPolicy";
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
    const useLocalState = yield* Config.Boolean("ALCHEMY_LOCAL_STATE").pipe(
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

const docsAssets = {
  base: "/agent/",
  notFoundHandling: "404-page",
} satisfies Omit<Cloudflare.Workers.AssetsProps, "directory">;

const stack = Effect.gen(function* () {
  yield* Cloudflare.Website.StaticSite("Docs", {
    name: "effect-agent-docs",
    command: "vp run docs:build",
    outdir: "docs/dist",
    domain: "effect-agent.com",
    routes: [{ pattern: "yielded.dev/agent*", zoneName: "yielded.dev" }],
    workersDev: false,
    dev: { command: "vp run docs:dev" },
    // Astro emits directory indexes and 404.html. Existing extensionless
    // links redirect to the same page with a trailing slash.
    assets: docsAssets,
    // The dist and cache directories are gitignored, so hashing docs/**
    // rebuilds exactly when a source page or the site config changes;
    // package.json is included because it owns the docs:build script.
    memo: { include: ["docs/**", "package.json"], lockfile: true },
  });

  const legacyZone = yield* Cloudflare.Zone.Zone("LegacyDocsZone", {
    name: "effect-agent.com",
  }).pipe(adopt());

  yield* Cloudflare.Ruleset.Ruleset("LegacyDocsRedirect", {
    zone: legacyZone,
    phase: "http_request_dynamic_redirect",
    rules: [
      {
        action: "redirect",
        expression: 'http.host in {"effect-agent.com" "www.effect-agent.com"}',
        description: "Move agent documentation to yielded.dev/agent",
        actionParameters: {
          fromValue: {
            statusCode: 301,
            preserveQueryString: true,
            targetUrl: {
              expression: 'concat("https://yielded.dev/agent", http.request.uri.path)',
            },
          },
        },
      },
    ],
  });

  return { url: "https://yielded.dev/agent/" };
});

export default Alchemy.Stack("effect-agent", { providers: Cloudflare.providers(), state }, stack);
