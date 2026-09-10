import { fileURLToPath } from "node:url";

import type { Sandbox } from "@cloudflare/sandbox";
import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect, Layer, Schema } from "effect";

import { adminEmail } from "./src/access-domain.ts";

const state = Layer.unwrap(
  Config.boolean("ALCHEMY_LOCAL_STATE").pipe(
    Config.withDefault(false),
    Effect.map((local) => (local ? Alchemy.localState() : Cloudflare.state())),
    Effect.orDie,
  ),
);

// A separate stack keeps application deployments independent of the docs site.
export default Alchemy.Stack(
  "effect-agent-travel-planner",
  {
    providers: Cloudflare.providers(),
    state,
  },
  Effect.gen(function* () {
    const { accountId } = yield* yield* Cloudflare.CloudflareEnvironment;

    const publicRegistration = yield* Config.boolean("PUBLIC_SIGN_UP").pipe(
      Config.withDefault(true),
    );

    const include = publicRegistration
      ? [{ everyone: {} }]
      : [
          { email: { email: adminEmail } },
          { group: { id: yield* Config.nonEmptyString("ACCESS_GROUP_ID") } },
        ];

    const allowInvited = yield* Cloudflare.Access.Policy("TravelInvitedUsers", {
      name: publicRegistration
        ? "effect-agent-travel-planner-public-sign-in"
        : "effect-agent-travel-planner-invited",
      decision: "allow",
      // Authenticate every user; do not use bypass, which would omit a verified identity.
      include,
    });

    const access = yield* Cloudflare.Access.Application("TravelAccess", {
      name: "Effect Agent Travel Planner",
      type: "self_hosted",
      domain: "travel.effect-agent.com",
      destinations: [
        { type: "public", uri: "travel.effect-agent.com" },
        { type: "public", uri: "effect-agent-travel-planner.danieljmerwe.workers.dev" },
      ],
      allowedIdps: ["363645f2-553c-455d-89d4-dd287272a51e"],
      autoRedirectToIdentity: true,
      sessionDuration: "720h",
      policies: [allowInvited.policyId],
    });

    const artifacts = yield* Cloudflare.Artifacts.Namespace("ARTIFACTS", {
      namespace: "effect-agent-travel-planner",
    });

    const zoneId = "9662e63e42d87b741fbcae6b65506924";

    yield* Cloudflare.DNS.Record("TripAppsWildcard", {
      zoneId,
      name: "*.effect-agent.com",
      type: "A",
      content: "192.0.2.1",
      proxied: true,
      ttl: "1",
    });
    const builds = yield* Cloudflare.R2.Bucket("TripAppBuilds");

    const app = yield* Cloudflare.Website.Vite("Planner", {
      name: "effect-agent-travel-planner",
      domain: "travel.effect-agent.com",
      routes: [{ pattern: "*-trip.effect-agent.com/*", zoneId }],
      workersDev: { enabled: true, previewsEnabled: false },
      rootDir: fileURLToPath(new URL(".", import.meta.url)),
      main: "src/worker.ts",
      compatibility: { date: "2026-07-01", flags: ["nodejs_compat"] },
      assets: { runWorkerFirst: true },
      env: {
        THREADS: Cloudflare.DurableObject("THREADS", { className: "PlannerThread" }),
        ARTIFACTS: artifacts,
        APP_BUILDS: builds,
        APP_LOADER: Cloudflare.WorkerLoader("APP_LOADER"),
        APP_DOMAIN: "effect-agent.com",
        SITE_BUILD: Cloudflare.Workflow("SiteBuild", { className: "SiteBuild" }),
        APP_SANDBOX: Cloudflare.Container<Sandbox>("AppSandbox", {
          className: "Sandbox",
          context: "./site-builder",
          // Explicit standard-3 resources: the named tier alone did not resize
          // the deployed application through the current Alchemy provider.
          vcpu: 2,
          memory: "8GiB",
          disk: { size_mb: 16000 },
          maxInstances: 4,
        }),
        ARTIFACTS_GIT_BASE: `https://${accountId}.artifacts.cloudflare.net/git/${artifacts.namespace}`,
        BROWSER: Cloudflare.Browser(),
        ACCESS_OPEN_REGISTRATION: String(publicRegistration),
        ACCESS_TEAM_DOMAIN: Config.nonEmptyString("ACCESS_TEAM_DOMAIN").pipe(
          Config.withDefault("https://orange-cake-d758.cloudflareaccess.com"),
        ),
        ACCESS_AUD: access.aud,
        OPENAI_MODEL: Config.string("OPENAI_MODEL").pipe(Config.withDefault("gpt-5.6-luna")),
        BYOK_ENCRYPTION_KEY: Config.schema(
          Schema.Redacted(Schema.NonEmptyString),
          "BYOK_ENCRYPTION_KEY",
        ),
      },
      observability: { enabled: true },
      memo: {
        include: ["src/**", "site-builder/**", "vite.config.ts", "package.json"],
        lockfile: true,
      },
    });

    return { url: app.url };
  }),
);
