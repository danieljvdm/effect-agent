import { fileURLToPath } from "node:url";

import type { Sandbox } from "@cloudflare/sandbox";
import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect, Layer, Redacted, Schema } from "effect";

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

    const serverOpenAiKey = yield* Config.redacted("SERVER_OPENAI_KEY").pipe(
      Config.withDefault(Redacted.make("")),
    );

    const artifacts = yield* Cloudflare.Artifacts.Namespace("ARTIFACTS", {
      namespace: "effect-agent-travel-planner-auth-v1",
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
      workersDev: { enabled: false, previewsEnabled: false },
      rootDir: fileURLToPath(new URL(".", import.meta.url)),
      main: "src/worker.ts",
      compatibility: { date: "2026-07-01", flags: ["nodejs_compat"] },
      assets: { runWorkerFirst: true },
      env: {
        ...(Redacted.value(serverOpenAiKey) ? { SERVER_OPENAI_KEY: serverOpenAiKey } : {}),
        // Alchemy keys env-bound Objects by the binding name, overriding the
        // declaration ID. Change both binding and class for this clean start.
        // Keep both stable after release; changing them deletes account data.
        ACCOUNT_THREADS: Cloudflare.DurableObject("AccountThreadsV1", {
          className: "AccountPlannerThread",
        }),
        AUTH: Cloudflare.DurableObject("AuthV1", { className: "PlannerAuth" }),
        AUTH_EMAIL: Cloudflare.Email.SendEmail("AuthEmail", {
          allowedSenderAddresses: [yield* Config.nonEmptyString("AUTH_EMAIL_FROM")],
        }),
        AUTH_ORIGIN: Config.nonEmptyString("AUTH_ORIGIN"),
        AUTH_EMAIL_FROM: Config.nonEmptyString("AUTH_EMAIL_FROM"),
        AUTH_GITHUB_CLIENT_ID: Config.nonEmptyString("AUTH_GITHUB_CLIENT_ID"),
        AUTH_GITHUB_CLIENT_SECRET: Config.schema(
          Schema.Redacted(Schema.NonEmptyString),
          "AUTH_GITHUB_CLIENT_SECRET",
        ),
        AUTH_BINDING_KEY: Config.schema(Schema.Redacted(Schema.NonEmptyString), "AUTH_BINDING_KEY"),
        AUTH_PROOF_KEY: Config.schema(Schema.Redacted(Schema.NonEmptyString), "AUTH_PROOF_KEY"),
        AUTH_TRANSACTION_KEY: Config.schema(
          Schema.Redacted(Schema.NonEmptyString),
          "AUTH_TRANSACTION_KEY",
        ),
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
        OPENAI_MODEL: Config.string("OPENAI_MODEL").pipe(Config.withDefault("gpt-5.6-luna")),
        BYOK_ENCRYPTION_KEY: Config.schema(
          Schema.Redacted(Schema.NonEmptyString),
          "BYOK_ENCRYPTION_KEY",
        ),
      },
      // Callback query values must not enter automatic request URL logs.
      observability: {
        enabled: true,
        logs: { enabled: true, invocationLogs: false },
        traces: { enabled: false },
      },
      logpush: false,
      memo: {
        include: ["src/**", "site-builder/**", "vite.config.ts", "package.json"],
        lockfile: true,
      },
    });

    return { url: app.url };
  }),
);
