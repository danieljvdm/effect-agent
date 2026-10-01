import { fileURLToPath } from "node:url";

import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Config, Effect } from "effect";

import { RunId } from "./proof.ts";

export const checkoutStack = Alchemy.Stack(
  "effect-agent-checkout",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const run = yield* Config.schema(RunId, "CHECKOUT_RUN_ID");

    const { accountId } = yield* yield* Cloudflare.CloudflareEnvironment;
    const expectedAccount = yield* Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID");

    if (accountId !== expectedAccount)
      return yield* Effect.die("Alchemy account differs from CLOUDFLARE_ACCOUNT_ID");
    const workerName = `ea-checkout-${run}`;
    const checkoutToken = yield* Config.Redacted("CHECKOUT_TOKEN");
    const checkoutPassword = yield* Config.Redacted("CHECKOUT_PASSWORD");
    const openaiKey = yield* Config.Redacted("OPENAI_API_KEY");
    const browserToken = yield* Config.Redacted("BROWSER_RENDERING_API_TOKEN");
    const model = yield* Config.NonEmptyString("CHECKOUT_MODEL");

    const worker = yield* Cloudflare.Worker("Checkout", {
      name: workerName,
      main: fileURLToPath(new URL("./worker.ts", import.meta.url).href),
      compatibility: { date: "2026-03-24", flags: ["nodejs_compat"] },
      workersDev: { enabled: true, previewsEnabled: false },
      env: {
        BROWSER: Cloudflare.Browser(),
        CHECKOUTS: Cloudflare.DurableObject("CheckoutRun", { className: "CheckoutRun" }),
        CHECKOUT_TOKEN: checkoutToken,
        CHECKOUT_PASSWORD: checkoutPassword,
        OPENAI_API_KEY: openaiKey,
        CHECKOUT_MODEL: model,
        CLOUDFLARE_ACCOUNT_ID: accountId,
        BROWSER_RENDERING_API_TOKEN: browserToken,
      },
      observability: { enabled: false },
    });

    return {
      url: worker.url,
      workerName,
    };
  }),
);
