import { fileURLToPath } from "node:url";

import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Output from "alchemy/Output";
import { Config, Effect, Schema } from "effect";

import { CheckoutController } from "./checkout-contract.ts";

export const checkoutStack = Alchemy.Stack(
  "effect-agent-checkout",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const run = yield* Config.schema(
      Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,24}$/)),
      "CHECKOUT_RUN_ID",
    );

    const { accountId } = yield* yield* Cloudflare.CloudflareEnvironment;
    const expectedAccount = yield* Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID");

    if (accountId !== expectedAccount)
      return yield* Effect.die("Alchemy account differs from CLOUDFLARE_ACCOUNT_ID");
    const paymentName = `ea-checkout-${run}-pay`;
    const shopName = `ea-checkout-${run}-shop`;
    const checkoutToken = yield* Config.Redacted("CHECKOUT_TOKEN");
    const openaiKey = yield* Config.Redacted("OPENAI_API_KEY");
    const browserToken = yield* Config.Redacted("BROWSER_RENDERING_API_TOKEN");
    const model = yield* Config.NonEmptyString("CHECKOUT_MODEL");

    const controller = yield* Config.schema(CheckoutController, "CHECKOUT_CONTROLLER").pipe(
      Config.withDefault("baseline"),
    );

    const typesafeKey =
      controller === "indexed-jev" ? yield* Config.Redacted("TYPESAFEAI_API_KEY") : undefined;

    const bindingName = `ea-checkout-${run}-binding`;

    const binding = yield* Cloudflare.Worker("BindingProof", {
      name: bindingName,
      main: fileURLToPath(new URL("./worker.ts", import.meta.url).href),
      compatibility: { date: "2026-03-24", flags: ["nodejs_compat"] },
      workersDev: { enabled: true, previewsEnabled: false },
      env: {
        BROWSER: Cloudflare.Browser(),
        CLOUDFLARE_ACCOUNT_ID: accountId,
        BROWSER_RENDERING_API_TOKEN: browserToken,
      },
      observability: { enabled: false },
    });

    const payment = yield* Cloudflare.Worker("Payment", {
      name: paymentName,
      main: fileURLToPath(new URL("./checkout-processor.ts", import.meta.url).href),
      compatibility: { date: "2026-03-24", flags: ["nodejs_compat"] },
      workersDev: { enabled: true, previewsEnabled: false },
    });

    const shop = yield* Cloudflare.Worker("Shop", {
      name: shopName,
      main: fileURLToPath(new URL("./checkout-worker.ts", import.meta.url).href),
      compatibility: { date: "2026-03-24", flags: ["nodejs_compat"] },
      workersDev: { enabled: true, previewsEnabled: false },
      env: {
        BROWSER: Cloudflare.Browser(),
        CHECKOUTS: Cloudflare.DurableObject("CheckoutRun", { className: "CheckoutRun" }),
        CHECKOUT_TOKEN: checkoutToken,
        OPENAI_API_KEY: openaiKey,
        CHECKOUT_MODEL: model,
        CHECKOUT_CONTROLLER: controller,
        ...(typesafeKey === undefined ? {} : { TYPESAFEAI_API_KEY: typesafeKey }),
        PROCESSOR_ORIGIN: Output.map(payment.url, (url) => url ?? "https://unavailable.invalid"),
        CLOUDFLARE_ACCOUNT_ID: accountId,
        BROWSER_RENDERING_API_TOKEN: browserToken,
      },
      observability: { enabled: false },
    });

    return {
      shopUrl: shop.url,
      processorUrl: payment.url,
      bindingUrl: binding.url,
      shopName,
      paymentName,
      bindingName,
    };
  }),
);
