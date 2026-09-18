import {
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionRpcError,
  browserQuickActionCaptureLayer,
} from "@effect-agent/platform-cloudflare/cloudflare-browser";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Layer, Redacted, Schema } from "effect";
import { CompactionError } from "effect-agent/context-compactor";
import { ThreadId } from "effect-agent/identifiers";
import { RunContextPreparation } from "effect-agent/run-options";
import { ThreadExport, ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { DurableObject } from "effect-cf";
import { FetchHttpClient } from "effect/unstable/http";

import { PlannerError, TripSiteStore } from "../../src/domain.ts";
import { ReadTravelPageLive } from "../../src/research.ts";
import { makeTravelPlannerThread, plannerApplication } from "../../src/server/cloudflare.ts";
import { credentialClient } from "../../src/server/models.ts";
import { ownerOfThread } from "../../src/server/tenancy.ts";
import { AppSourceLive } from "../../src/trip-app/service.ts";
import { fixtureOwner } from "./identity.ts";
import fixtureWorker from "./worker.ts";

// Only the local recorder handles these hosts. Credentials stay in its Node process.
const browserCall = (action: string, options: unknown) =>
  Effect.tryPromise({
    try: (signal) =>
      fetch(`https://browser-eval.invalid/${action}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(options),
        signal,
      }),
    catch: (cause) => new BrowserQuickActionRpcError({ action: "markdown", cause }),
  });

const browser = ReadTravelPageLive.pipe(
  Layer.provide(browserQuickActionCaptureLayer()),
  Layer.provide(
    Layer.succeed(BrowserQuickActionBrowserBinding, {
      markdown: (options) => browserCall("markdown", options),
      content: (options) => browserCall("content", options),
      screenshot: (options) => browserCall("screenshot", options),
      links: (options) => browserCall("links", options),
      scrape: (options) => browserCall("scrape", options),
      json: (options) => browserCall("json", options),
    }),
  ),
);

const model = OpenAiLanguageModel.model("gpt-5.6-luna", {
  store: false,
  max_output_tokens: 4096,
  max_tool_calls: 2,
  reasoning: { effort: "low" },
  service_tier: "default",
}).pipe(
  Layer.provide(
    Layer.effect(
      OpenAiClient.OpenAiClient,
      credentialClient(Effect.succeed(Redacted.make("local-recorder-proxy"))),
    ),
  ),
  Layer.provide(FetchHttpClient.layer),
);

const capture = Layer.succeed(RunContextPreparation, {
  hook: {
    prepare: (request) =>
      Effect.tryPromise({
        try: (signal) =>
          fetch("https://capture-eval.invalid/prompt", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(request),
            signal,
          }).then((response) => {
            if (!response.ok) throw new Error("Prompt capture failed");

            return { prompt: request.source };
          }),
        catch: () => new CompactionError({ message: "Could not preserve the native prompt" }),
      }),
  },
});

const sites = Layer.succeed(TripSiteStore, {
  publish: () =>
    Effect.fail(new PlannerError({ code: "publication", message: "Eval trips are private." })),
  load: () => Effect.succeed(null),
});

export class TravelPlannerThread extends makeTravelPlannerThread(
  sites,
  plannerApplication(
    model,
    "live-travel-eval-v1",
    "gpt-5.6-luna",
    browser,
    undefined,
    Layer.merge(AppSourceLive, capture),
  ),
) {
  fetch(request: Request): Promise<Response> {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const threadId = yield* Schema.decodeEffect(ThreadId)(
          new URL(request.url).searchParams.get("thread") ?? "",
        );

        const store = yield* ThreadStore;
        const exported = yield* store.export(ThreadExportRequest.make({ threadId }));

        return new Response(
          yield* Schema.encodeEffect(Schema.fromJsonString(ThreadExport))(exported),
          { headers: { "content-type": "application/json" } },
        );
      }),
    );
  }
}

// Test-only identity and export routes are never part of the deployed application.
export default {
  fetch(request: Request, env: Cloudflare.Env & { readonly PLANNER_TOKEN?: string }) {
    const url = new URL(request.url);

    if (url.pathname === "/__eval/journal") {
      if (
        !env.PLANNER_TOKEN ||
        request.headers.get("authorization") !== `Bearer ${env.PLANNER_TOKEN}`
      )
        return new Response("Unauthorized", { status: 401 });
      const requested = url.searchParams.get("thread") ?? "";

      const thread =
        requested.startsWith("worker:") || requested.startsWith("account-")
          ? requested
          : `${fixtureOwner()}--${requested}`;

      url.searchParams.set("thread", thread);

      return env.ACCOUNT_THREADS.getByName(
        thread.startsWith("worker:") ? thread : ownerOfThread(thread),
      ).fetch(new Request(url, request));
    }

    return fixtureWorker.fetch(request, env);
  },
};
