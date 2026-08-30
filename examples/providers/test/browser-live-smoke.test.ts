import { WebCapture, WebCaptureSuccess } from "@effect-agent/capabilities";
import { Agent, AgentPolicy, IdGenerator } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import {
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionRpcError,
  browserQuickActionCaptureLayer,
  type BrowserQuickActionClient,
} from "@effect-agent/platform-cloudflare/browser-quick-action";
import {
  PHASE7_LIVE_CREDENTIAL_ENV,
  phase7LiveProfileEnabled,
} from "@effect-agent/testing/fixtures/travel-planner";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Console, Effect, Layer, Ref, Schema, Stream } from "effect";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { describe, expect, it } from "vite-plus/test";

const CLOUDFLARE_ACCOUNT_ENV = "CLOUDFLARE_ACCOUNT_ID";
const CLOUDFLARE_TOKEN_ENV = "CLOUDFLARE_API_TOKEN";
const PRICING_URL = "https://linear.app/pricing";
const FIRST_PLAN = "Basic";
const SECOND_PLAN = "Business";
const OPENAI_MODEL = "gpt-5.6-luna";
const REASONING_EFFORT = "medium";
const SERVICE_TIER = "fast";

const liveEnabled =
  phase7LiveProfileEnabled(process.env) &&
  (process.env[CLOUDFLARE_ACCOUNT_ENV] ?? "") !== "" &&
  (process.env[CLOUDFLARE_TOKEN_ENV] ?? "") !== "";

interface RecordedQuickAction {
  readonly action: "screenshot" | "content" | "markdown" | "links" | "scrape" | "json";
  readonly options: Record<string, unknown>;
}

/** Map the REST smoke transport onto the Effect-native binding client. */
const makeLiveBrowserBinding = Effect.gen(function* () {
  const accountId = yield* Config.nonEmptyString(CLOUDFLARE_ACCOUNT_ENV);
  const apiToken = yield* Config.redacted(CLOUDFLARE_TOKEN_ENV);
  const client = yield* HttpClient.HttpClient;
  const calls = yield* Ref.make<ReadonlyArray<RecordedQuickAction>>([]);

  const runQuickAction = Effect.fn("BrowserLiveSmoke.runQuickAction")(function* (
    action: RecordedQuickAction["action"],
    options: object,
  ): Effect.fn.Return<Response, BrowserQuickActionRpcError> {
    const rpcError = (cause: unknown) => BrowserQuickActionRpcError.make({ action, cause });
    yield* Ref.update(calls, (previous) => [...previous, { action, options: { ...options } }]);

    const request = yield* HttpClientRequest.post(
      `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(accountId)}/browser-rendering/${encodeURIComponent(action)}`,
    ).pipe(
      HttpClientRequest.bearerToken(apiToken),
      HttpClientRequest.bodyJson(options),
      Effect.mapError(rpcError),
    );
    const response = yield* client.execute(request).pipe(Effect.mapError(rpcError));
    const rawBody = yield* response.text.pipe(Effect.mapError(rpcError));
    const contentType = response.headers["content-type"]?.toLowerCase() ?? "";
    const wrapTextResult =
      response.status >= 200 &&
      response.status < 300 &&
      (action === "content" || action === "markdown") &&
      !contentType.includes("application/json");
    const body = wrapTextResult ? JSON.stringify({ success: true, result: rawBody }) : rawBody;

    return new Response(body, {
      status: response.status,
      headers: {
        ...response.headers,
        ...(wrapTextResult ? { "content-type": "application/json" } : {}),
      },
    });
  });
  const binding: BrowserQuickActionClient = {
    screenshot: (options) => runQuickAction("screenshot", options),
    content: (options) => runQuickAction("content", options),
    markdown: (options) => runQuickAction("markdown", options),
    links: (options) => runQuickAction("links", options),
    scrape: (options) => runQuickAction("scrape", options),
    json: (options) => runQuickAction("json", options),
  };

  return { binding, calls };
});

const PricingPlan = Schema.Struct({
  name: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  monthlyUsdPerUser: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  billingTerms: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
});

const PricingComparison = Schema.Struct({
  company: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  plans: Schema.Array(PricingPlan).check(Schema.isMinLength(2), Schema.isMaxLength(2)),
  cheaperPlan: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  monthlySavingsUsdPerUser: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  recommendation: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
});

const readWebpage = WebCapture.make("read_webpage", {
  description: "Render Linear's public pricing page and return its Markdown.",
  urls: ["linear.app"],
  actions: ["markdown"],
  maxResponseBytes: 32 * 1024,
});

const BrowserResearcher = Agent.withModel(
  Agent.define("live-pricing-researcher", {
    input: Schema.Struct({
      url: Schema.NonEmptyString,
      firstPlan: Schema.NonEmptyString,
      secondPlan: Schema.NonEmptyString,
    }),
    output: PricingComparison,
    instructions:
      "You must call read_webpage exactly once with the input URL and action markdown. " +
      "Use the returned public pricing page to identify the two requested plans. " +
      "Return the company, exactly those two plan names, their actual monthly USD price per " +
      "user, and their stated billing terms. Name the cheaper plan, calculate its monthly " +
      "savings per user, and explain the recommendation without inventing prices. " +
      "Keep the recommendation to one sentence under 240 characters.",
    toolkit: Toolkit.make(readWebpage.tool),
    policy: AgentPolicy.make({
      maxTurns: 3,
      maxToolCalls: 1,
      maxDuration: "90 seconds",
      toolConcurrency: 1,
    }),
  }),
  OpenAiLanguageModel.model(OPENAI_MODEL, {
    reasoning: { effort: REASONING_EFFORT },
    service_tier: SERVICE_TIER,
  }),
);

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted(PHASE7_LIVE_CREDENTIAL_ENV),
}).pipe(Layer.provide(FetchHttpClient.layer));

const BrowserSmokeLayer = Layer.mergeAll(
  IdGenerator.layer,
  OpenAiClientLayer,
  FetchHttpClient.layer,
);

describe.skipIf(!liveEnabled)("Browser Run live smoke (opt-in)", () => {
  it(
    "compares real Linear subscription prices through OpenAI and Cloudflare Browser Run",
    { timeout: 120_000 },
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const browser = yield* makeLiveBrowserBinding;
          const browserHandlers = readWebpage.handlers.pipe(
            Layer.provide(
              browserQuickActionCaptureLayer().pipe(
                Layer.provide(Layer.succeed(BrowserQuickActionBrowserBinding)(browser.binding)),
              ),
            ),
          );
          const events = yield* AgentRuntime.stream(BrowserResearcher, {
            url: PRICING_URL,
            firstPlan: FIRST_PLAN,
            secondPlan: SECOND_PLAN,
          }).pipe(Stream.runCollect, Effect.provide(browserHandlers));

          const toolResults = events.filter((event) => event._tag === "ToolCallSucceeded");

          expect(toolResults.length).toBe(1);
          expect(toolResults[0]?.toolName).toBe("read_webpage");

          const capturedPage = yield* Schema.decodeUnknownEffect(WebCaptureSuccess)(
            toolResults[0]?.result,
          );

          expect(capturedPage.url).toBe(PRICING_URL);
          expect(capturedPage.action).toBe("markdown");
          expect(capturedPage.markdown?.includes(FIRST_PLAN)).toBe(true);
          expect(capturedPage.markdown?.includes(SECOND_PLAN)).toBe(true);

          const completed = events.filter((event) => event._tag === "RunCompleted");

          expect(completed.length).toBe(1);

          const answer = yield* Schema.decodeUnknownEffect(PricingComparison)(completed[0]?.output);
          const firstPlan = answer.plans.find((plan) => plan.name === FIRST_PLAN);
          const secondPlan = answer.plans.find((plan) => plan.name === SECOND_PLAN);
          const firstPrice = firstPlan?.monthlyUsdPerUser ?? Number.NaN;
          const secondPrice = secondPlan?.monthlyUsdPerUser ?? Number.NaN;

          expect(answer.company.toLowerCase()).toBe("linear");
          expect(firstPrice).toBeGreaterThan(0);
          expect(secondPrice).toBeGreaterThan(firstPrice);
          expect(capturedPage.markdown?.includes(`$${String(firstPrice)}`)).toBe(true);
          expect(capturedPage.markdown?.includes(`$${String(secondPrice)}`)).toBe(true);
          expect(answer.cheaperPlan).toBe(FIRST_PLAN);
          expect(answer.monthlySavingsUsdPerUser).toBeCloseTo(secondPrice - firstPrice, 2);

          const calls = yield* Ref.get(browser.calls);

          expect(calls.length).toBe(1);
          expect(calls[0]?.action).toBe("markdown");
          expect(calls[0]?.options.url).toBe(PRICING_URL);
          expect(calls[0]?.options.allowRequestPattern).toEqual([
            "^https://linear\\.app(?::[0-9]+)?(?:[/?#]|$)",
          ]);

          yield* Console.log(
            "\nCloudflare Browser Run result:",
            JSON.stringify(
              {
                url: capturedPage.url,
                action: capturedPage.action,
                markdown: capturedPage.markdown?.slice(0, 2_048),
              },
              null,
              2,
            ),
          );
          yield* Console.log(
            "\nOpenAI pricing comparison:",
            JSON.stringify(
              {
                model: OPENAI_MODEL,
                reasoningEffort: REASONING_EFFORT,
                serviceTier: SERVICE_TIER,
                comparison: answer,
              },
              null,
              2,
            ),
          );
        }).pipe(Effect.scoped, Effect.provide(BrowserSmokeLayer)),
      ),
  );
});
