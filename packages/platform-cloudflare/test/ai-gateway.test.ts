import * as Gateway from "@effect-agent/platform-cloudflare/CloudflareAiGateway";
import { expect, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";

it.effect("uses the portable Gateway transport in workerd with scoped fetch settings", () =>
  Effect.gen(function* () {
    const config = Gateway.rest({
      accountId: "account",
      gatewayId: "gateway",
      apiToken: Redacted.make("worker-token"),
      protocol: "chat-completions",
    });

    let sentUrl = "";
    let sentOptions: RequestInit | undefined;

    const response = yield* Effect.gen(function* () {
      const client = config.transformClient(yield* HttpClient.HttpClient);

      return yield* client.post(`${config.apiUrl}/chat/completions`);
    }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, async (url, options) => {
        sentUrl = typeof url === "string" ? url : url instanceof URL ? url.href : url.url;
        sentOptions = options;

        return Response.json({ choices: [] });
      }),
    );

    expect(sentUrl).toBe(
      "https://api.cloudflare.com/client/v4/accounts/account/ai/v1/chat/completions",
    );
    expect(sentOptions?.redirect).toBe("error");
    expect(response.request.headers["cf-aig-gateway-id"]).toBe("gateway");
    expect(JSON.stringify(response.request.headers)).not.toContain("worker-token");
  }),
);
