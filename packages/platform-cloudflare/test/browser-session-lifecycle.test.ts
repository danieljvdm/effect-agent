import { expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted } from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/unstable/http";

import { BrowserRunSessionLifecycle } from "../src/browser-session-lifecycle.ts";

const sessionId = "c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99";
const accountId = "1234567890abcdef1234567890abcdef";

it.effect("uses a credential-preserving redirect policy supported by the Worker runtime", () =>
  Effect.gen(function* () {
    yield* (yield* BrowserRunSessionLifecycle).close(Redacted.make(sessionId));
  }).pipe(
    Effect.provide(
      BrowserRunSessionLifecycle.layer({
        accountId,
        apiToken: Redacted.make("fixture-token"),
      }).pipe(Layer.provide(FetchHttpClient.layer)),
    ),
    Effect.provideService(FetchHttpClient.Fetch, async (input, init) => {
      // The real workerd Request constructor rejects unsupported fetch options.
      const request = new Request(input, init);
      expect(request.redirect).toBe("manual");
      return new Response('{"status":"closed"}', {
        headers: { "content-type": "application/json" },
      });
    }),
  ),
);

it.effect(
  "confirms exact-session termination without treating pending or untrusted responses as absence",
  () =>
    Effect.gen(function* () {
      for (const scenario of [
        { responses: [[200, '{"status":"closed"}']], terminal: true, requests: 1 },
        { responses: [[404, '{"error":"Session not found"}']], terminal: true, requests: 1 },
        {
          responses: [
            [200, '{"status":"closing"}'],
            [200, `{"sessionId":"${sessionId}"}`],
            [404, '{"error":"Session not found"}'],
          ],
          terminal: true,
          requests: 3,
        },
        {
          responses: [
            [200, '{"status":"closing"}'],
            [200, `{"sessionId":"${sessionId}","endTime":1}`],
          ],
          terminal: true,
          requests: 2,
        },
        {
          responses: [
            [200, '{"status":"closing"}'],
            [200, `{"sessionId":"${sessionId}"}`],
            [200, `{"sessionId":"${sessionId}"}`],
          ],
          terminal: false,
          requests: 3,
        },
        { responses: [[401, "{}"]], terminal: false, requests: 1 },
        { responses: [[403, '{"error":"Session not found"}']], terminal: false, requests: 1 },
        { responses: [[429, "{}"]], terminal: false, requests: 1 },
        { responses: [[500, '{"error":"Session not found"}']], terminal: false, requests: 1 },
        { responses: [[404, '{"error":"Route not found"}']], terminal: false, requests: 1 },
        {
          responses: [
            [200, '{"status":"closing"}'],
            [200, '{"sessionId":"b8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99","endTime":1}'],
          ],
          terminal: false,
          requests: 2,
        },
        { responses: [[200, "broken"]], terminal: false, requests: 1 },
      ] as const) {
        const calls: Array<string> = [];
        const client = HttpClient.make((request, url) =>
          Effect.sync(() => {
            const response = scenario.responses[calls.length];
            calls.push(request.method);
            expect(url.origin).toBe("https://api.cloudflare.com");
            expect(url.pathname).toBe(
              `/client/v4/accounts/${accountId}/browser-rendering/devtools/${request.method === "DELETE" ? "browser" : "session"}/${sessionId}`,
            );
            if (response === undefined) throw new Error("Exceeded cleanup request budget");
            return HttpClientResponse.fromWeb(
              request,
              new Response(response[1], {
                status: response[0],
                headers: { "content-type": "application/json" },
              }),
            );
          }),
        );
        const result = yield* Effect.gen(function* () {
          return yield* (yield* BrowserRunSessionLifecycle).close(Redacted.make(sessionId));
        }).pipe(
          Effect.provide(
            BrowserRunSessionLifecycle.layer({
              accountId,
              apiToken: Redacted.make("fixture-token"),
            }),
          ),
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.exit,
        );
        expect(result._tag === "Success").toBe(scenario.terminal);
        expect(calls.length).toBe(scenario.requests);
        expect(calls.filter((method) => method === "DELETE")).toHaveLength(1);
      }
    }),
);
