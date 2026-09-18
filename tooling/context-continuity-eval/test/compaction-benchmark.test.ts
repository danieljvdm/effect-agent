import { OpenAiClient } from "@effect/ai-openai";
import { Effect, Exit, Layer, Redacted, Ref, Stream } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { expect, it } from "vite-plus/test";

import {
  benchmarkCases,
  pressureBenchmarkCases,
  transferBenchmarkCases,
} from "../src/compaction-benchmark-cases.ts";
import { makeLiveClient } from "../src/live-model.ts";
import { RequestAuditSink } from "../src/request-audit.ts";

it("keeps hidden evidence and oracle answers separate while respecting each declared result bound", () => {
  for (const fixture of [...benchmarkCases, ...transferBenchmarkCases, ...pressureBenchmarkCases]) {
    const instructions = `${fixture.scenario.task}\n${fixture.scenario.question}`;

    for (const answer of fixture.expected) expect(instructions).not.toContain(answer);
    for (const entry of fixture.scenario.history) {
      if (entry.kind === "tool")
        expect(new TextEncoder().encode(JSON.stringify(entry.result)).length).toBeLessThanOrEqual(
          fixture.maxResultBytes,
        );
    }
    const source = fixture.scenario.required[1];

    const result = fixture.scenario.history.find(
      (entry) => entry.kind === "tool" && entry.id === source?.toolCallId,
    );

    expect(result?.kind).toBe("tool");
    if (result?.kind === "tool" && source !== undefined) {
      expect(result.result).toContain(source.value);
      if (fixture.scale !== "small" || fixture.profile !== "many-small")
        expect(result.result.slice(0, 800)).not.toContain(source.value);
    }
  }
});

it.each([
  { profile: undefined, tokens: 300_000, admitted: false, cost: 0 },
  { profile: "large-compaction", tokens: 922_001, admitted: false, cost: 0 },
  { profile: "large-compaction", tokens: 300_000, admitted: true, cost: 145_580 },
] as const)(
  "enforces the opt-in input bound and full-request long pricing: $profile / $tokens",
  async ({ profile, tokens, admitted, cost }) => {
    let inferences = 0;

    const http = HttpClient.make((request) => {
      if (request.url.endsWith("/input_tokens"))
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            Response.json({ object: "response.input_tokens", input_tokens: tokens }),
          ),
        );
      inferences++;

      const event = {
        type: "response.completed",
        sequence_number: 0,
        response: {
          id: "resp_benchmark",
          object: "response",
          model: "gpt-5.6-luna",
          created_at: 1,
          status: "completed",
          service_tier: "default",
          output: [],
          usage: {
            input_tokens: tokens,
            output_tokens: 100,
            total_tokens: tokens + 100,
            input_tokens_details: { cached_tokens: 10_000 },
            output_tokens_details: { reasoning_tokens: 0 },
          },
        },
      };

      return Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(`data: ${JSON.stringify(event)}\n\n`, {
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      );
    });

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const live = yield* makeLiveClient({
          model: "gpt-5.6-luna",
          maxCostMicrousd: 1_000_000,
          maxInputTokens: 922_000,
          ...(profile === undefined ? {} : { profile }),
          phase: yield* Ref.make(0),
        });

        const exit = yield* live.client
          .createResponseStream({
            model: "gpt-5.6-luna",
            store: false,
            service_tier: "default",
            max_output_tokens: 4096,
            input: "synthetic",
          })
          .pipe(
            Effect.flatMap(([, stream]) => Stream.runDrain(stream)),
            Effect.exit,
          );

        return { exit, usage: yield* live.snapshot };
      }).pipe(
        Effect.provide(
          Layer.merge(
            Layer.succeed(RequestAuditSink, RequestAuditSink.of({ write: () => Effect.void })),
            OpenAiClient.layer({ apiKey: Redacted.make("test") }).pipe(
              Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
            ),
          ),
        ),
      ),
    );

    expect(Exit.isSuccess(result.exit)).toBe(admitted);
    expect(inferences).toBe(admitted ? 1 : 0);
    expect(result.usage.estimatedCostMicrousd).toBe(cost);
    expect(result.usage.reservedCostMicrousd).toBe(0);
  },
);
