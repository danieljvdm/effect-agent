import type { OpenAiClient } from "@effect/ai-openai";
import { NodeServices } from "@effect/platform-node";
import type { Layer } from "effect";
import { Effect, FileSystem, Ref, Schema } from "effect";
import { expect, expectTypeOf, it } from "vite-plus/test";

import { makeLiveClient } from "../src/live-model.ts";
import { RequestAudit, RequestAuditSink } from "../src/request-audit.ts";

it("exposes the audit capability and file adapter requirement", () => {
  const client = Effect.gen(function* () {
    return yield* makeLiveClient({
      model: "gpt-6-astra",
      maxCostMicrousd: 10_000_000,
      phase: yield* Ref.make(0),
    });
  });

  const file = RequestAuditSink.file("unused");

  expectTypeOf<Effect.Services<typeof client>>().toEqualTypeOf<
    OpenAiClient.OpenAiClient | RequestAuditSink
  >();
  expectTypeOf<Effect.Error<typeof client>>().toEqualTypeOf<never>();
  expectTypeOf<Layer.Services<typeof file>>().toEqualTypeOf<FileSystem.FileSystem>();
});

it("durably appends request/response evidence and fails when its path is unavailable", async () => {
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "continuity-audit-test-" });
      const path = `${directory}/requests.ndjson`;

      const event: RequestAudit = {
        kind: "request",
        request: 1,
        phase: 6,
        inputTokens: 40,
        outputTokens: 0,
        json: "synthetic",
      };

      yield* Effect.gen(function* () {
        const sink = yield* RequestAuditSink;

        yield* sink.write(event);
        yield* sink.write({ ...event, kind: "response", outputTokens: 10 });
      }).pipe(Effect.provide(RequestAuditSink.file(path)));

      const events = yield* Effect.forEach(
        (yield* fs.readFileString(path)).trim().split("\n"),
        (line) => Schema.decodeUnknownEffect(Schema.fromJsonString(RequestAudit))(line),
      );

      const failure = yield* RequestAuditSink.use((sink) => sink.write(event)).pipe(
        Effect.provide(RequestAuditSink.file(`${directory}/missing/requests.ndjson`)),
        Effect.exit,
      );

      return { events, failure };
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  expect(result.events.map((e) => e.kind)).toEqual(["request", "response"]);
  expect(result.failure._tag).toBe("Failure");
});
