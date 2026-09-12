import { fileURLToPath } from "node:url";

import { NodeServices } from "@effect/platform-node";
import { Effect, Exit, FileSystem, Layer, Redacted, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect, expectTypeOf, it } from "vite-plus/test";

import { EvaluationError } from "../src/contracts.ts";
import {
  PerformanceIdentity,
  PerformanceSnapshot,
  expectedPerformanceOutput,
} from "../src/performance-contracts.ts";
import {
  PerformanceDeployment,
  PerformanceOwnership,
  withPerformanceDeployment,
  type PerformanceTarget,
} from "../src/performance-deployment.ts";
import {
  gradePerformancePhase,
  performanceTiming,
  runPerformanceSample,
} from "../src/performance-evaluate.ts";

it.each([
  "success",
  "ownership-save-failure",
  "upload-failure",
  "flow-failure",
  "defect",
  "interruption",
  "timeout",
  "cleanup-failure",
  "cleanup-save-failure",
] as const)("owns disposable cleanup through %s", async (mode) => {
  const actions: Array<string> = [];

  const target: typeof PerformanceTarget.Type = {
    label: "candidate",
    name: `effect-agent-perf-${"a".repeat(32)}-candidate`,
    url: "https://example.test",
    directory: "/unused",
    sourceCommit: "a".repeat(40),
    cleanupRequired: false,
    cleanupComplete: false,
  };

  const fail = EvaluationError.make({ stage: "test", message: "expected" });

  const deployment = withPerformanceDeployment(
    target,
    mode === "flow-failure"
      ? Effect.fail(fail)
      : mode === "defect"
        ? Effect.die("fixture defect")
        : mode === "interruption"
          ? Effect.interrupt
          : mode === "timeout"
            ? Effect.never.pipe(
                Effect.timeout("1 millis"),
                Effect.mapError(() => fail),
              )
            : Effect.void,
  );

  expectTypeOf<Effect.Services<typeof deployment>>().toEqualTypeOf<
    PerformanceDeployment | PerformanceOwnership
  >();
  expectTypeOf<Effect.Error<typeof deployment>>().toEqualTypeOf<EvaluationError>();

  const exit = await Effect.runPromiseExit(
    deployment.pipe(
      Effect.provideService(PerformanceOwnership, {
        saveTarget: (saved) =>
          Effect.sync(() => {
            actions.push(saved.cleanupComplete ? "save-cleanup" : "save-ownership");
          }).pipe(
            Effect.andThen(
              mode === (saved.cleanupComplete ? "cleanup-save-failure" : "ownership-save-failure")
                ? Effect.fail(fail)
                : Effect.void,
            ),
          ),
      }),
      Effect.provideService(PerformanceDeployment, {
        exists: () => Effect.succeed(false),
        deploy: () =>
          Effect.sync(() => actions.push("deploy")).pipe(
            Effect.andThen(mode === "upload-failure" ? Effect.fail(fail) : Effect.void),
          ),
        remove: () =>
          Effect.sync(() => actions.push("remove")).pipe(
            Effect.andThen(mode === "cleanup-failure" ? Effect.fail(fail) : Effect.void),
          ),
      }),
    ),
  );

  expect(actions).toEqual(
    mode === "ownership-save-failure"
      ? ["save-ownership"]
      : mode === "cleanup-failure"
        ? ["save-ownership", "deploy", "remove"]
        : ["save-ownership", "deploy", "remove", "save-cleanup"],
  );
  expect(Exit.isSuccess(exit)).toBe(mode === "success");
});

it("refuses existing Workers without deployment or deletion", async () => {
  const actions: Array<string> = [];

  const target: typeof PerformanceTarget.Type = {
    label: "candidate",
    name: `effect-agent-perf-${"a".repeat(32)}-candidate`,
    url: "https://example.test",
    directory: "/unused",
    sourceCommit: "a".repeat(40),
    cleanupRequired: false,
    cleanupComplete: false,
  };

  const exit = await Effect.runPromiseExit(
    withPerformanceDeployment(target, Effect.void).pipe(
      Effect.provideService(PerformanceOwnership, {
        saveTarget: () =>
          Effect.sync(() => {
            actions.push("save");
          }),
      }),
      Effect.provideService(PerformanceDeployment, {
        exists: () => Effect.succeed(true),
        deploy: () =>
          Effect.sync(() => {
            actions.push("deploy");
          }),
        remove: () =>
          Effect.sync(() => {
            actions.push("remove");
          }),
      }),
    ),
  );

  expect(Exit.isFailure(exit)).toBe(true);
  expect(actions).toEqual([]);
});

/** Offline transport fixture only; this does not constitute deployed/live evidence. */
it("verifies tool consumption, fresh/warm/recovery, exact identity, timing and finalizers through workerd", async () => {
  const bundled = await build({
    entryPoints: [fileURLToPath(new URL("../src/performance-worker.ts", import.meta.url).href)],
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    external: ["cloudflare:*", "node:*"],
    conditions: ["workerd", "worker", "browser"],
    define: {
      PERFORMANCE_SOURCE_COMMIT: JSON.stringify("a".repeat(40)),
      PERFORMANCE_DIRTY: "false",
      PERFORMANCE_FIXTURE_DIGEST: JSON.stringify("fixture-test"),
    },
  });

  const script = bundled.outputFiles[0]?.text;

  if (script === undefined) throw new Error("No bundle");
  let countedInput = 0;
  const calls = new Map<number, number>();

  const runtime = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script,
      modulesRoot: "/",
      compatibilityDate: "2026-08-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { PERFORMANCE_THREADS: { className: "PerformanceThread", useSQLite: true } },
      bindings: {
        PERFORMANCE_TOKEN: "test-token",
        OPENAI_API_KEY: "test-only",
        PERFORMANCE_MODEL: "gpt-6-astra",
        PERFORMANCE_RUN: "offline",
        PERFORMANCE_SAMPLES: "1",
        PERFORMANCE_VERSION: { id: "offline-only" },
      },
      outboundService: async (request) => {
        const json = await request.text();

        if (new URL(request.url).hostname !== "api.openai.com")
          throw new Error("Unexpected outbound host");
        if (request.url.endsWith("/input_tokens")) {
          countedInput = Math.ceil(json.length / 4);

          return Response.json({ object: "response.input_tokens", input_tokens: countedInput });
        }

        const phase = Math.max(
          ...[...json.matchAll(/Order update (\d+)/g)].map((match) => Number(match[1])),
        );

        const ordinal = calls.get(phase) ?? 0;

        calls.set(phase, ordinal + 1);
        const text = JSON.stringify(expectedPerformanceOutput(phase));

        const output =
          ordinal === 0
            ? ["read_price", "read_stock"].map((name, index) => ({
                type: "function_call",
                id: `fc_${phase}_${index}`,
                call_id: `call_${phase}_${index}`,
                name,
                arguments: JSON.stringify({ sku: "lamp" }),
                status: "completed",
              }))
            : [
                {
                  type: "message",
                  id: `msg_${phase}`,
                  role: "assistant",
                  status: "completed",
                  content: [{ type: "output_text", text, annotations: [] }],
                },
              ];

        const events = output.flatMap((item, output_index) => [
          { type: "response.output_item.added", output_index, item },
          ...(item.type === "function_call"
            ? [
                {
                  type: "response.function_call_arguments.delta",
                  output_index,
                  item_id: item.id,
                  delta: JSON.stringify({ sku: "lamp" }),
                },
                {
                  type: "response.function_call_arguments.done",
                  output_index,
                  item_id: item.id,
                  arguments: JSON.stringify({ sku: "lamp" }),
                },
              ]
            : [
                {
                  type: "response.output_text.delta",
                  output_index,
                  item_id: item.id,
                  content_index: 0,
                  delta: text,
                },
              ]),
          { type: "response.output_item.done", output_index, item },
        ]);

        return new Response(
          [
            ...events,
            {
              type: "response.completed",
              response: {
                id: `resp_${phase}_${ordinal}`,
                object: "response",
                model: "gpt-6-astra",
                created_at: 1,
                status: "completed",
                service_tier: "default",
                output,
                usage: {
                  input_tokens: countedInput,
                  output_tokens: 100,
                  total_tokens: countedInput + 100,
                  input_tokens_details: { cached_tokens: 0 },
                  output_tokens_details: { reasoning_tokens: 0 },
                },
              },
            },
          ]
            .map(
              (event, sequence_number) =>
                `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
            )
            .join(""),
          { headers: { "content-type": "text/event-stream" } },
        );
      },
    }),
  );

  try {
    expect((await runtime.dispatchFetch("https://eval.test/identity")).status).toBe(401);

    const localFetch: typeof globalThis.fetch = async (input, init) => {
      const request = new Request(input, init);

      const response = await runtime.dispatchFetch(request.url, {
        method: request.method,
        headers: Object.fromEntries(request.headers),
        ...(request.method === "POST" ? { body: await request.text() } : {}),
      });

      return new Response(response.body, {
        status: response.status,
        headers: Object.fromEntries(response.headers),
      });
    };

    const identity = Schema.decodeUnknownSync(PerformanceIdentity)(
      await (
        await localFetch("https://eval.test/identity", {
          headers: { authorization: "Bearer test-token" },
        })
      ).json(),
    );

    const evidence = await Effect.runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const outputDirectory = yield* fs.makeTempDirectoryScoped({ prefix: "performance-test-" });

        const result = yield* runPerformanceSample({
          url: "https://eval.test",
          token: Redacted.make("test-token"),
          identity,
          target: "offline-scripted",
          sample: 0,
          outputDirectory,
        });

        const snapshots = yield* Effect.forEach([0, 1, 2], (phase) =>
          fs
            .readFileString(`${outputDirectory}/phase-${phase}-snapshot.json`)
            .pipe(
              Effect.flatMap(
                Schema.decodeUnknownEffect(Schema.fromJsonString(PerformanceSnapshot)),
              ),
            ),
        );

        return { result, snapshots };
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.merge(
            NodeServices.layer,
            FetchHttpClient.layer.pipe(
              Layer.provide(Layer.succeed(FetchHttpClient.Fetch, localFetch)),
            ),
          ),
        ),
      ),
    );

    expect(evidence.result.failure).toBeNull();
    expect(evidence.result.phases.flatMap((phase) => phase.failures)).toEqual([]);
    expect(evidence.result.passed).toBe(true);
    expect(evidence.result.closeRequested).toBe(true);
    expect([...calls.values()]).toEqual([2, 2, 2]);
    const final = evidence.snapshots[2];

    if (final === undefined) throw new Error("Missing final evidence");

    const settlement = final.records.findLast(
      ({ record }) => record.payload._tag === "SubmissionSettled",
    )?.record.payload;

    if (settlement?._tag !== "SubmissionSettled") throw new Error("Missing settlement");

    const rejected = await Effect.runPromise(
      gradePerformancePhase(
        {
          ...final,
          audits: final.audits.filter((audit) => audit.phase !== 2),
          records: final.records.map((envelope) =>
            envelope.record.payload._tag === "ToolCallSettled"
              ? {
                  ...envelope,
                  record: {
                    ...envelope.record,
                    payload: { ...envelope.record.payload, result: {} },
                  },
                }
              : envelope,
          ),
        },
        2,
        { submissionId: settlement.submissionId },
        evidence.snapshots[1]?.incarnation ?? 0,
      ),
    );

    expect(rejected.passed).toBe(false);
    expect(rejected.failures).toContain("Canonical read_price payload differs from the fixture");
    expect(rejected.failures).toContain("Canonical read_stock payload differs from the fixture");
    expect(rejected.failures).toContain(
      "Subsequent real provider request must consume both current tool results",
    );

    const firstRequest = final.audits.find(
      (audit) => audit.phase === 2 && audit.kind === "request",
    );

    const missingHistory = await Effect.runPromise(
      gradePerformancePhase(
        {
          ...final,
          audits: final.audits.map((audit) => {
            if (audit !== firstRequest) return audit;

            const request = Schema.decodeSync(
              Schema.fromJsonString(
                Schema.Struct({ input: Schema.Array(Schema.Record(Schema.String, Schema.Json)) }),
              ),
            )(audit.json);

            return {
              ...audit,
              json: JSON.stringify({
                ...request,
                input: request.input.filter((part) => part.role !== "assistant"),
              }),
            };
          }),
        },
        2,
        { submissionId: settlement.submissionId },
        evidence.snapshots[1]?.incarnation ?? 0,
      ),
    );

    expect(missingHistory.passed).toBe(false);
    expect(missingHistory.failures).toContain(
      "Initial provider request must retain the previous canonical assistant order output",
    );
    expect(final.incarnation).toBeGreaterThan(evidence.snapshots[1]?.incarnation ?? 0);
    expect(final.events.filter((event) => event.kind === "provider-http-dispatch")).toHaveLength(6);
    expect(performanceTiming({ ...final, events: [] }, 0).toolResultsCommitMillis).toBeNull();

    const firstDispatch = final.events.find(
      (event) => event.phase === 0 && event.kind === "provider-http-dispatch",
    );

    const withoutFirstDelta = {
      ...final,
      events: final.events.filter(
        (event) =>
          !(
            event.kind === "first-provider-delta" &&
            event.request === firstDispatch?.request &&
            event.incarnation === firstDispatch?.incarnation
          ),
      ),
    };

    expect(performanceTiming(withoutFirstDelta, 0).providerDispatchToFirstDeltaMillis).toBeNull();
  } finally {
    await runtime.dispose();
  }
}, 45_000);
