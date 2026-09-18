import { NodeRuntime, NodeServices } from "@effect/platform-node";
import {
  Clock,
  Config,
  DateTime,
  Console,
  Effect,
  FileSystem,
  ManagedRuntime,
  Option,
  Redacted,
  Schema,
  Schedule,
  Semaphore,
} from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

const Conversation = Schema.Struct({
  id: Schema.String,
  messages: Schema.Array(Schema.String),
  checks: Schema.Record(Schema.String, Schema.String),
});

const Conversations = Schema.Array(Conversation);
const JsonObject = Schema.Record(Schema.String, Schema.Json);
const TokenCount = Schema.Struct({ input_tokens: Schema.Natural });

const ProviderCompletion = Schema.Struct({
  type: Schema.Literals(["response.completed", "response.failed", "response.incomplete"]),
  response: Schema.Struct({
    model: Schema.String,
    usage: Schema.NullOr(
      Schema.Struct({ input_tokens: Schema.Natural, output_tokens: Schema.Natural }),
    ),
    output: Schema.Array(JsonObject),
  }),
});

const Snapshot = Schema.Struct({
  conversationId: Schema.String,
  pending: Schema.Natural,
  messages: Schema.Array(Schema.Struct({ role: Schema.String, text: Schema.String })),
  scouts: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        id: Schema.String,
        sourceSequence: Schema.optionalKey(Schema.Natural),
      }),
    ),
  ),
});

const WorkerDetail = Schema.Struct({ state: Schema.String });

const CaptureUsage = Schema.Struct({
  calls: Schema.Natural,
  browserCalls: Schema.Natural,
  prompts: Schema.Natural,
  estimatedCostMicrousd: Schema.Natural,
  reservedCostMicrousd: Schema.Natural,
  usage: Schema.Array(Schema.Json),
});

const RpcReply = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  exit: Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
    Schema.Struct({ _tag: Schema.Literal("Failure"), cause: Schema.Unknown }),
  ]),
});

class CaptureError extends Schema.TaggedError<CaptureError>()("TravelCaptureError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const io = <A>(operation: string, run: (signal: AbortSignal) => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) => CaptureError.make({ message: operation, cause }),
  });

export const command = Command.make(
  "travel-capture",
  {
    output: Flag.String("output-dir"),
    caseId: Flag.String("case").pipe(Flag.withDefault("all")),
    messages: Flag.Int("messages").pipe(Flag.withDefault(10)),
    maxCalls: Flag.Int("max-calls").pipe(Flag.withDefault(300)),
    maxCost: Flag.Finite("max-cost-usd").pipe(Flag.withDefault(20)),
    dryRun: Flag.Boolean("dry-run").pipe(Flag.withDefault(false)),
    resumeFrom: Flag.String("resume-from").pipe(Flag.optional),
  },
  Effect.fn("TravelCapture.run")(function* (options) {
    const fs = yield* FileSystem.FileSystem;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const root = (yield* spawner.string(
      ChildProcess.make("git", ["rev-parse", "--show-toplevel"]),
    )).trim();

    const revision = (yield* spawner.string(
      ChildProcess.make("git", ["rev-parse", "HEAD"]),
    )).trim();

    const cases = yield* fs
      .readFileString(`${root}/tooling/context-continuity-eval/fixtures/travel-conversations.json`)
      .pipe(
        Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Conversations))),
        Effect.map((all) =>
          all.filter((item) => options.caseId === "all" || item.id === options.caseId),
        ),
      );

    if (
      cases.length === 0 ||
      options.messages < 1 ||
      options.messages > 10 ||
      options.maxCalls < 1 ||
      options.maxCalls > 300 ||
      options.maxCost <= 0 ||
      options.maxCost > 30
    )
      return yield* new CaptureError({
        message: "Choose an existing case, 1–10 messages, 1–300 calls and a $0–30 ceiling.",
      });
    if (options.dryRun) {
      yield* Console.log(
        JSON.stringify(
          {
            revision,
            cases: cases.map((c) => ({
              id: c.id,
              messages: c.messages.slice(0, options.messages),
            })),
            model: "gpt-5.6-luna",
            browser: "Cloudflare Browser Run REST",
            maxCalls: options.maxCalls,
            maxCostUsd: options.maxCost,
            output: options.output,
          },
          null,
          2,
        ),
      );

      return;
    }
    if (!(yield* Config.Boolean("EFFECT_AGENT_LIVE").pipe(Config.withDefault(false))))
      return yield* new CaptureError({ message: "Set EFFECT_AGENT_LIVE=1 for paid capture." });
    const openAiKey = yield* Config.Redacted("OPENAI_API_KEY");
    const cloudflareKey = yield* Config.Redacted("CLOUDFLARE_API_TOKEN");
    const account = yield* Config.String("CLOUDFLARE_ACCOUNT_ID");

    if (yield* fs.exists(options.output))
      return yield* new CaptureError({
        message: "Output directory already exists; retain each attempt separately.",
      });
    let prior: typeof CaptureUsage.Type | undefined;

    if (Option.isSome(options.resumeFrom)) {
      const previous = options.resumeFrom.value;

      prior = yield* fs
        .readFileString(`${previous}/usage.json`)
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(CaptureUsage))));

      const manifest = yield* fs
        .readFileString(`${previous}/manifest.json`)
        .pipe(
          Effect.flatMap(
            Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ cases: Conversations }))),
          ),
        );

      if (
        prior.reservedCostMicrousd !== 0 ||
        prior.calls !== prior.usage.length ||
        JSON.stringify(manifest.cases) !== JSON.stringify(cases)
      ) {
        return yield* CaptureError.make({
          message: "Resume requires settled provider accounting and identical scenarios",
        });
      }
      yield* fs.copy(previous, options.output, { overwrite: false });
    } else {
      yield* fs.makeDirectory(options.output, { recursive: true });
    }

    const write = (name: string, value: unknown) =>
      fs.writeFileString(`${options.output}/${name}`, JSON.stringify(value, null, 2));

    yield* write("manifest.json", {
      version: 1,
      revision,
      model: "gpt-5.6-luna",
      reasoning: "low",
      appPackages: "published beta.88",
      host: "local Miniflare; live OpenAI search and Cloudflare Browser Run",
      options,
      resumedFrom: Option.getOrNull(options.resumeFrom),
      startedAt: DateTime.formatIso(yield* DateTime.now),
      cases,
    });

    const proxyRuntime = yield* Effect.acquireRelease(
      Effect.sync(() => ManagedRuntime.make(NodeServices.layer)),
      (runtime) => Effect.promise(() => runtime.dispose()),
    );

    const permits = yield* Semaphore.make(1);
    let phase = "setup";
    let calls = prior?.calls ?? 0;
    let browserCalls = prior?.browserCalls ?? 0;
    let prompts = prior?.prompts ?? 0;
    let spent = prior?.estimatedCostMicrousd ?? 0;
    let unresolved = 0;
    let stopped = false;
    const usage: Array<Schema.Json> = [...(prior?.usage ?? [])];

    const progress = () =>
      Effect.suspend(() =>
        write("usage.json", {
          calls,
          browserCalls,
          prompts,
          estimatedCostMicrousd: spent,
          reservedCostMicrousd: unresolved,
          stopped,
          usage,
        }),
      );

    const forward = Effect.fn("TravelCapture.forward")(function* (request: {
      readonly url: string;
      readonly text: () => Promise<string>;
    }) {
      const url = new URL(request.url);
      const body = yield* io("Read local request", () => request.text());

      if (url.hostname === "capture-eval.invalid") {
        const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(body);
        const number = ++prompts;

        yield* write(`prompt-${String(number).padStart(4, "0")}.json`, {
          phase,
          number,
          prompt: payload,
        });

        return new Response("Saved");
      }
      if (url.hostname === "browser-eval.invalid") {
        if (url.pathname !== "/markdown" || browserCalls >= 300 || stopped)
          return new Response("Browser evaluation allowance exhausted", { status: 429 });
        const number = ++browserCalls;
        const started = yield* Clock.currentTimeMillis;

        const response = yield* io("Cloudflare page read failed", (signal) =>
          fetch(
            `https://api.cloudflare.com/client/v4/accounts/${account}/browser-rendering/markdown`,
            {
              method: "POST",
              headers: {
                authorization: `Bearer ${Redacted.value(cloudflareKey)}`,
                "content-type": "application/json",
              },
              body,
              signal,
            },
          ),
        );

        const text = yield* io("Read browser response", () => response.text());

        yield* write(`browser-${number}.json`, {
          phase,
          request: yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(body),
          status: response.status,
          elapsedMs: (yield* Clock.currentTimeMillis) - started,
          body: text,
        });

        return new Response(text, {
          status: response.status,
          headers: { "content-type": response.headers.get("content-type") ?? "application/json" },
        });
      }
      if (url.hostname !== "api.openai.com" || url.pathname !== "/v1/responses")
        return new Response("Unexpected eval egress", { status: 403 });

      return yield* Effect.gen(function* () {
        if (stopped || calls >= options.maxCalls)
          return yield* new CaptureError({ message: "Live-call allowance exhausted" });
        const original = yield* Schema.decodeEffect(Schema.fromJsonString(JsonObject))(body);

        if (
          original.model !== "gpt-5.6-luna" ||
          original.store !== false ||
          original.max_output_tokens !== 4096 ||
          original.service_tier !== "default"
        )
          return yield* new CaptureError({
            message: "Provider request escaped live capture bounds",
          });
        const payload = { ...original, truncation: "disabled" };

        const countBody = Object.fromEntries(
          ["model", "input", "instructions", "tools", "tool_choice", "reasoning", "text"]
            .filter((key) => original[key] !== undefined)
            .map((key) => [key, original[key]]),
        );

        const countResponse = yield* io("Token preflight failed", (signal) =>
          fetch("https://api.openai.com/v1/responses/input_tokens", {
            method: "POST",
            headers: {
              authorization: `Bearer ${Redacted.value(openAiKey)}`,
              "content-type": "application/json",
            },
            body: JSON.stringify(countBody),
            signal,
          }),
        );

        if (!countResponse.ok)
          return yield* new CaptureError({
            message: `Token preflight HTTP ${countResponse.status}`,
          });
        const countText = yield* io("Read token preflight", () => countResponse.text());

        const { input_tokens: inputTokens } = yield* Schema.decodeEffect(
          Schema.fromJsonString(TokenCount),
        )(countText);

        if (inputTokens > 850_000)
          return yield* new CaptureError({
            message: "Captured context exceeded 850k input tokens",
          });
        // Built-in search adds input tokens after preflight. Reserve against the model's
        // full input allowance, including long-context rates and two search calls.
        const reservation = 600_000;

        if (spent + reservation > options.maxCost * 1_000_000)
          return yield* new CaptureError({
            message: "Insufficient capture budget for the full next request",
          });
        const number = ++calls;

        unresolved = reservation;
        const capturedPhase = phase;

        yield* write(`request-${String(number).padStart(4, "0")}.json`, {
          phase: capturedPhase,
          number,
          inputTokens,
          payload,
        });
        yield* progress();
        const started = yield* Clock.currentTimeMillis;

        const response = yield* io("OpenAI request failed; reservation retained", (signal) =>
          fetch(request.url, {
            method: "POST",
            headers: {
              authorization: `Bearer ${Redacted.value(openAiKey)}`,
              "content-type": "application/json",
            },
            body: JSON.stringify(payload),
            signal,
          }),
        );

        const text = yield* io("Read OpenAI stream; reservation retained", () => response.text());

        yield* fs.writeFileString(
          `${options.output}/response-${String(number).padStart(4, "0")}.sse`,
          text,
        );

        const completed = text
          .split("\n")
          .filter((line) => line.startsWith("data: "))
          .map((line) =>
            Schema.decodeOption(Schema.fromJsonString(ProviderCompletion))(line.slice(6)),
          )
          .find(Option.isSome);

        if (!response.ok || completed === undefined || completed.value.response.usage === null) {
          stopped = true;
          yield* progress();

          return yield* new CaptureError({
            message: `Unmetered or failed OpenAI response HTTP ${response.status}`,
          });
        }
        const result = completed.value.response;
        const tokens = result.usage;

        if (tokens === null) return yield* new CaptureError({ message: "Missing usage" });
        const searches = result.output.filter((item) => item.type === "web_search_call").length;

        const cost = Math.ceil(
          tokens.input_tokens * (tokens.input_tokens > 272_000 ? 0.5 : 0.25) +
            tokens.output_tokens * (tokens.input_tokens > 272_000 ? 1.8 : 1.2) +
            searches * 30_000,
        );

        spent += cost;
        unresolved = 0;
        usage.push({
          number,
          phase: capturedPhase,
          status: completed.value.type,
          model: result.model,
          inputTokens: tokens.input_tokens,
          outputTokens: tokens.output_tokens,
          searches,
          estimatedCostMicrousd: cost,
          elapsedMs: (yield* Clock.currentTimeMillis) - started,
        });
        if (
          completed.value.type !== "response.completed" ||
          cost > reservation ||
          tokens.output_tokens > 4096
        )
          stopped = true;
        yield* progress();
        yield* Console.log(
          `${capturedPhase}: model call ${number}, ${tokens.input_tokens} input tokens, ${searches} searches`,
        );

        return new Response(text, {
          status: response.status,
          headers: { "content-type": response.headers.get("content-type") ?? "text/event-stream" },
        });
      }).pipe(permits.withPermits(1));
    });

    const bundled = yield* io("Build local planner recorder", () =>
      build({
        entryPoints: [`${root}/examples/travel-planner/test/fixtures/live-eval-worker.ts`],
        bundle: true,
        write: false,
        format: "esm",
        target: "es2022",
        platform: "browser",
        conditions: ["workerd", "worker", "browser"],
        external: ["cloudflare:*", "node:*"],
        alias: {
          "@tanstack/react-start/server-entry": `${root}/examples/travel-planner/test/fixtures/start.ts`,
        },
        banner: {
          js: 'import { createRequire } from "node:module"; const require = createRequire("/fixture.mjs");',
        },
        logLevel: "silent",
      }),
    );

    const output = bundled.outputFiles?.[0];

    if (output === undefined) return yield* new CaptureError({ message: "Missing worker bundle" });
    const token = "local-travel-eval-only";

    const runtime = yield* Effect.acquireRelease(
      Effect.sync(
        () =>
          new Miniflare(
            convertV4MiniflareOptions({
              modules: true,
              script: output.text,
              modulesRoot: "/",
              compatibilityDate: "2026-07-01",
              compatibilityFlags: ["nodejs_compat"],
              bindings: { PLANNER_TOKEN: token },
              durableObjects: {
                ACCOUNT_THREADS: { className: "TravelPlannerThread", useSQLite: true },
              },
              r2Buckets: ["APP_BUILDS"],
              resourcePersistencePath: `${options.output}/state`,
              outboundService: (request) =>
                proxyRuntime.runPromise(
                  forward(request).pipe(
                    Effect.catchCause((cause) => {
                      stopped = true;

                      return Console.error(String(cause)).pipe(
                        Effect.andThen(progress()),
                        Effect.as(new Response("Capture transport failed", { status: 503 })),
                      );
                    }),
                  ),
                ),
            }),
          ),
      ),
      (instance) => Effect.promise(() => instance.dispose()),
    );

    const rpc = Effect.fn("TravelCapture.rpc")(function* (tag: string, payload: Schema.Json) {
      const call = io(`Planner RPC ${tag}`, () =>
        runtime.dispatchFetch("http://planner/api/rpc", {
          method: "POST",
          headers: { authorization: `Bearer ${token}`, "content-type": "application/ndjson" },
          body: `${JSON.stringify({ _tag: "Request", id: "1", tag, payload, headers: [] })}\n`,
        }),
      );

      const response = yield* tag.startsWith("Get")
        ? call.pipe(Effect.retry({ times: 2, schedule: Schedule.exponential("500 millis") }))
        : call;

      const text = yield* io("Read planner RPC", () => response.text());

      const reply = yield* Schema.decodeEffect(Schema.fromJsonString(RpcReply))(
        text.trim().split("\n")[0] ?? "",
      );

      if (reply.exit._tag !== "Success")
        return yield* new CaptureError({
          message: `${tag} failed: ${JSON.stringify(reply.exit.cause).slice(0, 1000)}`,
        });

      return reply.exit.value;
    });

    const snapshot = (id: string) =>
      rpc("GetPlanner", { conversationId: id }).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Snapshot)),
      );

    yield* Effect.gen(function* () {
      for (const scenario of cases) {
        for (const [index, message] of scenario.messages.slice(0, options.messages).entries()) {
          if (yield* fs.exists(`${options.output}/${scenario.id}-${index + 1}-snapshot.json`))
            continue;
          phase = `${scenario.id}/${index + 1}`;
          yield* Console.log(`Starting ${phase}`);
          yield* rpc("SendMessage", {
            conversationId: scenario.id,
            message,
            selectedTripId: null,
            requestId: `eval-${scenario.id}-${index + 1}`,
            settings: { model: "gpt-5.6-luna", reasoningEffort: "low", fast: false },
          });
          const deadline = (yield* Clock.currentTimeMillis) + 900_000;
          let settled = 0;

          while (settled < 2) {
            yield* Effect.sleep("2 seconds");
            const state = yield* snapshot(scenario.id);

            const statuses = yield* Effect.forEach(
              state.scouts ?? [],
              (worker) =>
                rpc("GetPlannerWorker", {
                  conversationId: scenario.id,
                  workerId: worker.id,
                  ...(worker.sourceSequence === undefined
                    ? {}
                    : { sourceSequence: worker.sourceSequence }),
                }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(WorkerDetail))),
              { concurrency: 3 },
            );

            const idle =
              state.pending === 0 &&
              statuses.every((item) =>
                ["completed", "failed", "aborted", "idle"].includes(item.state),
              );

            settled = idle ? settled + 1 : 0;
            if (stopped || (yield* Clock.currentTimeMillis) > deadline) {
              yield* write(`${scenario.id}-${index + 1}-incomplete.json`, { state, statuses });

              return yield* new CaptureError({ message: `Capture did not settle: ${phase}` });
            }
          }
          const full = yield* rpc("GetPlanner", { conversationId: scenario.id });

          yield* write(`${scenario.id}-${index + 1}-snapshot.json`, full);
          const state = yield* Schema.decodeUnknownEffect(Snapshot)(full);
          const threads = [state.conversationId, ...(state.scouts ?? []).map((s) => s.id)];

          for (const [slot, thread] of threads.entries()) {
            const response = yield* io("Export canonical planner history", () =>
              runtime.dispatchFetch(
                `http://planner/__eval/journal?thread=${encodeURIComponent(thread)}`,
                { headers: { authorization: `Bearer ${token}` } },
              ),
            );

            const text = yield* io("Read canonical planner history", () => response.text());

            if (!response.ok)
              return yield* new CaptureError({ message: `Journal export HTTP ${response.status}` });
            yield* fs.writeFileString(
              `${options.output}/${scenario.id}-${index + 1}-thread-${slot}.json`,
              text,
            );
          }
          yield* Console.log(`Saved ${phase}; ${threads.length} threads`);
        }
      }
    }).pipe(Effect.ensuring(progress().pipe(Effect.orDie)));
    yield* write("complete.json", {
      cases: cases.map((c) => c.id),
      messagesPerCase: options.messages,
      calls,
      browserCalls,
      estimatedCostMicrousd: spent,
    });
  }),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: "1.0.0" }).pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );
