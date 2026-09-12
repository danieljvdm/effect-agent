import { ThreadId } from "@effect-agent/core/Identifiers";
import {
  DurableObjectContext,
  ThreadObjectIdentity,
  ThreadObjectNamespace,
} from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/CloudflareThreadClient";
import * as ThreadObject from "@effect-agent/platform-cloudflare/ThreadObject";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { BrowserCrypto } from "@effect/platform-browser";
import { Context, Effect, Layer, Redacted, Ref, Schema } from "effect";
import { DurableObject, WorkerEnvironment } from "effect-cf";
import { FetchHttpClient } from "effect/unstable/http";

import { EvaluationError, type ModelUsage } from "./contracts.ts";
import { readLog } from "./host-evidence.ts";
import { makeLiveClient } from "./live-model.ts";
import {
  PERFORMANCE_BUDGET,
  PERFORMANCE_FIXTURE,
  PerformanceIdentity,
  PerformancePhase,
  PerformanceSnapshot,
  PerformanceState,
  PerformanceEvent,
  emptyPerformanceUsage,
  performanceDefinition,
  performanceDefinitions,
  performanceMessage,
  performanceSettings,
  performanceToolkit,
} from "./performance-contracts.ts";
import { RequestAudit, RequestAuditSink } from "./request-audit.ts";

declare const PERFORMANCE_SOURCE_COMMIT: string;
declare const PERFORMANCE_DIRTY: boolean;
declare const PERFORMANCE_FIXTURE_DIGEST: string;
declare global {
  namespace Cloudflare {
    interface Env {
      PERFORMANCE_THREADS: DurableObjectNamespace<PerformanceThread>;
      PERFORMANCE_TOKEN: string;
      PERFORMANCE_VERSION: { id: string; tag?: string; timestamp?: string };
      PERFORMANCE_MODEL: string;
      PERFORMANCE_RUN: string;
      PERFORMANCE_SAMPLES: string;
      OPENAI_API_KEY: string;
    }
  }
}

const identityFor = (env: Cloudflare.Env) =>
  Schema.decodeUnknownEffect(PerformanceIdentity)({
    sourceCommit: PERFORMANCE_SOURCE_COMMIT,
    dirtyWorkingTree: PERFORMANCE_DIRTY,
    fixture: PERFORMANCE_FIXTURE,
    fixtureDigest: PERFORMANCE_FIXTURE_DIGEST,
    deploymentId: env.PERFORMANCE_VERSION.id,
    model: env.PERFORMANCE_MODEL,
    maxCostMicrousd: PERFORMANCE_BUDGET,
    maxModelCalls: 18,
    maxInputTokens: 8_192,
    settings: performanceSettings,
  });

class PerformanceHost extends Context.Service<
  PerformanceHost,
  {
    prepare(phase: number): Effect.Effect<void, EvaluationError>;
    readonly snapshot: Effect.Effect<typeof PerformanceSnapshot.Type, EvaluationError>;
    readonly close: Effect.Effect<void>;
  }
>()("example/PerformanceHost") {}
const callbacks = new WeakMap<DurableObjectState, (kind: string) => Effect.Effect<void>>();

const application = Layer.unwrap(
  Effect.gen(function* () {
    const { ctx } = yield* DurableObjectContext;
    const { threadId } = yield* ThreadObjectIdentity;
    const env = yield* WorkerEnvironment;
    const identity = yield* identityFor(env);

    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS performance_evidence (path TEXT PRIMARY KEY, value TEXT NOT NULL)",
    );

    const put = (path: string, value: string) =>
      ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO performance_evidence(path,value) VALUES (?,?)",
        path,
        value,
      );

    const read = (prefix: string) =>
      ctx.storage.sql
        .exec<{ value: string }>(
          "SELECT value FROM performance_evidence WHERE path LIKE ? ORDER BY path",
          prefix,
        )
        .toArray();

    const stored = read("state")[0]?.value;

    let state: typeof PerformanceState.Type =
      stored === undefined
        ? {
            identity,
            phase: 0,
            incarnation: 0,
            usage: emptyPerformanceUsage,
            aborted: false,
            closed: false,
          }
        : yield* Schema.decodeEffect(Schema.fromJsonString(PerformanceState))(stored);

    if (JSON.stringify(identity) !== JSON.stringify(state.identity))
      return yield* EvaluationError.make({
        stage: "source",
        message: "Performance identity changed across incarnations",
      });
    state = { ...state, incarnation: state.incarnation + 1 };

    const persist = () =>
      put("state", Schema.encodeSync(Schema.fromJsonString(PerformanceState))(state));

    persist();
    let index = read("event-%").length;

    // Native boundary clock: workerd freezes time between I/O. These are wall marks,
    // not CPU measurements, and cannot measure synchronous preparation below that resolution.
    const mark = (kind: string, request: number | null = null) => {
      if (index >= 2_048) throw new Error("Performance evidence bound exceeded");

      const event = {
        index: ++index,
        phase: state.phase,
        incarnation: state.incarnation,
        kind,
        atMillis: Date.now(),
        request,
      };

      put(
        `event-${index.toString().padStart(5, "0")}`,
        Schema.encodeSync(Schema.fromJsonString(PerformanceEvent))(event),
      );
    };

    mark("incarnation-start");
    const phase = yield* Ref.make(state.phase);
    let usage: Effect.Effect<ModelUsage> = Effect.succeed(state.usage);
    let requestNumber = state.usage.calls + 1;

    const auditedFetch: typeof globalThis.fetch = (input, init) => {
      if (state.closed) return Promise.reject(new Error("Performance host closed"));
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;

      if (new URL(url).pathname === "/v1/responses") mark("provider-http-dispatch", requestNumber);

      return globalThis.fetch(input, init);
    };

    const live = yield* makeLiveClient({
      model: identity.model,
      maxCostMicrousd: PERFORMANCE_BUDGET,
      maxModelCalls: 18,
      maxInputTokens: 8_192,
      phase,
      initialUsage: state.usage,
      observe: (kind, request) =>
        Effect.sync(() => {
          requestNumber = request;
          mark(kind, request);
        }),
    }).pipe(
      Effect.provide(
        Layer.merge(
          Layer.succeed(RequestAuditSink, {
            write: (event) =>
              Effect.gen(function* () {
                put(
                  `audit-${event.request.toString().padStart(3, "0")}-${event.kind}`,
                  yield* Schema.encodeEffect(Schema.fromJsonString(RequestAudit))(event),
                );
                state = { ...state, usage: yield* usage };
                persist();
                mark(
                  event.kind === "request" ? "reservation-persisted" : "provider-completed",
                  event.request,
                );
              }).pipe(
                Effect.mapError(() =>
                  EvaluationError.make({
                    stage: "evidence",
                    message: "Cannot persist request accounting",
                  }),
                ),
              ),
          }),
          OpenAiClient.layer({ apiKey: Redacted.make(env.OPENAI_API_KEY) }).pipe(
            Layer.provide(
              FetchHttpClient.layer.pipe(
                Layer.provide(Layer.succeed(FetchHttpClient.Fetch, auditedFetch)),
              ),
            ),
          ),
        ),
      ),
    );

    usage = live.snapshot;

    const tools = performanceToolkit.toLayer({
      read_price: () =>
        Effect.gen(function* () {
          mark("tool:read_price:start");
          yield* Effect.sleep("20 millis");
          mark("tool:read_price:end");

          return { unitPriceCents: 3_700, evidence: `price-${state.phase}-q7` };
        }),
      read_stock: () =>
        Effect.gen(function* () {
          mark("tool:read_stock:start");
          yield* Effect.sleep("20 millis");
          mark("tool:read_stock:end");

          return { availableUnits: 12, evidence: `stock-${state.phase}-m9` };
        }),
    });

    const host = ThreadObject.layer([
      {
        agent: performanceDefinition,
        model: OpenAiLanguageModel.model(identity.model, performanceSettings),
        definitions: performanceDefinitions(identity),
        attemptLayer: () =>
          Layer.effectDiscard(
            Effect.acquireRelease(
              Effect.sync(() => mark("attempt-start")),
              () => Effect.sync(() => mark("attempt-finalized")),
            ),
          ),
      },
    ]).pipe(
      Layer.provide(Layer.merge(tools, Layer.succeed(OpenAiClient.OpenAiClient, live.client))),
    );

    return Layer.effectContext(
      Effect.gen(function* () {
        const services = yield* Effect.context<ThreadObject.Services>();

        const hit = (kind: string) =>
          Effect.gen(function* () {
            mark(kind);
            if (kind === "turn:after-results-append" && state.phase === 2 && !state.aborted) {
              state = { ...state, aborted: true, usage: yield* usage };
              persist();
              mark("planned-abort-after-tool-commit");
              yield* Effect.promise(() => ctx.storage.sync());
              ctx.abort("performance fixture: committed tool batch recovery");
            }
          });

        callbacks.set(ctx, hit);

        return Context.add(services, PerformanceHost, {
          prepare: (next) =>
            Effect.gen(function* () {
              if (state.closed || next < state.phase || next > state.phase + 1)
                return yield* EvaluationError.make({
                  stage: "phase",
                  message: "Invalid performance phase",
                });
              if (next > state.phase) {
                const records = yield* readLog(threadId).pipe(
                  Effect.mapError(() =>
                    EvaluationError.make({ stage: "phase", message: "Previous order unavailable" }),
                  ),
                );

                if (
                  records.filter(({ record }) => record.payload._tag === "SubmissionSettled")
                    .length !== next
                )
                  return yield* EvaluationError.make({
                    stage: "phase",
                    message: "Previous order is not settled",
                  });
              }
              state = { ...state, phase: next };
              yield* Ref.set(phase, next);
              persist();
              mark("submission-ingress");
            }).pipe(Effect.provide(services)),
          snapshot: Effect.gen(function* () {
            const records = yield* readLog(threadId).pipe(
              Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed([])),
            );

            if (records.length > 1_024)
              return yield* EvaluationError.make({
                stage: "evidence",
                message: "Canonical fixture bound exceeded",
              });

            return {
              identity,
              phase: state.phase,
              incarnation: state.incarnation,
              events: yield* Effect.forEach(read("event-%"), (r) =>
                Schema.decodeEffect(Schema.fromJsonString(PerformanceEvent))(r.value),
              ),
              records,
              audits: yield* Effect.forEach(read("audit-%"), (r) =>
                Schema.decodeEffect(Schema.fromJsonString(RequestAudit))(r.value),
              ),
              usage: yield* usage,
              failure: yield* live.failure,
              databaseBytes: ctx.storage.sql.databaseSize,
            };
          }).pipe(
            Effect.provide(services),
            Effect.mapError(() =>
              EvaluationError.make({ stage: "evidence", message: "Performance snapshot failed" }),
            ),
          ),
          close: Effect.gen(function* () {
            state = { ...state, closed: true, usage: yield* usage };
            persist();
            yield* Effect.promise(() => ctx.storage.sync());
            ctx.abort("performance fixture: close before resource deletion");
          }),
        });
      }),
    ).pipe(Layer.provide(host));
  }),
);

export class PerformanceThread extends ThreadObject.make(application, {
  namespaceBinding: "PERFORMANCE_THREADS",
  deploymentId: "performance-cloudflare-v1",
  producerPrefix: "performance-eval",
  ownershipLeaseDuration: 2_000,
  leaseRenewalInterval: 500,
  alarmBackoffBase: 100,
  alarmBackoffCap: 1_000,
  maxQueueDepthPerLane: 1,
  maxInputBytes: 2_048,
  maxDatabaseBytes: 16 * 1024 * 1024,
  runtimeFailpoint: (ctx) => (location) =>
    Effect.suspend(() => callbacks.get(ctx)?.(location) ?? Effect.void),
  storageFailpoint: (ctx) => (location) =>
    Effect.suspend(() =>
      location === "ledger:admit:before" ||
      location === "ledger:admit:after" ||
      location === "ledger:claim:after" ||
      location === "append:before" ||
      location === "append:after"
        ? (callbacks.get(ctx)?.(location) ?? Effect.void)
        : Effect.void,
    ),
}) {
  prepare(phase: number) {
    return this[DurableObject.RunSymbol](
      Effect.flatMap(PerformanceHost, (host) => host.prepare(phase)),
    );
  }
  snapshot() {
    return this[DurableObject.RunSymbol](
      Effect.flatMap(PerformanceHost, (host) => host.snapshot).pipe(
        Effect.flatMap(Schema.encodeEffect(PerformanceSnapshot)),
      ),
    );
  }
  close() {
    return this[DurableObject.RunSymbol](Effect.flatMap(PerformanceHost, (host) => host.close));
  }
}

export default {
  fetch(request: Request, env: Cloudflare.Env): Promise<Response> {
    return Effect.runPromise(
      Effect.gen(function* () {
        if (
          !env.PERFORMANCE_TOKEN ||
          request.headers.get("authorization") !== `Bearer ${env.PERFORMANCE_TOKEN}`
        )
          return new Response("Unauthorized", { status: 401 });
        const identity = yield* identityFor(env);
        const url = new URL(request.url);

        if (url.pathname === "/identity") return Response.json(identity);

        const sample = yield* Schema.decodeUnknownEffect(
          Schema.FiniteFromString.check(
            Schema.isBetween({ minimum: 0, maximum: 2 }),
            Schema.isInt(),
          ),
        )(url.searchParams.get("sample"));

        const samples = yield* Schema.decodeEffect(
          Schema.FiniteFromString.check(
            Schema.isBetween({ minimum: 1, maximum: 3 }),
            Schema.isInt(),
          ),
        )(env.PERFORMANCE_SAMPLES);

        if (sample >= samples)
          return new Response("Sample outside deployment budget", { status: 400 });
        const name = `performance-${env.PERFORMANCE_RUN}-${sample}`;
        const stub = env.PERFORMANCE_THREADS.getByName(name);

        const rpc = <A>(f: () => Promise<A>) =>
          Effect.tryPromise({
            try: f,
            catch: () => EvaluationError.make({ stage: "host", message: "Performance RPC failed" }),
          });

        if (url.pathname === "/snapshot") return Response.json(yield* rpc(() => stub.snapshot()));
        if (url.pathname === "/close" && request.method === "POST") {
          // Native abort intentionally rejects its RPC; deletion is performed by the owner.
          yield* rpc(() => stub.close()).pipe(Effect.ignore);

          return Response.json({ closeRequested: true });
        }
        if (url.pathname !== "/submit" || request.method !== "POST")
          return new Response("Not found", { status: 404 });

        const input = yield* Schema.decodeUnknownEffect(Schema.Struct({ phase: PerformancePhase }))(
          yield* rpc(() => request.json()),
        );

        yield* rpc(() => stub.prepare(input.phase));
        const definitions = yield* digestDefinitions(performanceDefinitions(identity));

        const receipt = yield* CloudflareThreadClient.use((client) =>
          client.submit({ definition: performanceDefinition }, performanceMessage(input.phase), {
            threadId: ThreadId.make(name),
            principal: Principal.make("performance-eval"),
            idempotencyKey: IdempotencyKey.make(`phase-${input.phase}`),
            definitions,
          }),
        );

        return Response.json(receipt);
      }).pipe(
        Effect.provide(
          CloudflareThreadClient.layer.pipe(
            Layer.provideMerge(
              Layer.merge(
                BrowserCrypto.layer,
                ThreadObjectNamespace.layer(env.PERFORMANCE_THREADS),
              ),
            ),
          ),
        ),
        Effect.scoped,
        Effect.catch(() =>
          Effect.succeed(new Response("Performance host failed", { status: 500 })),
        ),
      ),
    );
  },
};
