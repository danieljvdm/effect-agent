import * as ContextTools from "@effect-agent/capabilities/ContextTools";
import * as MemoryNotes from "@effect-agent/capabilities/MemoryNotes";
import { ThreadId } from "@effect-agent/core/Identifiers";
import {
  MemoryKey,
  MemoryReader,
  MemoryStorageError,
  MemoryWriter,
} from "@effect-agent/core/MemoryStore";
import { ContextHistory, ContextHistoryError } from "@effect-agent/engine/ContextHistory";
import {
  DurableObjectContext,
  ThreadObjectIdentity,
  ThreadObjectNamespace,
} from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/CloudflareThreadClient";
import * as ThreadObject from "@effect-agent/platform-cloudflare/ThreadObject";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { memoryStoreLayer } from "@effect-agent/thread/SqlMemoryStore";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import * as ThreadContextHistory from "@effect-agent/thread/ThreadContextHistory";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { BrowserCrypto } from "@effect/platform-browser";
import { Context, Effect, Layer, Redacted, Ref, Schema } from "effect";
import { DurableObject, WorkerEnvironment } from "effect-cf";
import { IdGenerator } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import {
  cloudflareDefinition,
  cloudflareDefinitions,
  CloudflareIdentity,
  cloudflareModelSettings,
  CloudflareSnapshot,
} from "./cloudflare-contracts.ts";
import { CompactionEvidence, EvaluationError, ModelUsage, RestartEvidence } from "./contracts.ts";
import { notesNamespace, readLog, readNotes, readRecoveryCheckpoint } from "./host-evidence.ts";
import { makeLiveClient } from "./live-model.ts";
import { manifestLayer, observedCompactor } from "./pressure.ts";
import { RequestAudit, RequestAuditSink } from "./request-audit.ts";
import { RESTARTS } from "./scenario.ts";

// Embedded by the build from this checkout, not a mutable deployment variable.
declare const CONTEXT_EVAL_SOURCE_COMMIT: string;
declare const CONTEXT_EVAL_DIRTY: boolean;
declare global {
  namespace Cloudflare {
    interface Env {
      THREADS: DurableObjectNamespace<ContinuityThread>;
      CONTEXT_EVAL_TOKEN: string;
      OPENAI_API_KEY: string;
      CONTEXT_EVAL_MODEL: string;
    }
  }
}

const identityFor = (env: Cloudflare.Env) =>
  Schema.decodeUnknownEffect(CloudflareIdentity)({
    sourceCommit: CONTEXT_EVAL_SOURCE_COMMIT,
    dirtyWorkingTree: CONTEXT_EVAL_DIRTY,
    profile: "pressure-cloudflare-v1",
    model: env.CONTEXT_EVAL_MODEL,
    seed: 17,
    contextTokenLimit: 16_000,
    maxCostMicrousd: 10_000_000,
  });

const emptyUsage: ModelUsage = {
  calls: 0,
  completedCalls: 0,
  inputTokens: 0,
  outputTokens: 0,
  maxInputTokens: 0,
  estimatedCostMicrousd: 0,
  reservedCostMicrousd: 0,
  returnedModels: [],
};

const State = Schema.Struct({
  identity: CloudflareIdentity,
  phase: Schema.Natural,
  incarnation: Schema.Natural,
  usage: ModelUsage,
  compactions: Schema.Array(CompactionEvidence),
  restarts: Schema.Array(RestartEvidence),
  pendingRestart: Schema.NullOr(
    Schema.Struct({
      phase: Schema.Natural,
      runId: Schema.String,
      location: RestartEvidence.fields.location,
      revision: Schema.NullOr(Schema.String),
      text: Schema.String,
      incarnation: Schema.Natural,
    }),
  ),
});

class EvalState extends Context.Service<
  EvalState,
  {
    readonly prepare: (phase: number) => Effect.Effect<void, EvaluationError>;
    readonly snapshot: Effect.Effect<typeof CloudflareSnapshot.Type, EvaluationError>;
    readonly hit: (location: string) => Effect.Effect<void>;
  }
>()("example/ContextContinuityCloudflare") {}

// Only this example's durable key/value evidence uses native SQLite. Framework
// notes/history still go through public adapters and the host's one SQL client.
const application = Layer.unwrap(
  Effect.gen(function* () {
    const { ctx } = yield* DurableObjectContext;
    const { threadId } = yield* ThreadObjectIdentity;
    const env = yield* WorkerEnvironment;
    const identity = yield* identityFor(env);

    const key = MemoryKey.make({
      namespace: notesNamespace.make({ threadId }),
      id: "working-notes",
    });

    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS context_eval_artifacts (path TEXT PRIMARY KEY, value TEXT NOT NULL)",
    );

    const put = (path: string, value: string) =>
      ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO context_eval_artifacts(path,value) VALUES (?,?)",
        path,
        value,
      );

    const stored = ctx.storage.sql
      .exec<{ value: string }>("SELECT value FROM context_eval_artifacts WHERE path='state'")
      .toArray()[0]?.value;

    let state: typeof State.Type =
      stored === undefined
        ? {
            identity,
            phase: 0,
            incarnation: 0,
            usage: emptyUsage,
            compactions: [],
            restarts: [],
            pendingRestart: null,
          }
        : yield* Schema.decodeEffect(Schema.fromJsonString(State))(stored);

    if (JSON.stringify(state.identity) !== JSON.stringify(identity))
      return yield* EvaluationError.make({
        stage: "source",
        message: "Persisted Cloudflare candidate identity changed",
      });
    state = { ...state, incarnation: state.incarnation + 1 };
    const phase = yield* Ref.make(state.phase);
    const persist = () => put("state", Schema.encodeSync(Schema.fromJsonString(State))(state));

    persist();
    // A reservation surviving eviction is ambiguous; never dispatch it again.

    // Registration captures these forwarding ports. Construction installs their
    // real implementations from ThreadObject's services before any event can run.
    let reader = MemoryReader.fromAdapter({
      get: () =>
        Effect.fail(
          MemoryStorageError.make({ operation: "uninitialized eval notes", reason: "unavailable" }),
        ),
    });

    let writer = MemoryWriter.fromAdapter({
      change: () =>
        Effect.fail(
          MemoryStorageError.make({ operation: "uninitialized eval notes", reason: "unavailable" }),
        ),
    });

    let history = ContextHistory.of({
      search: () =>
        Effect.fail(
          ContextHistoryError.make({
            reason: "unavailable",
            message: "Eval history not initialized",
          }),
        ),
      read: () =>
        Effect.fail(
          ContextHistoryError.make({
            reason: "unavailable",
            message: "Eval history not initialized",
          }),
        ),
    });

    const ports = Layer.mergeAll(
      Layer.succeed(MemoryReader, MemoryReader.fromAdapter({ get: (k) => reader.get(k) })),
      Layer.succeed(MemoryWriter, MemoryWriter.fromAdapter({ change: (w) => writer.change(w) })),
      Layer.succeed(
        ContextHistory,
        ContextHistory.of({ search: (r) => history.search(r), read: (r) => history.read(r) }),
      ),
    );

    const currentNotes = readNotes(key).pipe(Effect.provide(ports));
    let snapshotUsage: Effect.Effect<ModelUsage> = Effect.succeed(state.usage);

    const auditSink = RequestAuditSink.of({
      write: (event) =>
        Effect.gen(function* () {
          put(
            `audit-${event.request.toString().padStart(4, "0")}-${event.kind}`,
            yield* Schema.encodeEffect(Schema.fromJsonString(RequestAudit))(event),
          );
          state = { ...state, usage: yield* snapshotUsage };
          persist();
        }).pipe(
          Effect.mapError(() =>
            EvaluationError.make({
              stage: "evidence",
              message: "Cannot persist Cloudflare request accounting",
            }),
          ),
        ),
    });

    const live = yield* makeLiveClient({
      model: identity.model,
      maxCostMicrousd: identity.maxCostMicrousd,
      initialUsage: state.usage,
      phase,
    }).pipe(
      Effect.provide(
        Layer.merge(
          Layer.succeed(RequestAuditSink, auditSink),
          OpenAiClient.layer({ apiKey: Redacted.make(env.OPENAI_API_KEY) }).pipe(
            Layer.provide(FetchHttpClient.layer),
          ),
        ),
      ),
    );

    snapshotUsage = live.snapshot;

    const compactor = observedCompactor((evidence) => {
      state = { ...state, compactions: [...state.compactions, evidence] };
      persist();
    });

    const handlers = MemoryNotes.layer({
      key,
      locator: `memory://context-eval/${threadId}`,
      scopes: [],
      attributions: [
        {
          originId: "context-eval-agent",
          speaker: "Agent",
          observers: [],
          locator: `memory://context-eval/${threadId}`,
          activityAt: null,
          interpretation: "private working notes",
        },
      ],
    }).pipe(
      Layer.provide(ports),
      Layer.provide(Layer.succeed(IdGenerator.IdGenerator, IdGenerator.defaultIdGenerator)),
    );

    const host = ThreadObject.layer([
      {
        agent: cloudflareDefinition,
        model: OpenAiLanguageModel.model(identity.model, cloudflareModelSettings),
        definitions: cloudflareDefinitions(identity),
      },
    ]).pipe(
      Layer.provide(
        Layer.mergeAll(
          handlers,
          ContextTools.layer,
          manifestLayer(16_000),
          ports,
          compactor,
          Layer.succeed(OpenAiClient.OpenAiClient, live.client),
        ),
      ),
    );

    return Layer.effectContext(
      Effect.gen(function* () {
        const services = yield* Effect.context<ThreadObject.Services>();

        const adapters = yield* Layer.build(
          Layer.mergeAll(memoryStoreLayer, ThreadContextHistory.layer({ maxRecords: 16_384 })).pipe(
            Layer.provide(Layer.succeedContext(services)),
          ),
        );

        reader = Context.get(adapters, MemoryReader);
        writer = Context.get(adapters, MemoryWriter);
        history = Context.get(adapters, ContextHistory);
        if (state.pendingRestart !== null) {
          const before = state.pendingRestart;
          const notes = yield* currentNotes;

          state = {
            ...state,
            pendingRestart: null,
            restarts: [
              ...state.restarts,
              {
                phase: before.phase,
                location: before.location,
                runId: before.runId,
                notesRevisionBefore: before.revision,
                notesRevisionAfter: notes.revision,
                notesTextUnchanged: before.text === notes.text,
                mechanism: "durable-object-eviction",
                processBefore: null,
                processAfter: null,
                killConfirmed: before.incarnation < state.incarnation,
              },
            ],
          };
          persist();
        }

        const evalState = EvalState.of({
          prepare: (index) =>
            Effect.gen(function* () {
              if (index > 12 || index < state.phase || index > state.phase + 1)
                return yield* EvaluationError.make({
                  stage: "phase",
                  message: "Out-of-order Cloudflare phase",
                });
              state = { ...state, phase: index };
              yield* Ref.set(phase, index);
              persist();
            }),
          snapshot: Effect.gen(function* () {
            const records = yield* readLog(threadId);
            const notes = yield* currentNotes;

            const audits = yield* Effect.forEach(
              ctx.storage.sql
                .exec<{ value: string }>(
                  "SELECT value FROM context_eval_artifacts WHERE path LIKE 'audit-%' ORDER BY path",
                )
                .toArray(),
              (row) => Schema.decodeEffect(Schema.fromJsonString(RequestAudit))(row.value),
            );

            return {
              identity,
              records,
              notes,
              audits,
              usage: yield* live.snapshot,
              failure: yield* live.failure,
              compactions: state.compactions,
              restarts: state.restarts,
              recoveryCheckpoint: yield* readRecoveryCheckpoint(threadId),
            };
          }).pipe(
            Effect.provide(services),
            Effect.mapError(() =>
              EvaluationError.make({
                stage: "evidence",
                message: "Cloudflare evidence unavailable",
              }),
            ),
          ),
          hit: (location) =>
            Effect.gen(function* () {
              const boundary = RESTARTS.find(
                (r) => r.phase === state.phase && r.location === location,
              );

              if (boundary === undefined || state.restarts.some((r) => r.phase === boundary.phase))
                return;
              const notes = yield* currentNotes;
              const records = yield* readLog(threadId);

              const run = records.findLast(({ record }) => record.payload._tag === "RunStarted")
                ?.record.payload;

              if (run?._tag !== "RunStarted")
                return yield* Effect.die("No run at eviction barrier");
              state = {
                ...state,
                usage: yield* live.snapshot,
                pendingRestart: {
                  ...boundary,
                  runId: run.runId,
                  revision: notes.revision,
                  text: notes.text,
                  incarnation: state.incarnation,
                },
              };
              persist();
              // Confirm the evidence before aborting; abort discards unconfirmed SQLite writes.
              yield* Effect.tryPromise({
                try: () => ctx.storage.sync(),
                catch: () =>
                  EvaluationError.make({
                    stage: "evidence",
                    message: "Eviction checkpoint did not persist",
                  }),
              });
              ctx.abort("context continuity: planned native eviction");
            }).pipe(Effect.provide(services), Effect.orDie),
        });

        hitByContext.set(ctx, evalState.hit);

        return Context.add(Context.merge(services, adapters), EvalState, evalState);
      }),
    ).pipe(Layer.provide(host));
  }),
);

const hitByContext = new WeakMap<DurableObjectState, (location: string) => Effect.Effect<void>>();

export class ContinuityThread extends ThreadObject.make(application, {
  namespaceBinding: "THREADS",
  deploymentId: "context-continuity-cloudflare-v1",
  producerPrefix: "context-eval",
  ownershipLeaseDuration: 2_000,
  leaseRenewalInterval: 500,
  alarmBackoffBase: 100,
  alarmBackoffCap: 1_000,
  runtimeFailpoint: (ctx) => (location) =>
    Effect.suspend(
      () => hitByContext.get(ctx)?.(location) ?? Effect.die("Eval failpoint not initialized"),
    ),
}) {
  prepare(phase: number) {
    return this[DurableObject.RunSymbol](Effect.flatMap(EvalState, (s) => s.prepare(phase)));
  }
  snapshot() {
    return this[DurableObject.RunSymbol](
      Effect.flatMap(EvalState, (s) => s.snapshot).pipe(
        Effect.flatMap(Schema.encodeEffect(CloudflareSnapshot)),
      ),
    );
  }
}

export default {
  fetch(request: Request, env: Cloudflare.Env): Promise<Response> {
    return Effect.runPromise(
      Effect.gen(function* () {
        if (
          !env.CONTEXT_EVAL_TOKEN ||
          request.headers.get("authorization") !== `Bearer ${env.CONTEXT_EVAL_TOKEN}`
        )
          return new Response("Unauthorized", { status: 401 });
        const identity = yield* identityFor(env);
        const url = new URL(request.url);

        if (url.pathname === "/identity") return Response.json(identity);

        const name = yield* Schema.decodeUnknownEffect(
          Schema.String.check(Schema.isPattern(/^context-eval-[a-zA-Z0-9-]{1,100}$/)),
        )(url.searchParams.get("thread"));

        const stub = env.THREADS.getByName(name);

        if (url.pathname === "/snapshot")
          return Response.json(
            yield* Effect.tryPromise({
              try: () => stub.snapshot(),
              catch: () => EvaluationError.make({ stage: "host", message: "Snapshot RPC failed" }),
            }),
          );
        if (url.pathname !== "/submit" || request.method !== "POST")
          return new Response("Not found", { status: 404 });

        const input = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ phase: Schema.Natural, message: Schema.String }),
        )(
          yield* Effect.tryPromise({
            try: () => request.json(),
            catch: () => EvaluationError.make({ stage: "request", message: "Invalid request" }),
          }),
        );

        yield* Effect.tryPromise({
          try: () => stub.prepare(input.phase),
          catch: () => EvaluationError.make({ stage: "host", message: "Prepare RPC failed" }),
        });
        const definitions = yield* digestDefinitions(cloudflareDefinitions(identity));

        const receipt = yield* CloudflareThreadClient.use((client) =>
          client.submit({ definition: cloudflareDefinition }, input.message, {
            threadId: ThreadId.make(name),
            principal: Principal.make("context-eval"),
            idempotencyKey: IdempotencyKey.make(`phase-${input.phase}`),
            definitions,
          }),
        );

        return Response.json(receipt);
      }).pipe(
        Effect.provide(
          CloudflareThreadClient.layer.pipe(
            Layer.provideMerge(
              Layer.merge(BrowserCrypto.layer, ThreadObjectNamespace.layer(env.THREADS)),
            ),
          ),
        ),
        Effect.scoped,
        Effect.tapCause(Effect.logError),
        Effect.catch(() => Effect.succeed(new Response("Evaluation host failed", { status: 500 }))),
      ),
    );
  },
};
