import { ModelCallContext } from "@effect-agent/engine/ContextWindow";
import { RunContextPreparation, RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import {
  ScriptedModel,
  type ScriptedTurnInput,
  type ScriptedStreamPart,
} from "@effect-agent/testing/ScriptedModel";
import { Clock, Context, Effect, Layer, Schema } from "effect";
import { Agent, AgentRuntime } from "effect-agent";
import { IdGenerator } from "effect-agent/IdGenerator";
import { ThreadHistory } from "effect-agent/ThreadHistory";
import { AiError, type LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";

import { BenchmarkError, check } from "./contracts.js";
import { policyCases } from "./diagnostic-cases.js";
import {
  DiagnosticProgress,
  type DiagnosticCase,
  type DiagnosticResult,
} from "./diagnostic-contracts.js";

export { policyCases } from "./diagnostic-cases.js";

const parameters = Schema.Struct({ index: Schema.Natural });

const work = Tool.make("diagnostic_work", {
  parameters,
  success: Schema.Natural,
  needsApproval: true,
});

const toolkit = Toolkit.make(work);

const definition = Agent.make("diagnostic-policy", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Perform the declared work and answer.",
  toolkit,
  policy: { maxTurns: 6, maxToolCalls: 32, maxDuration: "20 seconds", toolConcurrency: 4 },
});

const context = ModelCallContext.make({
  contextCapacity: 100_000,
  outputReserveTokens: 1_000,
  uncountedOverheadTokens: 0,
});

const delay = (milliseconds: number) =>
  milliseconds === 0 ? Effect.void : Effect.sleep(milliseconds);

/** Controlled public hook delays diagnose scheduling; they are not provider latency estimates. */
export const runPolicyCase = Effect.fn("diagnostic.policy")(function* (workload: DiagnosticCase) {
  const selected = policyCases.find((candidate) => candidate.name === workload.name);

  if (selected === undefined || workload.family !== "policy")
    return yield* BenchmarkError.make({ message: "Unknown policy diagnostic case" });
  yield* check(
    Object.keys(workload.parameters).length === Object.keys(selected.parameters).length &&
      Object.entries(selected.parameters).every(
        ([key, value]) => workload.parameters[key] === value,
      ),
    "Policy diagnostic parameters differ from the named fixture",
  );
  const rounds = selected.parameters.rounds ?? 0;
  const authorizationMs = selected.parameters.authorizationMs ?? 0;
  const modelAcquisitionMs = selected.parameters.modelAcquisitionMs ?? 0;
  const progress = yield* DiagnosticProgress;

  yield* progress.phase("setup");
  let started = 0n;
  let providers = 0;
  let streamsClosed = 0;
  let approvals = 0;
  let authorizations = 0;
  let authorizationFinalizers = 0;
  let activeAuthorizations = 0;
  let handlers = 0;
  let handlerFinalizers = 0;
  let activeHandlers = 0;
  let maxHandlers = 0;
  let modelsOpened = 0;
  let modelsReady = 0;
  let modelsClosed = 0;
  let activeModels = 0;
  let preparationMs = 0;
  let acquisitionMs = 0;
  let authorizationSumMs = 0;
  let waitMaxMs = 0;
  let firstProviderMs = 0;
  const authorizedAt = new Map<number, number>();
  const authorizationPhases = Array.from({ length: rounds }, () => ({ start: -1, end: 0 }));
  const handlerPhases = Array.from({ length: rounds }, () => ({ start: -1, end: 0 }));

  const stamp = Effect.fn("diagnostic.policyMark")(function* (name: string) {
    const evidence = yield* DiagnosticProgress;
    const elapsedMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;

    yield* evidence.mark({ name, elapsedMs });

    return elapsedMs;
  });

  const modelAndHandlers = Layer.unwrap(
    Effect.gen(function* () {
      const evidence = yield* DiagnosticProgress;

      const parts = (turn: number): ReadonlyArray<ScriptedStreamPart> =>
        turn < rounds
          ? [
              ...Array.from({ length: 8 }, (_, index) => ({
                type: "tool-call" as const,
                id: `call-${turn * 8 + index}`,
                name: "diagnostic_work",
                params: { index: turn * 8 + index },
                providerExecuted: false,
              })),
              {
                type: "finish",
                reason: "tool-calls",
                usage: { inputTokens: {}, outputTokens: {} },
              },
            ]
          : [
              { type: "text-start", id: "answer" },
              { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
              { type: "text-end", id: "answer" },
              { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
            ];

      const turns: ReadonlyArray<ScriptedTurnInput> = Array.from(
        { length: rounds + 1 },
        (_, turn) => ({
          _tag: "Stream",
          parts: parts(turn),
          termination: { _tag: "Complete" },
          assertRequest: (request) =>
            Effect.gen(function* () {
              const at = yield* stamp(`provider:${turn + 1}`);

              if (providers === 0) firstProviderMs = at;
              providers++;
              yield* check(
                modelsReady === turn + 1 && activeModels === 1,
                "Provider entered before its model was ready",
              );

              const results = request.prompt.content
                .filter((message) => message.role === "tool")
                .flatMap((message) => message.content)
                .filter((part) => part.type === "tool-result");

              yield* check(
                results.length === turn * 8 &&
                  results.every(
                    (part, index) =>
                      !part.isFailure && part.id === `call-${index}` && part.result === index,
                  ),
                "Provider lost, reordered, or duplicated successful tool results",
              );
            }).pipe(
              Effect.provideService(DiagnosticProgress, evidence),
              Effect.mapError((cause) =>
                AiError.AiError.make({
                  module: "runtime-diagnostics",
                  method: "policy.assertRequest",
                  reason: AiError.UnknownError.make({ description: cause.message }),
                }),
              ),
            ),
          onStreamFinalize: Effect.sync(() => {
            streamsClosed++;
          }),
        }),
      );

      const handlersLayer = toolkit.toLayer({
        diagnostic_work: ({ index }) =>
          Effect.acquireUseRelease(
            Effect.gen(function* () {
              const round = Math.floor(index / 8);

              yield* check(
                authorizationFinalizers === (round + 1) * 8 &&
                  approvals === (round + 1) * 8 &&
                  activeAuthorizations === 0,
                "A handler started before the complete batch was approved and authorized",
              );
              const at = yield* stamp(`handler:start:${index}`);
              const authorized = authorizedAt.get(index);

              yield* check(authorized !== undefined, "Handler lacked successful authorization");
              waitMaxMs = Math.max(waitMaxMs, at - (authorized ?? at));
              const phase = handlerPhases[round];

              if (phase !== undefined && phase.start < 0) phase.start = at;
              handlers++;
              activeHandlers++;
              maxHandlers = Math.max(maxHandlers, activeHandlers);
            }),
            () => Effect.sleep(20).pipe(Effect.as(index)),
            () =>
              Effect.gen(function* () {
                activeHandlers--;
                handlerFinalizers++;
                const phase = handlerPhases[Math.floor(index / 8)];
                const at = yield* stamp(`handler:end:${index}`);

                if (phase !== undefined) phase.end = at;
              }).pipe(Effect.orDie),
          ).pipe(Effect.provideService(DiagnosticProgress, evidence), Effect.orDie),
      });

      return Layer.mergeAll(
        ScriptedModel.layer(turns),
        handlersLayer,
        Layer.succeed(Model.ProviderName, "scripted"),
        Layer.succeed(Model.ModelName, "diagnostic-policy"),
      );
    }),
  );

  const hooks = Layer.effectContext(
    Effect.gen(function* () {
      const evidence = yield* DiagnosticProgress;

      const captured = Layer.succeedContext(
        yield* Effect.context<LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName>(),
      );

      const preparation = RunContextPreparation.of({
        hook: {
          prepare: (request) =>
            Effect.gen(function* () {
              const before = yield* stamp(`prepare:start:${request.turn}`);

              yield* check(
                activeModels === 0 && modelsClosed === request.turn - 1,
                "Previous model layer did not close before preparation",
              );

              const acquisition = Layer.effectDiscard(
                Effect.gen(function* () {
                  yield* Effect.acquireRelease(
                    Effect.sync(() => {
                      modelsOpened++;
                      activeModels++;
                    }),
                    () =>
                      Effect.gen(function* () {
                        activeModels--;
                        modelsClosed++;
                        yield* stamp(`model:close:${request.turn}`);
                      }).pipe(Effect.orDie),
                  );
                  const opened = yield* stamp(`model:open:${request.turn}`);

                  yield* delay(modelAcquisitionMs);
                  modelsReady++;
                  acquisitionMs += (yield* stamp(`model:ready:${request.turn}`)) - opened;
                }).pipe(Effect.provideService(DiagnosticProgress, evidence), Effect.orDie),
              );

              preparationMs += (yield* stamp(`prepare:end:${request.turn}`)) - before;

              return {
                prompt: request.source,
                modelCall: { model: Layer.merge(captured, acquisition), context },
              };
            }).pipe(Effect.provideService(DiagnosticProgress, evidence), Effect.orDie),
        },
      });

      const authorization = RunToolAuthorization.of({
        authorize: (request) =>
          Effect.acquireUseRelease(
            Effect.gen(function* () {
              const decoded = yield* Schema.decodeUnknownEffect(parameters)(
                request.call.parameters,
              );

              yield* check(
                decoded.index === authorizations &&
                  approvals === (Math.floor(decoded.index / 8) + 1) * 8 &&
                  activeAuthorizations === 0 &&
                  authorizations === authorizationFinalizers,
                "Authorization Effects did not run and finalize in declaration order",
              );
              authorizations++;
              activeAuthorizations++;
              const at = yield* stamp(`authorize:start:${decoded.index}`);
              const phase = authorizationPhases[Math.floor(decoded.index / 8)];

              if (phase !== undefined && phase.start < 0) phase.start = at;

              return { index: decoded.index, at };
            }),
            () => delay(authorizationMs).pipe(Effect.as({ _tag: "allowed" as const })),
            ({ index, at }) =>
              Effect.gen(function* () {
                activeAuthorizations--;
                authorizationFinalizers++;
                const ended = yield* stamp(`authorize:end:${index}`);

                authorizationSumMs += ended - at;
                authorizedAt.set(index, ended);
                const phase = authorizationPhases[Math.floor(index / 8)];

                if (phase !== undefined) phase.end = ended;
              }).pipe(Effect.orDie),
          ).pipe(Effect.provideService(DiagnosticProgress, evidence), Effect.orDie),
      });

      return Context.make(RunContextPreparation, preparation).pipe(
        Context.add(RunToolAuthorization, authorization),
      );
    }),
  );

  const app = hooks.pipe(
    Layer.provideMerge(modelAndHandlers),
    Layer.provideMerge(Layer.merge(IdGenerator.layer, ThreadHistory.layerTransient)),
  );

  return yield* Effect.gen(function* () {
    const scripted = yield* ScriptedModel;

    yield* progress.phase("operation");
    started = yield* Clock.monotonicTimeNanos;

    const result = yield* AgentRuntime.run(definition, "Do the work", {
      approval: {
        request: () =>
          Effect.sync(() => {
            approvals++;

            return { _tag: "approved" as const };
          }),
      },
    });

    const totalMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;

    yield* progress.phase("verification");
    yield* scripted.assertExhausted;
    yield* check(
      result.output.answer === "done" && result.turns === rounds + 1,
      "Policy run did not return the expected answer/turn count",
    );
    yield* check(
      providers === rounds + 1 &&
        streamsClosed === providers &&
        modelsOpened === providers &&
        modelsReady === providers &&
        modelsClosed === providers &&
        activeModels === 0,
      "Policy model call or finalizer counts changed",
    );
    yield* check(
      approvals === rounds * 8 &&
        authorizations === approvals &&
        authorizationFinalizers === approvals &&
        handlers === approvals &&
        handlerFinalizers === approvals &&
        activeHandlers === 0 &&
        activeAuthorizations === 0 &&
        maxHandlers > 1 &&
        maxHandlers <= 4,
      "Policy authorization, handler, concurrency, or finalizer counts changed",
    );

    return {
      totalMs,
      metrics: [
        { name: "firstProvider", value: firstProviderMs },
        { name: "preparationSum", value: preparationMs },
        { name: "modelAcquisitionSum", value: acquisitionMs },
        { name: "authorizationSum", value: authorizationSumMs },
        {
          name: "authorizationPhasesSum",
          value: authorizationPhases.reduce((sum, phase) => sum + phase.end - phase.start, 0),
        },
        {
          name: "handlerPhasesSum",
          value: handlerPhases.reduce((sum, phase) => sum + phase.end - phase.start, 0),
        },
        { name: "postAuthorizationWaitMax", value: waitMaxMs },
      ],
      counters: [
        { name: "providers", value: providers },
        { name: "streamFinalizers", value: streamsClosed },
        { name: "approvals", value: approvals },
        { name: "authorizations", value: authorizations },
        { name: "authorizationFinalizers", value: authorizationFinalizers },
        { name: "handlers", value: handlers },
        { name: "handlerFinalizers", value: handlerFinalizers },
        { name: "maxConcurrentHandlers", value: maxHandlers },
        { name: "modelsOpened", value: modelsOpened },
        { name: "modelsReady", value: modelsReady },
        { name: "modelsClosed", value: modelsClosed },
      ],
    } satisfies DiagnosticResult;
  }).pipe(
    Effect.provide(app),
    Effect.scoped,
    Effect.mapError((cause) => BenchmarkError.make({ message: "Policy diagnostic failed", cause })),
  );
});
