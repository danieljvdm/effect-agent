import { IdGenerator } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import {
  CodeModeHostEntrypoint,
  cloudflareCryptoLayer,
  dynamicWorkerCodeExecutorLayer,
  dynamicWorkerImplementation,
  type CodeModeHostStub,
} from "@effect-agent/platform-cloudflare";
import { Effect, Layer, Option, Schema, Stream } from "effect";

import { codeModeHandlersLayer } from "./agent.ts";
import { liveAgent, scriptedAgent } from "./profiles.ts";
import { isValidTenant, Warehouse, WarehouseObject, warehouseLayer } from "./warehouse-object.ts";
import { AgentAnswer, AskRequest, AskResult } from "./wire.ts";

export { AskRequest, AskResult } from "./wire.ts";

/**
 * The demo Worker: it answers a natural-language question about the invoice
 * warehouse by running a Code Mode Agent. The model writes one JavaScript
 * program; the program executes in an isolated Cloudflare Dynamic Worker
 * (`globalOutbound: null`, no ambient authority) and reaches the warehouse
 * only through the brokered `warehouse.listInvoices` method, whose typed
 * filters select a fixed adapter-owned query. Deployment class E: the
 * Agent runs ephemerally; the Durable Object is the warehouse data store, not
 * a Conversation store.
 *
 * `WarehouseObject` and `CodeModeHostEntrypoint` are exported for the Worker
 * runtime; the host entrypoint is bound to itself as `CODE_MODE_HOST` (see
 * wrangler.jsonc), matching the production `ctx.exports.CodeModeHostEntrypoint()`
 * seam.
 */
export { WarehouseObject, CodeModeHostEntrypoint };

interface WorkerEnv {
  readonly WAREHOUSE: DurableObjectNamespace<WarehouseObject>;
  readonly LOADER: WorkerLoader;
  readonly CODE_MODE_HOST: CodeModeHostStub;
  readonly OPENAI_API_KEY?: string;
  /**
   * Optional shared secret. When set, `/ask` requires a matching
   * `Authorization: Bearer <token>` header — set it whenever `OPENAI_API_KEY`
   * is deployed so the paid live endpoint cannot be driven anonymously.
   */
  readonly DEMO_AUTH_TOKEN?: string;
}

const decodeAskRequest = Schema.decodeUnknownEffect(AskRequest);
const encodeAskResult = Schema.encodeEffect(AskResult);
const decodeProgramInput = Schema.decodeUnknownOption(Schema.Struct({ code: Schema.String }));
const decodeCodeModeSuccess = Schema.decodeUnknownOption(
  Schema.Struct({ result: Schema.Json, logs: Schema.Array(Schema.Json) }),
);

/** A terminal Run event could not be converted into the demo's declared HTTP result. */
export class DemoRunFailure extends Schema.TaggedError<DemoRunFailure>()("DemoRunFailure", {
  reason: Schema.Literals(["incomplete", "invalid-output", "runtime-failed"]),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** Decode untrusted terminal output through the Agent's answer contract. */
export const decodeAgentAnswer = (output: unknown) =>
  Schema.decodeUnknownEffect(AgentAnswer)(output).pipe(
    Effect.mapError((cause) =>
      DemoRunFailure.make({
        reason: "invalid-output",
        message: "the completed agent output did not match the Answer Schema",
        cause,
      }),
    ),
  );

/**
 * Both profiles' bindings share one definition and both models resolve to a
 * bare `LanguageModel` with no residual requirements, so they are the same
 * runtime binding shape; the live binding is cast to the scripted one to
 * avoid threading two Model types through one function (a known type trap).
 */
type DemoBinding = typeof scriptedAgent;

/** Narrow seam used to corrupt only a real RunCompleted payload in boundary tests. */
export type ProjectCompletedOutput = (output: unknown) => unknown;

/**
 * A `run_javascript` tool call's parameters are UNTRUSTED model output
 * (`Schema.Json` — possibly `null`, a primitive, or an array), so extract the
 * program source defensively rather than dereferencing a cast. Returning
 * `undefined` on anything unexpected keeps the observer from turning a
 * malformed declaration into a defect that aborts the request; the engine
 * still handles the actual (in)valid tool call through its typed channel.
 *
 * This reads the raw declared parameters for a display-only evidence field. No
 * engine event currently carries the DECODED tool input (`ToolCallStarted`
 * has none; `ToolCallDeclared` is raw), and the Code Mode input schema applies
 * no transform (raw `code` === executed source), so the raw value is faithful
 * here. A fully general "executed program" would need the engine to surface
 * the decoded input as an event.
 */
const programSourceOf = (parameters: unknown): string | undefined => {
  const decoded = decodeProgramInput(parameters);
  return Option.isSome(decoded) ? decoded.value.code : undefined;
};

/** The `run_javascript` success value is `{ result, logs }` — read it safely. */
const successOutcomeOf = (
  value: unknown,
): { readonly result?: Schema.Json; readonly logs?: ReadonlyArray<Schema.Json> } => {
  const decoded = decodeCodeModeSuccess(value);
  return Option.isSome(decoded) ? decoded.value : {};
};

/** Run one bound agent to an `AskResult`. */
const runBound = (
  env: WorkerEnv,
  agent: DemoBinding,
  question: string,
  tenant: string,
  profile: AskResult["profile"],
  projectCompletedOutput: ProjectCompletedOutput,
): Effect.Effect<AskResult, DemoRunFailure> => {
  // The Code Mode handler needs the warehouse service and the executor
  // provided INTO it (its handler runs with the captured construction
  // context); the Run additionally needs IdGenerator.
  const layers = Layer.mergeAll(
    codeModeHandlersLayer.pipe(
      Layer.provide(warehouseLayer(env.WAREHOUSE, tenant)),
      Layer.provide(
        dynamicWorkerCodeExecutorLayer({ loader: env.LOADER, hostStub: env.CODE_MODE_HOST }),
      ),
    ),
    IdGenerator.layer.pipe(Layer.provide(cloudflareCryptoLayer)),
  );
  return Effect.gen(function* () {
    // Stream the Run and correlate each `run_javascript` call's declared
    // program with the SAME call's success BY tool-call id, so the reported
    // program always matches the execution that produced the reported result
    // (a multi-call model can otherwise interleave a failed declaration with an
    // earlier call's result). `used` reflects a real successful execution, not
    // merely a declaration.
    const declaredPrograms = new Map<string, string | undefined>();
    const successes = new Map<
      string,
      { readonly result?: Schema.Json; readonly logs?: ReadonlyArray<Schema.Json> }
    >();
    let completedOutput: Option.Option<unknown> = Option.none();
    yield* AgentRuntime.stream(agent, { question }).pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          if (event._tag === "ToolCallDeclared" && event.toolName === "run_javascript") {
            declaredPrograms.set(event.toolCallId, programSourceOf(event.parameters));
          }
          if (event._tag === "ToolCallSucceeded" && event.toolName === "run_javascript") {
            successes.set(event.toolCallId, successOutcomeOf(event.result));
          }
          if (event._tag === "RunCompleted") {
            completedOutput = Option.some(projectCompletedOutput(event.output));
          }
        }),
      ),
      Effect.provide(layers),
      Effect.scoped,
      Effect.mapError((cause) =>
        DemoRunFailure.make({
          reason: "runtime-failed",
          message: "the agent runtime failed",
          cause,
        }),
      ),
    );
    // A Run that ended without emitting RunCompleted (e.g. it hit a policy
    // limit) is NOT a success — fail in the typed channel so `runPromise`
    // rejects and the fetch handler returns 500, rather than a 200 with an empty answer.
    if (Option.isNone(completedOutput)) {
      return yield* DemoRunFailure.make({
        reason: "incomplete",
        message: "the agent run did not complete",
      });
    }
    const answer = (yield* decodeAgentAnswer(completedOutput.value)).answer;
    // Claim a single program/result ONLY when exactly one run_javascript call
    // succeeded (with a matching declaration) — never arbitrarily pick among
    // several, and never assert provenance for a run that used zero or multiple
    // calls. `used` and `calls` stay honest either way; ambiguous runs simply
    // do not attach a single program/result to the answer.
    const successIds = [...successes.keys()];
    const soleId =
      successIds.length === 1 && declaredPrograms.has(successIds[0]) ? successIds[0] : undefined;
    const executed = soleId === undefined ? undefined : successes.get(soleId);
    return AskResult.make({
      answer,
      codeMode: {
        used: successes.size > 0,
        tool: "run_javascript",
        executor: dynamicWorkerImplementation.identity,
        calls: declaredPrograms.size,
        ...(soleId === undefined || declaredPrograms.get(soleId) === undefined
          ? {}
          : { program: declaredPrograms.get(soleId) }),
        ...(executed?.result === undefined ? {} : { result: executed.result }),
        ...(executed?.logs === undefined ? {} : { logs: executed.logs }),
      },
      profile,
    });
  });
};

const runAsk = (
  env: WorkerEnv,
  question: string,
  tenant: string,
  projectCompletedOutput: ProjectCompletedOutput,
  agentOverride?: DemoBinding,
): Effect.Effect<AskResult, DemoRunFailure> => {
  if (agentOverride !== undefined) {
    return runBound(env, agentOverride, question, tenant, "scripted", projectCompletedOutput);
  }
  if (env.OPENAI_API_KEY !== undefined && env.OPENAI_API_KEY.length > 0) {
    return runBound(
      env,
      liveAgent(env.OPENAI_API_KEY) as unknown as DemoBinding,
      question,
      tenant,
      "openai",
      projectCompletedOutput,
    );
  }
  return runBound(env, scriptedAgent, question, tenant, "scripted", projectCompletedOutput);
};

/** Construct the Worker; tests may alter a real terminal projection or provide a test binding. */
export const makeDemoWorker = (
  projectCompletedOutput: ProjectCompletedOutput = (output) => output,
  agentOverride?: DemoBinding,
) => ({
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== "/ask") {
      return new Response(
        "POST /ask { question } — Code Mode over a SQLite Durable Object warehouse",
        { status: url.pathname === "/" ? 200 : 404 },
      );
    }
    if (request.method !== "POST") {
      return Response.json({ error: "POST required" }, { status: 405 });
    }
    const liveMode = env.OPENAI_API_KEY !== undefined && env.OPENAI_API_KEY.length > 0;
    const authToken = env.DEMO_AUTH_TOKEN;
    const hasAuthToken = authToken !== undefined && authToken.length > 0;
    // Fail CLOSED on the paid path: if the live model is enabled but no shared
    // secret is configured, refuse rather than serve paid inference to
    // anonymous callers. The offline scripted default needs no secret.
    if (liveMode && !hasAuthToken) {
      return Response.json(
        { error: "server misconfigured: DEMO_AUTH_TOKEN must be set when OPENAI_API_KEY is" },
        { status: 503 },
      );
    }
    // When a shared secret is configured, require a matching bearer token.
    if (hasAuthToken && request.headers.get("authorization") !== `Bearer ${authToken}`) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }
    let body: unknown;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "request body must be valid JSON" }, { status: 400 });
    }
    let decodedAsk: AskRequest;
    try {
      decodedAsk = await Effect.runPromise(decodeAskRequest(body));
    } catch {
      return Response.json(
        { error: "body must be { question: non-empty string <= 2000 chars }" },
        { status: 400 },
      );
    }
    // The tenant addresses a Durable Object, so reject anything that is not a
    // short, safe identifier rather than minting an arbitrary Object.
    const tenant = url.searchParams.get("tenant") ?? "acme";
    if (!isValidTenant(tenant)) {
      return Response.json({ error: "tenant must match /^[a-z0-9-]{1,32}$/" }, { status: 400 });
    }
    try {
      const result = await Effect.runPromise(
        runAsk(env, decodedAsk.question, tenant, projectCompletedOutput, agentOverride),
      );
      return Response.json(await Effect.runPromise(encodeAskResult(result)));
    } catch (cause) {
      return Response.json(
        { error: cause instanceof Error ? cause.message : String(cause) },
        { status: 500 },
      );
    }
  },
});

export default makeDemoWorker();

// Re-exported so a consumer can compose the warehouse service directly.
export { Warehouse };
