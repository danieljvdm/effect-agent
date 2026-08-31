import { IdGenerator } from "@effect-agent/core";
import { AgentRuntime, ThreadHistory } from "@effect-agent/engine";
import {
  dynamicWorkerCodeExecutorLayer,
  dynamicWorkerImplementation,
} from "@effect-agent/platform-cloudflare";
import { Effect, Layer, Stream } from "effect";

import { codeModeHandlersLayer } from "./agent.ts";
import { liveAgent, scriptedAgent, scriptedWriteProbeAgent } from "./profiles.ts";
import { isValidTenant, Warehouse, WarehouseObject, warehouseLayer } from "./warehouse-object.ts";

/**
 * The demo Worker: it answers a natural-language question about the invoice
 * warehouse by running a Code Mode Agent. The model writes one JavaScript
 * program; the program executes in an isolated Cloudflare Dynamic Worker
 * (`globalOutbound: null`, no ambient authority) and reaches the warehouse
 * only through the brokered `warehouse.query` method, which runs a read-only
 * SQL query against a SQLite-backed Durable Object. Deployment class E: the
 * Agent runs ephemerally; the Durable Object is the warehouse data store, not
 * a Thread store.
 *
 * `WarehouseObject` is exported for the Worker runtime. The Code Mode
 * executor creates each host RPC capability inside the request that owns it,
 * so the application needs no callback entrypoint or self service binding.
 */
export { WarehouseObject };

interface WorkerEnv {
  readonly WAREHOUSE: DurableObjectNamespace<WarehouseObject>;
  readonly LOADER: WorkerLoader;
  readonly OPENAI_API_KEY?: string;
  /**
   * Optional shared secret. When set, `/ask` requires a matching
   * `Authorization: Bearer <token>` header — set it whenever `OPENAI_API_KEY`
   * is deployed so the paid live endpoint cannot be driven anonymously.
   */
  readonly DEMO_AUTH_TOKEN?: string;
}

interface AskResult {
  readonly answer: string;
  /**
   * Evidence that the answer came from Code Mode: the tool the model called,
   * how many times, the JavaScript program it actually wrote, and the isolated
   * executor that ran it — plus the program's returned value and console logs.
   */
  readonly codeMode: {
    readonly used: boolean;
    readonly tool: string;
    readonly executor: string;
    readonly calls: number;
    readonly program?: string;
    readonly result?: unknown;
    readonly logs?: ReadonlyArray<unknown>;
  };
  readonly profile: "scripted" | "openai";
}

/**
 * Both profiles' bindings share one definition and both models resolve to a
 * bare `LanguageModel` with no residual requirements, so they are the same
 * runtime binding shape; the live binding is cast to the scripted one to
 * avoid threading two Model types through one function (a known type trap).
 */
type DemoBinding = typeof scriptedAgent;

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
  if (typeof parameters === "object" && parameters !== null && "code" in parameters) {
    const code = (parameters as Record<string, unknown>).code;
    return typeof code === "string" ? code : undefined;
  }
  return undefined;
};

/** The `run_javascript` success value is `{ result, logs }` — read it safely. */
const successOutcomeOf = (
  value: unknown,
): { readonly result: unknown; readonly logs: ReadonlyArray<unknown> } => {
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    return { result: record.result, logs: Array.isArray(record.logs) ? record.logs : [] };
  }
  return { result: undefined, logs: [] };
};

/** Run one bound agent to an `AskResult`. */
const runBound = (
  env: WorkerEnv,
  agent: DemoBinding,
  question: string,
  tenant: string,
  profile: AskResult["profile"],
): Effect.Effect<AskResult> => {
  // The Code Mode handler needs the warehouse service and the executor
  // provided INTO it (its handler runs with the captured construction
  // context); the Run additionally needs IdGenerator.
  const layers = Layer.mergeAll(
    codeModeHandlersLayer.pipe(
      Layer.provide(warehouseLayer(env.WAREHOUSE, tenant)),
      Layer.provide(dynamicWorkerCodeExecutorLayer({ loader: env.LOADER })),
    ),
    IdGenerator.layer,
    ThreadHistory.layerTransient,
  );
  return Effect.gen(function* () {
    // Stream the Run and correlate each `run_javascript` call's declared
    // program with the SAME call's success BY tool-call id, so the reported
    // program always matches the execution that produced the reported result
    // (a multi-call model can otherwise interleave a failed declaration with an
    // earlier call's result). `used` reflects a real successful execution, not
    // merely a declaration.
    const declaredPrograms = new Map<string, string | undefined>();
    const successes = new Map<string, { result: unknown; logs: ReadonlyArray<unknown> }>();
    let answer = "";
    let completed = false;
    // Expected Run failures become defects at this HTTP boundary: `runPromise`
    // rejects and the fetch handler answers 500 instead of fabricating a 200.
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
            completed = true;
            const output = event.output as { readonly answer?: unknown };
            if (typeof output.answer === "string") {
              answer = output.answer;
            }
          }
        }),
      ),
      Effect.provide(layers),
      Effect.scoped,
      Effect.orDie,
    );
    // A Run that ended without emitting RunCompleted (e.g. it hit a policy
    // limit) is NOT a success — surface it as a defect so `runPromise` rejects
    // and the fetch handler returns 500, rather than a 200 with an empty answer.
    if (!completed) {
      return yield* Effect.die(new Error("the agent run did not complete"));
    }
    // Claim a single program/result ONLY when exactly one run_javascript call
    // succeeded (with a matching declaration) — never arbitrarily pick among
    // several, and never assert provenance for a run that used zero or multiple
    // calls. `used` and `calls` stay honest either way; ambiguous runs simply
    // do not attach a single program/result to the answer.
    const successIds = [...successes.keys()];
    const soleId =
      successIds.length === 1 && declaredPrograms.has(successIds[0]) ? successIds[0] : undefined;
    const executed = soleId === undefined ? undefined : successes.get(soleId);
    return {
      answer,
      codeMode: {
        used: successes.size > 0,
        tool: "run_javascript",
        executor: dynamicWorkerImplementation.identity,
        calls: declaredPrograms.size,
        program: soleId === undefined ? undefined : declaredPrograms.get(soleId),
        result: executed?.result,
        logs: executed?.logs,
      },
      profile,
    };
  });
};

const runAsk = (
  env: WorkerEnv,
  question: string,
  tenant: string,
  probe: string | null,
): Effect.Effect<AskResult> => {
  if (env.OPENAI_API_KEY !== undefined && env.OPENAI_API_KEY.length > 0) {
    return runBound(
      env,
      liveAgent(env.OPENAI_API_KEY) as unknown as DemoBinding,
      question,
      tenant,
      "openai",
    );
  }
  const agent = probe === "write" ? scriptedWriteProbeAgent : scriptedAgent;
  return runBound(env, agent, question, tenant, "scripted");
};

export default {
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
    let question = "Which customers have more than $10,000 in revenue?";
    try {
      const body = (await request.json()) as { readonly question?: unknown };
      if (typeof body.question === "string" && body.question.trim().length > 0) {
        // Bound the question so an oversized body cannot drive an unbounded prompt.
        question = body.question.slice(0, 2_000);
      }
    } catch {
      // fall back to the default question
    }
    // The tenant addresses a Durable Object, so reject anything that is not a
    // short, safe identifier rather than minting an arbitrary Object.
    const tenant = url.searchParams.get("tenant") ?? "acme";
    if (!isValidTenant(tenant)) {
      return Response.json({ error: "tenant must match /^[a-z0-9-]{1,32}$/" }, { status: 400 });
    }
    try {
      const result = await Effect.runPromise(
        runAsk(env, question, tenant, url.searchParams.get("probe")),
      );
      return Response.json(result);
    } catch (cause) {
      return Response.json(
        { error: cause instanceof Error ? cause.message : String(cause) },
        { status: 500 },
      );
    }
  },
};

// Re-exported so a consumer can compose the warehouse service directly.
export { Warehouse };
