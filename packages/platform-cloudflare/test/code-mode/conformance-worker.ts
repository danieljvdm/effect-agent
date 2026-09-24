import { DurableObject } from "cloudflare:workers";
import { Duration, Effect, ManagedRuntime, Predicate, type Layer } from "effect";
import {
  CodeExecutionHost,
  CodeExecutionLimits,
  CodeExecutionNamespace,
  CodeExecutionRequest,
  CodeExecutor,
  CodeHostCallFailure,
  CodeHostCallSuccess,
  type CodeExecutionError,
} from "effect-agent/code-executor";
import { NetworkDisabled } from "effect-agent/sandbox";

import { dynamicWorkerCodeExecutorLayer } from "../../src/CloudflareCodeMode.ts";

interface WorkerEnv {
  readonly LOADER: WorkerLoader;
  readonly CODE_MODE_EXECUTORS: DurableObjectNamespace<CodeModeExecutorObject>;
}

const executorLayerFor = (env: WorkerEnv): Layer.Layer<CodeExecutor> =>
  dynamicWorkerCodeExecutorLayer({ loader: env.LOADER });

const baseLimits = CodeExecutionLimits.make({
  maxSourceBytes: 64 * 1024,
  maxWallTime: Duration.seconds(15),
  maxLogBytes: 16 * 1024,
  maxResultBytes: 64 * 1024,
  maxHostCalls: 8,
  maxHostCallArgumentBytes: 16 * 1024,
  maxHostCallResultBytes: 32 * 1024,
});

const request = (
  source: string,
  overrides?: {
    readonly limits?: CodeExecutionLimits;
    readonly namespaces?: ReadonlyArray<CodeExecutionNamespace>;
    readonly network?: CodeExecutionRequest["network"];
  },
): CodeExecutionRequest =>
  CodeExecutionRequest.make({
    language: "javascript",
    source,
    namespaces: overrides?.namespaces ?? [],
    network: overrides?.network ?? NetworkDisabled.make({}),
    limits: overrides?.limits ?? baseLimits,
  });

const unusedHost: CodeExecutionHost["Service"] = {
  call: () => Effect.die(new Error("no host call expected")),
};

const executeOutcome = (
  req: CodeExecutionRequest,
  host: CodeExecutionHost["Service"] = unusedHost,
): Effect.Effect<{ readonly tag: string; readonly detail: unknown }, never, CodeExecutor> =>
  Effect.gen(function* () {
    const executor = yield* CodeExecutor;

    return yield* executor
      .execute(req)
      .pipe(Effect.provideService(CodeExecutionHost, CodeExecutionHost.of(host)));
  }).pipe(
    Effect.scoped,
    Effect.map((result) => ({
      tag: "success",
      detail: { value: result.value, logs: result.logs, implementation: result.implementation },
    })),
    Effect.catch((error: CodeExecutionError) => Effect.succeed({ tag: error._tag, detail: error })),
  );

const runOutcome = (
  req: CodeExecutionRequest,
  layer: Layer.Layer<CodeExecutor>,
  host: CodeExecutionHost["Service"] = unusedHost,
): Effect.Effect<{ readonly tag: string; readonly detail: unknown }> =>
  executeOutcome(req, host).pipe(Effect.provide(layer));

const runAllChecks = (env: WorkerEnv): Effect.Effect<ReadonlyArray<string>> =>
  Effect.gen(function* () {
    const layer = executorLayerFor(env);
    const failures: Array<string> = [];

    // Isolated-only enforcement (testing spec §8.1): the deterministic
    // substitute cannot prove these, but a real Dynamic Worker must.
    const networkOutcome = yield* runOutcome(
      request(`async () => { const r = await fetch("https://example.com"); return r.status; }`),
      layer,
    );

    if (networkOutcome.tag !== "CodeProgramFailedError") {
      failures.push(
        `ambient network denial: expected a program failure, got ${networkOutcome.tag}`,
      );
    }

    // A non-completing pass must not outlive its deadline (ADR-0017 §9.1):
    // an asynchronously suspended program returns a typed termination and
    // never a fabricated result. workerd's own hang detection may terminate
    // the loaded worker before the executor-owned wall-clock deadline fires,
    // so either typed termination satisfies the boundary.
    const deadlineOutcome = yield* runOutcome(
      request("async () => { await new Promise(() => {}); return 1; }", {
        limits: CodeExecutionLimits.make({ ...baseLimits, maxWallTime: Duration.seconds(5) }),
      }),
      layer,
    );

    if (
      !["CodeExecutionTimeoutError", "CodeExecutorTerminatedError"].includes(deadlineOutcome.tag)
    ) {
      failures.push(
        `non-completing pass: expected a typed termination, got ${deadlineOutcome.tag}`,
      );
    }

    // End-to-end host composition through the real Worker Loader RPC.
    const namespace = CodeExecutionNamespace.make({ name: "warehouse", methods: ["query"] });

    const resultLimit = CodeExecutionLimits.make({
      ...baseLimits,
      maxHostCallResultBytes: 128,
    });

    for (const [label, hostCallResult] of [
      ["success", CodeHostCallSuccess.make({ value: "x".repeat(256) })],
      [
        "failure",
        CodeHostCallFailure.make({ error: { _tag: "OversizedFailure", detail: "x".repeat(256) } }),
      ],
    ] as const) {
      const outcome = yield* runOutcome(
        request("async () => warehouse.query({})", {
          namespaces: [namespace],
          limits: resultLimit,
        }),
        layer,
        { call: () => Effect.succeed(hostCallResult) },
      );

      if (
        outcome.tag !== "CodeOutputLimitError" ||
        !Predicate.isObject(outcome.detail) ||
        outcome.detail.surface !== "host-call-result"
      ) {
        failures.push(
          `oversized host ${label}: expected a host-call-result limit error, got ${JSON.stringify(outcome)}`,
        );
      }
    }

    return failures;
  });

export class CodeModeExecutorObject extends DurableObject<WorkerEnv> {
  readonly #runtime: ManagedRuntime.ManagedRuntime<CodeExecutor, never>;

  constructor(ctx: DurableObjectState, env: WorkerEnv) {
    super(ctx, env);
    this.#runtime = ManagedRuntime.make(executorLayerFor(this.env));
  }

  async prime(): Promise<void> {
    await this.ctx.storage.put("marker", "executor");
    await this.#runtime.runPromise(
      Effect.promise(() => this.ctx.storage.get("marker")).pipe(
        Effect.andThen(CodeExecutor),
        Effect.asVoid,
      ),
    );
  }

  async run(): Promise<{ readonly tag: string; readonly value: unknown }> {
    await this.ctx.storage.put("marker", "executor");
    const namespace = CodeExecutionNamespace.make({ name: "warehouse", methods: ["query"] });

    const host: CodeExecutionHost["Service"] = {
      call: () =>
        Effect.promise(() => this.ctx.storage.get<string>("marker")).pipe(
          Effect.map((value) => CodeHostCallSuccess.make({ value })),
        ),
    };

    return this.#runtime.runPromise(
      executeOutcome(
        request("async () => warehouse.query({ sql: 'select 1' })", {
          namespaces: [namespace],
        }),
        host,
      ).pipe(
        Effect.map((outcome) => ({
          tag: outcome.tag,
          value:
            Predicate.isObject(outcome.detail) && "value" in outcome.detail
              ? outcome.detail.value
              : null,
        })),
      ),
    );
  }
}

interface CodeModeExecutorStub {
  readonly prime: () => Promise<void>;
  readonly run: () => Promise<{ readonly tag: string; readonly value: unknown }>;
}

export default {
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    try {
      if (new URL(request.url).pathname === "/durable-object-host-call") {
        const first = env.CODE_MODE_EXECUTORS.getByName("first") as unknown as CodeModeExecutorStub;

        const second = env.CODE_MODE_EXECUTORS.getByName(
          "second",
        ) as unknown as CodeModeExecutorStub;

        await first.prime();
        await second.prime();
        const outcomes = [await second.run(), await first.run()];

        return Response.json({ outcomes });
      }
      const failures = await Effect.runPromise(runAllChecks(env));

      return Response.json({ failures });
    } catch (cause) {
      const detail =
        cause instanceof Error
          ? `${cause.constructor.name}: ${cause.message}\n${String(cause)}`
          : String(cause);

      return Response.json({ failures: [`worker threw: ${detail.slice(0, 2_000)}`] });
    }
  },
};
