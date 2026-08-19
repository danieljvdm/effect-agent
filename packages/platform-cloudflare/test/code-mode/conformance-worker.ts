import {
  CodeExecutionHost,
  CodeExecutionLimits,
  CodeExecutionNamespace,
  CodeExecutionRequest,
  CodeExecutor,
  CodeHostCallFailure,
  CodeHostCallSuccess,
  NetworkAllowlist,
  NetworkDisabled,
  type CodeExecutionError,
  type CodeHostCall,
  type CodeHostCallResult,
} from "@effect-agent/sandbox";
import { Cause, Context, Duration, Effect, Exit, Option, Predicate, type Layer } from "effect";

// Workspace-relative source import: esbuild bundles it, and the package
// barrel would pull in Node-only storage fixtures the worker cannot load.
import { codeExecutorConformanceCases } from "../../../testing/src/code-executor-conformance.ts";
import {
  CodeModeHostEntrypoint,
  dynamicWorkerCodeExecutorLayer,
  dynamicWorkerImplementation,
  type CodeModeHostStub,
} from "../../src/code-mode-executor.ts";

export { CodeModeHostEntrypoint };

interface WorkerEnv {
  readonly LOADER: WorkerLoader;
  readonly CODE_MODE_HOST: CodeModeHostStub;
}

const executorLayerFor = (env: WorkerEnv): Layer.Layer<CodeExecutor> =>
  dynamicWorkerCodeExecutorLayer({ loader: env.LOADER, hostStub: env.CODE_MODE_HOST });

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

const runOutcome = (
  req: CodeExecutionRequest,
  layer: Layer.Layer<CodeExecutor>,
  host: CodeExecutionHost["Service"] = unusedHost,
): Effect.Effect<{ readonly tag: string; readonly detail: unknown }> =>
  Effect.gen(function* () {
    const executor = yield* CodeExecutor;
    return yield* executor
      .execute(req)
      .pipe(Effect.provideService(CodeExecutionHost, CodeExecutionHost.of(host)));
  }).pipe(
    Effect.scoped,
    Effect.provide(layer),
    Effect.map((result) => ({
      tag: "success",
      detail: { value: result.value, logs: result.logs, implementation: result.implementation },
    })),
    Effect.catch((error: CodeExecutionError) => Effect.succeed({ tag: error._tag, detail: error })),
  );

/**
 * A representative subset of the shared conformance suite plus the
 * isolated-only enforcement cases, run against the real adapter in workerd.
 * The full 17-case kit runs against the deterministic substitute in the
 * testing package; every worker load here boots a fresh dynamic worker, so
 * the workerd lane proves the trust and enforcement boundaries on a curated
 * set rather than paying that cost 17 times. Case names are matched against
 * the shared kit's names so a rename here fails loudly.
 */
const workerdConformanceCaseNames: ReadonlyArray<string> = [
  "TEST-015 executes bounded JSON computation and returns the program value",
  "CAP-015 reports its isolation posture honestly in results and errors",
  "TEST-015 routes host calls through the CodeExecutionHost in program order",
  "TEST-015 a caught failed host call lets the program branch on the envelope",
  "TEST-015 an uncaught failed host call fails the program with the envelope",
  "TEST-015 fails typed on syntactically invalid source",
  "TEST-015 fails typed when the expression is not one async function",
  "TEST-015 fails typed when the final result exceeds its byte budget",
  "TEST-015 fails typed when host calls exceed the executor cap",
  "TEST-015 fails typed on a host outcome outside the protocol schema",
  "TEST-015 surfaces an uncaught program throw with its bounded log capture",
  "TEST-015 fails typed when the program returns a non-JSON value",
  "CAP-015 rejects a network allowlist it cannot enforce with a typed unsupported error",
  "TEST-015 interruption reaches in-flight host calls and pass teardown",
];

const runAllChecks = (env: WorkerEnv): Effect.Effect<ReadonlyArray<string>> =>
  Effect.gen(function* () {
    const layer = executorLayerFor(env);
    const failures: Array<string> = [];

    const allCases = codeExecutorConformanceCases({
      implementation: dynamicWorkerImplementation,
    });
    for (const name of workerdConformanceCaseNames) {
      const conformanceCase = allCases.find((candidate) => candidate.name === name);
      if (conformanceCase === undefined) {
        failures.push(`missing shared conformance case: ${name}`);
        continue;
      }
      const exit = yield* conformanceCase.run.pipe(Effect.provide(layer), Effect.exit);
      if (Exit.isFailure(exit)) {
        const violation = Cause.findErrorOption(exit.cause);
        failures.push(
          Option.isSome(violation)
            ? `${conformanceCase.name}: ${violation.value.message}`
            : `${conformanceCase.name} DEFECT: ${Cause.pretty(exit.cause).slice(0, 1_000)}`,
        );
      }
    }

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

    const allowlistOutcome = yield* runOutcome(
      request("async () => 1", {
        network: NetworkAllowlist.make({ domains: ["example.com"], ports: [443] }),
      }),
      layer,
    );
    if (allowlistOutcome.tag !== "CodeExecutorUnsupportedError") {
      failures.push(
        `network allowlist rejection: expected CodeExecutorUnsupportedError, got ${allowlistOutcome.tag}`,
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
    const calls: Array<CodeHostCall> = [];
    // A plain-object outcome (the shape the Code Mode capability's broker
    // route returns), not a class instance — the adapter must accept both.
    const host: CodeExecutionHost["Service"] = {
      call: (hostCall) =>
        Effect.sync(() => {
          calls.push(hostCall);
          return { _tag: "CodeHostCallSuccess", value: { rows: [1, 2, 3] } } as CodeHostCallResult;
        }),
    };
    const composed = yield* runOutcome(
      request(
        `async () => {
          const result = await warehouse.query({ sql: "select 1" });
          console.log("rows", result.rows.length);
          return { doubled: result.rows.map((n) => n * 2) };
        }`,
        { namespaces: [namespace] },
      ),
      layer,
      host,
    );
    if (
      composed.tag !== "success" ||
      JSON.stringify((composed.detail as { value: unknown }).value) !==
        JSON.stringify({ doubled: [2, 4, 6] })
    ) {
      failures.push(`host composition: unexpected outcome ${JSON.stringify(composed)}`);
    } else if (calls.length !== 1) {
      failures.push(`host composition: expected 1 host call, observed ${calls.length}`);
    }

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

/**
 * Distinguishes a `runFork` root fiber (default `"root"`) from a child of
 * the `execute` Scope (provided `"executor"`). Host calls must see the pass
 * Context — they still run on a sibling fiber of the guest RPC waiter so
 * the return RPC is not coupled to `entrypoint.run()`.
 */
const ExecutorFiberMarker = Context.Reference<"root" | "executor">(
  "@effect-agent/platform-cloudflare/test/ExecutorFiberMarker",
  { defaultValue: () => "root" },
);

const runHostCallScopeRegression = (env: WorkerEnv) => {
  const layer = executorLayerFor(env);
  const namespace = CodeExecutionNamespace.make({ name: "warehouse", methods: ["query"] });
  const host: CodeExecutionHost["Service"] = {
    call: () =>
      ExecutorFiberMarker.pipe(Effect.map((value) => CodeHostCallSuccess.make({ value }))),
  };
  return runOutcome(
    request("async () => warehouse.query({ sql: 'select 1' })", {
      namespaces: [namespace],
    }),
    layer,
    host,
  ).pipe(Effect.provideService(ExecutorFiberMarker, "executor"));
};

export default {
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    try {
      if (new URL(request.url).pathname === "/host-call-pass-scope") {
        return Response.json(await Effect.runPromise(runHostCallScopeRegression(env)));
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
