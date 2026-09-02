import {
  CodeExecutionHost,
  CodeExecutionLimits,
  CodeExecutionRequest,
  CodeExecutor,
  NetworkDisabled,
} from "@effect-agent/sandbox";
import {
  codeExecutorConformanceCases,
  inProcessCodeExecutorImplementation,
  inProcessCodeExecutorLayer,
} from "@effect-agent/testing/code-executor";
import { expect, layer } from "@effect/vitest";
import { Duration, Effect } from "effect";

const limits = CodeExecutionLimits.make({
  maxSourceBytes: 64 * 1024,
  maxWallTime: Duration.seconds(10),
  maxLogBytes: 16 * 1024,
  maxResultBytes: 64 * 1024,
  maxHostCalls: 8,
  maxHostCallArgumentBytes: 16 * 1024,
  maxHostCallResultBytes: 32 * 1024,
});

const request = (
  source: string,
  overrides?: Partial<Pick<CodeExecutionRequest, "limits">>,
): CodeExecutionRequest =>
  CodeExecutionRequest.make({
    language: "javascript",
    source,
    namespaces: [],
    network: NetworkDisabled.make(),
    limits: overrides?.limits ?? limits,
  });

const unusedHost = CodeExecutionHost.of({
  call: () => Effect.die(new Error("no host call expected")),
});

const execute = (req: CodeExecutionRequest) =>
  Effect.gen(function* () {
    const executor = yield* CodeExecutor;

    return yield* executor.execute(req).pipe(Effect.provideService(CodeExecutionHost, unusedHost));
  }).pipe(Effect.scoped);

// The wall-clock conformance case needs the live Clock, so the suite opts out
// of the injected test services the same way the sandbox-local suite does.
layer(inProcessCodeExecutorLayer, { excludeTestServices: true })(
  "CAP-015 in-process CodeExecutor substitute",
  (it) => {
    for (const conformanceCase of codeExecutorConformanceCases({
      implementation: inProcessCodeExecutorImplementation,
    })) {
      it.effect(conformanceCase.name, () => conformanceCase.run);
    }

    it.effect("rejects a CPU limit it cannot enforce with a typed unsupported error", () =>
      Effect.gen(function* () {
        const error = yield* execute(
          request("async () => 1", {
            limits: CodeExecutionLimits.make({ ...limits, cpuMillis: 100 }),
          }),
        ).pipe(Effect.flip);

        expect(error._tag).toBe("CodeExecutorUnsupportedError");
        if (error._tag === "CodeExecutorUnsupportedError") {
          expect(error.feature).toBe("cpu-limit");
        }
      }),
    );

    it.effect(
      "shadows the obvious ambient globals as a usability check, not a security boundary",
      () =>
        Effect.gen(function* () {
          const result = yield* execute(
            request(
              "async () => ({ fetch: typeof fetch, process: typeof process, require: typeof require })",
            ),
          );

          expect(result.value).toEqual({
            fetch: "undefined",
            process: "undefined",
            require: "undefined",
          });
          expect(result.implementation.isolation).toBe("unisolated");
        }),
    );

    it.effect("keeps per-line log capture bounded and reports byte accounting", () =>
      Effect.gen(function* () {
        const result = yield* execute(
          request("async () => { console.log('a', { b: 1 }, [2, 3]); return null; }"),
        );

        expect(result.logs).toEqual(['a {"b":1} [2,3]']);
        expect(result.resourceUse.logBytes).toBeGreaterThan(0);
        expect(result.resourceUse.hostCalls).toBe(0);
      }),
    );
  },
);
