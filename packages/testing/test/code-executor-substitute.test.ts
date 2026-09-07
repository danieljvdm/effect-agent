import {
  CodeExecutionHost,
  CodeExecutionLimits,
  CodeExecutionNamespace,
  CodeExecutionRequest,
  CodeExecutor,
} from "@effect-agent/sandbox/CodeExecutor";
import { NetworkDisabled } from "@effect-agent/sandbox/Sandbox";
import { codeExecutorConformanceCases } from "@effect-agent/testing/CodeExecutorConformance";
import {
  inProcessCodeExecutorImplementation,
  inProcessCodeExecutorLayer,
} from "@effect-agent/testing/CodeExecutorSubstitute";
import { expect, layer } from "@effect/vitest";
import { Cause, Duration, Effect, Exit } from "effect";

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

    for (const { name, source, reason } of [
      {
        name: "a returned object with a throwing getter",
        source:
          "async () => { console.log('before failure'); return { get value() { throw new Error('result getter'); } }; }",
        reason: "non-json-result",
      },
      {
        name: "a returned Proxy with a throwing prototype trap",
        source:
          "async () => { console.log('before failure'); return new Proxy({}, { getPrototypeOf() { throw new Error('result prototype'); } }); }",
        reason: "non-json-result",
      },
      {
        name: "a rejected Proxy with throwing prototype and property traps",
        source:
          "async () => { console.log('before failure'); throw new Proxy({}, { getPrototypeOf() { throw new Error('rejection prototype'); }, get() { throw new Error('rejection property'); } }); }",
        reason: "rejected",
      },
    ]) {
      it.effect(`reports ${name} as a typed program failure`, () =>
        Effect.gen(function* () {
          const error = yield* execute(request(source)).pipe(Effect.flip);

          expect(error._tag).toBe("CodeProgramFailedError");
          if (error._tag !== "CodeProgramFailedError") return;
          expect(error.reason).toBe(reason);
          expect(error.logs).toEqual(["before failure"]);
          expect(error.message.length).toBeLessThanOrEqual(4_000);
          expect(JSON.stringify(error.thrown).length).toBeLessThanOrEqual(4_002);
        }),
      );
    }

    for (const disposition of ["return", "throw"]) {
      it.effect(
        `snapshots a ${disposition} value before its getter can change during construction`,
        () =>
          Effect.gen(function* () {
            const exit = yield* execute(
              request(
                `async () => { let reads = 0; ${disposition} new Proxy({ value: 1 }, { get(target, key) { if (key === 'value' && ++reads > 2) throw new Error('later read'); return Reflect.get(target, key); } }); }`,
              ),
            ).pipe(Effect.exit);

            if (disposition === "return") {
              expect(Exit.isSuccess(exit)).toBe(true);
              if (Exit.isSuccess(exit)) {
                expect(exit.value.value).toEqual({ value: 1 });
                expect(exit.value.resourceUse.resultBytes).toBe(11);
              }
            } else {
              expect(Exit.isFailure(exit)).toBe(true);
              if (Exit.isFailure(exit)) {
                expect(Cause.hasDies(exit.cause)).toBe(false);
                const reason = exit.cause.reasons.find(Cause.isFailReason);

                expect(reason?.error._tag).toBe("CodeProgramFailedError");
                if (reason?.error._tag === "CodeProgramFailedError") {
                  expect(reason.error.thrown).toEqual({ value: 1 });
                }
              }
            }
          }),
      );
    }

    it.effect("preserves a host defect and finalizes its in-flight call", () =>
      Effect.gen(function* () {
        const defect = new Error("host defect");
        let finalized = false;
        const executor = yield* CodeExecutor;

        const exit = yield* executor
          .execute(
            CodeExecutionRequest.make({
              ...request("async () => warehouse.query({})"),
              namespaces: [CodeExecutionNamespace.make({ name: "warehouse", methods: ["query"] })],
            }),
          )
          .pipe(
            Effect.provideService(
              CodeExecutionHost,
              CodeExecutionHost.of({
                call: () =>
                  Effect.die(defect).pipe(
                    Effect.ensuring(
                      Effect.sync(() => {
                        finalized = true;
                      }),
                    ),
                  ),
              }),
            ),
            Effect.scoped,
            Effect.exit,
          );

        expect(finalized).toBe(true);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          expect(exit.cause.reasons.filter(Cause.isDieReason)).toHaveLength(1);
          expect(exit.cause.reasons.find(Cause.isDieReason)?.defect).toBe(defect);
          expect(Cause.hasFails(exit.cause)).toBe(false);
        }
      }),
    );

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
