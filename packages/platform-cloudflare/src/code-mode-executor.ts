import {
  CodeExecutionHost,
  CodeExecutionResourceUse,
  CodeExecutionResult,
  CodeExecutionTimeoutError,
  CodeExecutor,
  CodeExecutorStartError,
  CodeExecutorTerminatedError,
  CodeExecutorUnsupportedError,
  CodeExecutionProtocolError,
  CodeHostCall,
  CodeHostCallLimitError,
  CodeHostCallResult,
  CodeOutputLimitError,
  CodeProgramFailedError,
  CodeSourceError,
  SandboxImplementation,
  type CodeExecutorExecute,
  type CodeExecutionRequest,
} from "@effect-agent/sandbox";
import { parse } from "acorn";
import { WorkerEntrypoint } from "cloudflare:workers";
import { Cause, Duration, Effect, Exit, Fiber, Layer, Option, Schema } from "effect";

/**
 * The Cloudflare Dynamic Worker `CodeExecutor` adapter (C4 of ADR-0017;
 * DEPLOY-011). Each pass loads one fresh Worker through the Worker Loader
 * with `globalOutbound: null`, so generated code has no ambient network,
 * bindings, or secrets; its only authority is the pass-scoped host stub that
 * routes back to `CodeModeHostEntrypoint` and, from there, into the pass's
 * `CodeExecutionHost` service. Platform CPU limits stop synchronous runaway
 * programs; the executor-owned wall-clock deadline interrupts asynchronously
 * suspended passes. Deployment class `E` only: the adapter records no
 * persistent state and a later pass may run in a completely different
 * isolate.
 */
export const dynamicWorkerImplementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "cloudflare-dynamic-worker",
});

/** The narrow host stub the dynamic worker receives (Workers RPC). */
export interface CodeModeHostStub {
  readonly call: (passId: string, hostCall: unknown) => Promise<unknown>;
}

interface RegisteredPass {
  readonly dispatch: (hostCall: unknown) => Promise<unknown>;
}

/**
 * Live passes by identity. Entries are Scope-managed: registered when a pass
 * opens and removed by its finalizer, so a stale harness (or a forged
 * `passId`) cannot reach any host authority.
 */
const passRegistry = new Map<string, RegisteredPass>();

/**
 * The host-side RPC target for dynamic workers. The application exposes it
 * from its Worker entry (`export { CodeModeHostEntrypoint }`) and hands the
 * adapter a same-instance stub — `ctx.exports.CodeModeHostEntrypoint()` in
 * production (a self service binding may reach a different instance and must
 * not be used there); tests bind it through Miniflare's `kCurrentWorker`.
 */
export class CodeModeHostEntrypoint extends WorkerEntrypoint {
  async call(passId: unknown, hostCall: unknown): Promise<unknown> {
    const pass = passRegistry.get(String(passId));
    if (pass === undefined) {
      throw new Error("Unknown Code Mode pass");
    }
    return pass.dispatch(hostCall);
  }
}

/**
 * The fixed harness loaded as the dynamic worker's main module. The generated
 * source becomes a module exporting one loader function — a module, never
 * `eval`. The harness installs namespace globals and a bounded console before
 * calling that loader, so even expression evaluation is resource-accounted,
 * then invokes the resulting program exactly once and returns one envelope the
 * host validates through Effect Schema.
 */
const HARNESS_MODULE = String.raw`
import { WorkerEntrypoint } from "cloudflare:workers";
import loadProgram from "./program.js";

const encoder = new TextEncoder();
const utf8 = (text) => encoder.encode(text).byteLength;
const safeText = (value) => {
  try {
    if (value instanceof Error) return (value.name + ": " + value.message).slice(0, 4000);
    if (typeof value === "string") return value.slice(0, 4000);
    const encoded = JSON.stringify(value);
    return (encoded === undefined ? String(value) : encoded).slice(0, 4000);
  } catch {
    return "[unserializable value]";
  }
};
const safeJson = (value) => {
  try {
    const encoded = JSON.stringify(value);
    if (encoded !== undefined && encoded.length <= 4000) return JSON.parse(encoded);
  } catch {}
  return safeText(value);
};

export default class CodeModeHarness extends WorkerEntrypoint {
  async run() {
    const config = JSON.parse(this.env.CODE_MODE_PASS);
    const host = this.env.CODE_MODE_HOST;
    const limits = config.limits;
    const logs = [];
    let logBytes = 0;
    let fatal;
    const boundedLogs = () => logs.slice(0, 4096);
    const write = (...values) => {
      const joined = values.map(safeText).join(" ");
      const line = joined.length > 16000 ? joined.slice(0, 15999) + "…" : joined;
      const bytes = utf8(line);
      if (logs.length >= 4096 || logBytes + bytes > limits.maxLogBytes) {
        fatal = fatal ?? { _tag: "log-limit", observed: logBytes + bytes, logs: boundedLogs() };
        throw new Error("code-mode log limit exceeded");
      }
      logs.push(line);
      logBytes += bytes;
    };
    globalThis.console = { log: write, info: write, warn: write, error: write, debug: write };

    let hostCalls = 0;
    const makeMethod = (namespace, method) => async (argument) => {
      hostCalls += 1;
      if (hostCalls > limits.maxHostCalls) {
        fatal = fatal ?? { _tag: "host-call-limit", logs: boundedLogs() };
        throw new Error("code-mode host-call limit exceeded");
      }
      let argText;
      try {
        argText = JSON.stringify(argument);
      } catch {}
      if (argText === undefined || utf8(argText) > limits.maxHostCallArgumentBytes) {
        fatal = fatal ?? {
          _tag: "argument-limit",
          observed: argText === undefined ? 0 : utf8(argText),
          logs: boundedLogs(),
        };
        throw new Error("code-mode host-call argument limit exceeded");
      }
      const outcome = await host.call(config.passId, {
        namespace,
        method,
        argument: JSON.parse(argText),
      });
      if (outcome !== null && typeof outcome === "object" && outcome._tag === "CodeHostCallSuccess") {
        return outcome.value;
      }
      if (outcome !== null && typeof outcome === "object" && outcome._tag === "CodeHostCallFailure") {
        throw outcome.error;
      }
      fatal = fatal ?? { _tag: "protocol", message: "host returned an unrecognized outcome" };
      throw new Error("code-mode host protocol violation");
    };
    for (const namespace of config.namespaces) {
      const methods = {};
      for (const method of namespace.methods) {
        methods[method] = makeMethod(namespace.name, method);
      }
      globalThis[namespace.name] = methods;
    }

    try {
      // program.js is imported statically, so invalid syntax still fails the
      // module load and maps to a source error. Its module body only defines
      // loadProgram; the guest expression itself is evaluated here, after the
      // bounded console and namespace globals are installed.
      const program = await loadProgram();
      if (typeof program !== "function") {
        return { _tag: "source-not-a-function", actual: typeof program };
      }
      const value = await program();
      if (fatal !== undefined) return fatal;
      let text;
      try {
        text = JSON.stringify(value);
      } catch {}
      if (text === undefined) {
        return {
          _tag: "program-failed",
          reason: "non-json-result",
          thrown: null,
          message: "The program must return a JSON value",
          logs: boundedLogs(),
        };
      }
      const resultBytes = utf8(text);
      if (resultBytes > limits.maxResultBytes) {
        return { _tag: "result-limit", observed: resultBytes, logs: boundedLogs() };
      }
      return {
        _tag: "completed",
        value: JSON.parse(text),
        logs: boundedLogs(),
        hostCalls,
        logBytes,
        resultBytes,
      };
    } catch (cause) {
      if (fatal !== undefined) return fatal;
      return {
        _tag: "program-failed",
        reason: cause instanceof Error ? "threw" : "rejected",
        thrown: safeJson(cause),
        message: safeText(cause),
        logs: boundedLogs(),
      };
    }
  }
}
`;

const BoundedLogs = Schema.Array(Schema.String.check(Schema.isMaxLength(16 * 1024))).check(
  Schema.isMaxLength(4_096),
);

const HarnessCompleted = Schema.Struct({
  _tag: Schema.Literal("completed"),
  value: Schema.Json,
  logs: BoundedLogs,
  hostCalls: Schema.Natural,
  logBytes: Schema.Natural,
  resultBytes: Schema.Natural,
});
const HarnessSourceInvalid = Schema.Struct({
  _tag: Schema.Literal("source-invalid"),
  message: Schema.String,
});
const HarnessNotAFunction = Schema.Struct({
  _tag: Schema.Literal("source-not-a-function"),
  actual: Schema.String,
});
const HarnessProgramFailed = Schema.Struct({
  _tag: Schema.Literal("program-failed"),
  reason: Schema.Literals(["threw", "rejected", "non-json-result"]),
  thrown: Schema.Json,
  message: Schema.String,
  logs: BoundedLogs,
});
const HarnessLogLimit = Schema.Struct({
  _tag: Schema.Literal("log-limit"),
  observed: Schema.Natural,
  logs: BoundedLogs,
});
const HarnessArgumentLimit = Schema.Struct({
  _tag: Schema.Literal("argument-limit"),
  observed: Schema.Natural,
  logs: BoundedLogs,
});
const HarnessResultLimit = Schema.Struct({
  _tag: Schema.Literal("result-limit"),
  observed: Schema.Natural,
  logs: BoundedLogs,
});
const HarnessHostCallLimit = Schema.Struct({
  _tag: Schema.Literal("host-call-limit"),
  logs: BoundedLogs,
});
const HarnessProtocol = Schema.Struct({
  _tag: Schema.Literal("protocol"),
  message: Schema.String,
});
const HarnessOutcome = Schema.Union([
  HarnessCompleted,
  HarnessSourceInvalid,
  HarnessNotAFunction,
  HarnessProgramFailed,
  HarnessLogLimit,
  HarnessArgumentLimit,
  HarnessResultLimit,
  HarnessHostCallLimit,
  HarnessProtocol,
]);

const decodeHarnessOutcome = (value: unknown) => {
  try {
    return Schema.decodeUnknownOption(HarnessOutcome)(value);
  } catch {
    return Option.none<typeof HarnessOutcome.Type>();
  }
};

const decodeHostCall = (value: unknown) => {
  try {
    return Schema.decodeUnknownOption(CodeHostCall)(value);
  } catch {
    return Option.none<CodeHostCall>();
  }
};

const decodeHostCallResult = (value: unknown) => {
  try {
    return Schema.decodeUnknownOption(CodeHostCallResult)(value);
  } catch {
    return Option.none<CodeHostCallResult>();
  }
};

/**
 * Project a host outcome to the plain JSON envelope the harness reads. A
 * `CodeExecutionHost` may return either real `CodeHostCallResult` instances
 * (the substitute and conformance kit) or plain-object equivalents (the Code
 * Mode capability's broker route), so this reads the shared fields rather than
 * `Schema.encodeSync`, which would reject a plain object.
 */
interface EncodedHostResultPayload {
  readonly encodedPayload: string;
  readonly resultBytes: number;
}

const encodeHostResultPayload = (
  outcome: CodeHostCallResult,
): EncodedHostResultPayload | undefined => {
  try {
    const payload = outcome._tag === "CodeHostCallSuccess" ? outcome.value : outcome.error;
    const encodedPayload = JSON.stringify(payload);
    if (encodedPayload === undefined) return undefined;
    return {
      encodedPayload,
      resultBytes: utf8ByteLength(encodedPayload),
    };
  } catch {
    return undefined;
  }
};

const utf8ByteLength = (value: string): number => {
  let total = 0;
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }
  return total;
};

type HostDispatchFailure =
  | { readonly _tag: "host-call-limit" }
  | { readonly _tag: "host-call-result-limit"; readonly observed: number }
  | { readonly _tag: "host-call-protocol" }
  | { readonly _tag: "wall-clock-timeout" }
  | { readonly _tag: "host-call-defect"; readonly cause: Cause.Cause<never> };

interface QueuedHostCall {
  readonly call: CodeHostCall;
  readonly resolve: (value: unknown) => void;
  readonly reject: (reason: unknown) => void;
}

interface ActiveHostCall {
  readonly fiber: Fiber.Fiber<Record<string, unknown>, never>;
  readonly settlement: Promise<void>;
}

/** Reserved global names the harness owns inside the dynamic worker. */
const reservedHarnessGlobals = new Set(["console"]);

/** Keep syntax admission byte-for-byte aligned with the module sent to Worker Loader. */
const guestProgramModule = (source: string): string =>
  `export default async function loadProgram() {\n  return (\n${source}\n  );\n}`;

/**
 * Parse the model-supplied source in the exact async loader context where it
 * executes and admit only the loader's single return expression. This accepts
 * valid bare `await` expressions while preventing source from closing the
 * generated loader and appending another statement. Delimiter wrapping is not
 * an isolation boundary; this host-side parse and shape check is.
 */
const validateGuestExpression = (source: string): Effect.Effect<void, CodeSourceError> =>
  Effect.try({
    try: () => parse(guestProgramModule(source), { ecmaVersion: "latest", sourceType: "module" }),
    catch: (cause) =>
      CodeSourceError.make({
        implementation: dynamicWorkerImplementation,
        reason: "invalid",
        message: `The source is not valid JavaScript: ${String(cause).slice(0, 7_900)}`,
      }),
  }).pipe(
    Effect.flatMap((program) => {
      const exported = program.body[0];
      const declaration =
        exported?.type === "ExportDefaultDeclaration" ? exported.declaration : undefined;
      const statement =
        declaration?.type === "FunctionDeclaration" ? declaration.body.body[0] : undefined;
      const isSingleExpression =
        program.body.length === 1 &&
        declaration?.type === "FunctionDeclaration" &&
        declaration.async &&
        declaration.body.body.length === 1 &&
        statement?.type === "ReturnStatement" &&
        statement.argument !== null;
      return isSingleExpression
        ? Effect.void
        : CodeSourceError.make({
            implementation: dynamicWorkerImplementation,
            reason: "invalid",
            message: "The source must be exactly one JavaScript expression",
          });
    }),
  );

export interface DynamicWorkerCodeExecutorOptions {
  /** The `worker_loader` binding. */
  readonly loader: WorkerLoader;
  /**
   * A SAME-INSTANCE stub of `CodeModeHostEntrypoint`. In production create it
   * with `ctx.exports.CodeModeHostEntrypoint()`; a cross-instance stub would
   * dispatch host calls into an isolate without this pass's registry entry.
   */
  readonly hostStub: CodeModeHostStub;
  /** Compatibility date for dynamic workers; defaults to `2025-05-01`. */
  readonly compatibilityDate?: string | undefined;
}

const passCounterState = { next: 0 };

const makeExecute = (options: DynamicWorkerCodeExecutorOptions): CodeExecutorExecute =>
  Effect.fn("DynamicWorkerCodeExecutor.execute")(function* (request: CodeExecutionRequest) {
    if (request.network._tag !== "NetworkDisabled") {
      return yield* CodeExecutorUnsupportedError.make({
        implementation: dynamicWorkerImplementation,
        feature: "network",
        message:
          "The Dynamic Worker executor denies all egress with globalOutbound: null; an allowlist is not supported in the first slice",
      });
    }
    const sourceBytes = utf8ByteLength(request.source);
    if (sourceBytes > request.limits.maxSourceBytes) {
      return yield* CodeSourceError.make({
        implementation: dynamicWorkerImplementation,
        reason: "oversized",
        message: `Source is ${sourceBytes} bytes; the request allows ${request.limits.maxSourceBytes}`,
      });
    }
    yield* validateGuestExpression(request.source);
    for (const namespace of request.namespaces) {
      if (reservedHarnessGlobals.has(namespace.name)) {
        return yield* CodeExecutorUnsupportedError.make({
          implementation: dynamicWorkerImplementation,
          feature: "namespaces",
          message: `Namespace ${namespace.name} collides with a harness binding`,
        });
      }
    }

    const host = yield* CodeExecutionHost;
    passCounterState.next += 1;
    // The pass id is the only credential a loaded program presents to reach
    // its host authority, so it must be unguessable: a program cannot forge
    // another pass's id even if two passes were ever concurrent (the broker
    // keeps them sequential, but the id must not rely on that). The counter
    // prefix keeps ids debuggable; the random suffix makes them unforgeable.
    const passId = `code-mode-pass-${passCounterState.next}-${crypto.randomUUID()}`;

    const startedAt = performance.now();
    const passDeadline = startedAt + Duration.toMillis(request.limits.maxWallTime);
    const remainingPassWallTime = (): Duration.Duration =>
      Duration.millis(Math.max(0, passDeadline - performance.now()));
    let issuedHostCalls = 0;
    let passOpen = true;
    const queuedHostCalls: Array<QueuedHostCall> = [];
    let activeHostCall: ActiveHostCall | undefined;
    let hostDispatchFailure: HostDispatchFailure | undefined;
    let propagatedHostDispatchFailure: HostDispatchFailure | undefined;
    let signalHostDispatchFailure = (_failure: HostDispatchFailure): void => undefined;
    const hostDispatchFailureSignal = new Promise<HostDispatchFailure>((resolve) => {
      signalHostDispatchFailure = resolve;
    });
    const rejectQueuedHostCalls = (reason: Error): void => {
      for (const queued of queuedHostCalls.splice(0)) {
        queued.reject(reason);
      }
    };
    const recordHostDispatchFailure = (failure: HostDispatchFailure): void => {
      if (hostDispatchFailure !== undefined) return;
      hostDispatchFailure = failure;
      signalHostDispatchFailure(failure);
      rejectQueuedHostCalls(new Error("Code Mode pass failed"));
    };
    const failHostDispatch = (failure: HostDispatchFailure) => {
      switch (failure._tag) {
        case "host-call-limit":
          return Effect.fail(
            CodeHostCallLimitError.make({
              implementation: dynamicWorkerImplementation,
              limit: request.limits.maxHostCalls,
              logs: [],
            }),
          );
        case "host-call-result-limit":
          return Effect.fail(
            CodeOutputLimitError.make({
              implementation: dynamicWorkerImplementation,
              surface: "host-call-result",
              limit: request.limits.maxHostCallResultBytes,
              observed: failure.observed,
              logs: [],
            }),
          );
        case "host-call-protocol":
          return Effect.fail(
            CodeExecutionProtocolError.make({
              implementation: dynamicWorkerImplementation,
              message: "The execution host returned a value outside the CodeHostCallResult schema",
            }),
          );
        case "wall-clock-timeout":
          return Effect.fail(
            CodeExecutionTimeoutError.make({
              implementation: dynamicWorkerImplementation,
              kind: "wall-clock",
              maxWallTime: request.limits.maxWallTime,
              logs: [],
            }),
          );
        case "host-call-defect":
          return Effect.failCause(failure.cause);
      }
    };
    const propagateHostDispatchFailure = (failure: HostDispatchFailure) =>
      Effect.gen(function* () {
        propagatedHostDispatchFailure = failure;
        return yield* failHostDispatch(failure);
      });

    const startNextHostCall = (): void => {
      if (!passOpen || hostDispatchFailure !== undefined || activeHostCall !== undefined) return;
      const queued = queuedHostCalls.shift();
      if (queued === undefined) return;

      // A Dynamic Worker callback is a new Workers RPC into the loader isolate. Running the
      // complete host call on an independent root fiber breaks its dependency on the still-open
      // guest RPC. Retaining the handle and starting one call at a time preserves bounded,
      // serialized execution and lets pass teardown interrupt and await the active call.
      const fiber = Effect.runFork(
        Effect.yieldNow.pipe(
          Effect.andThen(host.call(queued.call)),
          Effect.timeoutOrElse({
            duration: remainingPassWallTime(),
            orElse: () =>
              Effect.sync(() => {
                recordHostDispatchFailure({ _tag: "wall-clock-timeout" });
                throw new Error("code-mode host call exceeded the pass wall-clock limit");
              }),
          }),
          Effect.map((outcome) => {
            const decoded = decodeHostCallResult(outcome);
            if (Option.isNone(decoded)) {
              recordHostDispatchFailure({ _tag: "host-call-protocol" });
              throw new Error("host-call protocol violation");
            }
            const encoded = encodeHostResultPayload(decoded.value);
            if (
              encoded === undefined ||
              encoded.resultBytes > request.limits.maxHostCallResultBytes
            ) {
              recordHostDispatchFailure({
                _tag: "host-call-result-limit",
                observed: encoded?.resultBytes ?? 0,
              });
              throw new Error("host-call result limit exceeded");
            }
            const normalizedPayload: unknown = JSON.parse(encoded.encodedPayload);
            return decoded.value._tag === "CodeHostCallSuccess"
              ? { _tag: "CodeHostCallSuccess", value: normalizedPayload }
              : { _tag: "CodeHostCallFailure", error: normalizedPayload };
          }),
        ),
      );
      const settlement = Effect.runPromise(Fiber.await(fiber))
        .then((exit) => {
          if (Exit.isSuccess(exit)) {
            queued.resolve(exit.value);
            return;
          }
          if (!Cause.hasInterruptsOnly(exit.cause)) {
            recordHostDispatchFailure({ _tag: "host-call-defect", cause: exit.cause });
          }
          queued.reject(new Error("Code Mode host call failed"));
        })
        .finally(() => {
          if (activeHostCall?.fiber === fiber) activeHostCall = undefined;
          startNextHostCall();
        });
      activeHostCall = { fiber, settlement };
    };

    const dispatch = (hostCall: unknown): Promise<unknown> => {
      if (!passOpen) {
        return Promise.reject(new Error("Code Mode pass is closing"));
      }
      issuedHostCalls += 1;
      if (issuedHostCalls > request.limits.maxHostCalls) {
        recordHostDispatchFailure({ _tag: "host-call-limit" });
        return Promise.reject(new Error("host-call limit exceeded"));
      }
      const decoded = decodeHostCall(hostCall);
      if (Option.isNone(decoded)) {
        return Promise.reject(new TypeError("host calls must match the CodeHostCall schema"));
      }
      return new Promise((resolve, reject) => {
        queuedHostCalls.push({ call: decoded.value, resolve, reject });
        startNextHostCall();
      });
    };

    const closeHostDispatch = Effect.gen(function* () {
      passOpen = false;
      passRegistry.delete(passId);
      rejectQueuedHostCalls(new Error("Code Mode pass is closing"));
      const active = activeHostCall;
      if (active !== undefined) {
        yield* Fiber.interrupt(active.fiber);
        yield* Effect.promise(() => active.settlement);
        if (activeHostCall === active) activeHostCall = undefined;
      }
      return hostDispatchFailure;
    });
    const stopHostDispatch = closeHostDispatch.pipe(
      Effect.flatMap((failure) =>
        failure !== undefined && propagatedHostDispatchFailure !== failure
          ? propagateHostDispatchFailure(failure)
          : Effect.void,
      ),
    );
    const releaseHostDispatch = closeHostDispatch.pipe(
      Effect.flatMap((failure) => {
        if (failure?._tag !== "host-call-defect" || propagatedHostDispatchFailure === failure) {
          return Effect.void;
        }
        propagatedHostDispatchFailure = failure;
        return Effect.failCause(failure.cause);
      }),
    );

    yield* Effect.acquireRelease(
      Effect.sync(() => {
        passRegistry.set(passId, { dispatch });
      }),
      () => releaseHostDispatch,
    );

    const workerCode = {
      compatibilityDate: options.compatibilityDate ?? "2025-05-01",
      allowExperimental: true,
      mainModule: "harness.js",
      modules: {
        "harness.js": HARNESS_MODULE,
        "program.js": guestProgramModule(request.source),
      },
      env: {
        CODE_MODE_HOST: options.hostStub,
        CODE_MODE_PASS: JSON.stringify({
          passId,
          namespaces: request.namespaces.map((namespace) => ({
            name: namespace.name,
            methods: namespace.methods,
          })),
          limits: {
            maxLogBytes: request.limits.maxLogBytes,
            maxResultBytes: request.limits.maxResultBytes,
            maxHostCalls: request.limits.maxHostCalls,
            maxHostCallArgumentBytes: request.limits.maxHostCallArgumentBytes,
          },
        }),
      },
      globalOutbound: null,
      ...(request.limits.cpuMillis === undefined
        ? {}
        : {
            limits: {
              cpuMs: request.limits.cpuMillis,
              subRequests: request.limits.maxHostCalls + 8,
            },
          }),
    };

    const worker = yield* Effect.acquireRelease(
      Effect.try({
        try: () => options.loader.load(workerCode as never),
        catch: (cause) => {
          const text = cause instanceof Error ? cause.message : String(cause);
          // Blame the program's source ONLY on a genuine compile diagnostic;
          // any other load rejection is an infrastructure start failure, not
          // the model's fault (see classifyWorkerFailure for the same split).
          if (/syntaxerror|failed to (compile|parse)/i.test(text)) {
            return CodeSourceError.make({
              implementation: dynamicWorkerImplementation,
              reason: "invalid",
              message: text.slice(0, 8_000),
            });
          }
          return CodeExecutorStartError.make({
            implementation: dynamicWorkerImplementation,
            message: `The Worker Loader rejected the pass: ${text}`.slice(0, 8_000),
            cause,
          });
        },
      }),
      (stub) =>
        Effect.sync(() => {
          (stub as Partial<Record<typeof Symbol.dispose, () => void>>)[Symbol.dispose]?.();
        }),
    );

    const rpc = Effect.tryPromise({
      try: async () => {
        const entrypoint = worker.getEntrypoint() as unknown as {
          run(): Promise<unknown>;
        };
        return await entrypoint.run();
      },
      catch: (cause) => classifyWorkerFailure(cause, request.limits.maxWallTime),
    });

    const raw = yield* Effect.raceFirst(
      rpc.pipe(
        Effect.timeoutOrElse({
          duration: remainingPassWallTime(),
          orElse: () =>
            CodeExecutionTimeoutError.make({
              implementation: dynamicWorkerImplementation,
              kind: "wall-clock",
              maxWallTime: request.limits.maxWallTime,
              logs: [],
            }),
        }),
      ),
      Effect.promise(() => hostDispatchFailureSignal).pipe(
        Effect.flatMap(propagateHostDispatchFailure),
      ),
    );
    yield* stopHostDispatch;
    const finishedAt = performance.now();

    if (hostDispatchFailure !== undefined) {
      return yield* propagateHostDispatchFailure(hostDispatchFailure);
    }

    const outcome = decodeHarnessOutcome(raw);
    if (Option.isNone(outcome)) {
      return yield* CodeExecutionProtocolError.make({
        implementation: dynamicWorkerImplementation,
        message: "The dynamic worker returned a value outside the harness envelope schema",
      });
    }
    switch (outcome.value._tag) {
      case "completed": {
        return CodeExecutionResult.make({
          implementation: dynamicWorkerImplementation,
          value: outcome.value.value,
          logs: outcome.value.logs,
          resourceUse: CodeExecutionResourceUse.make({
            wallTime: Duration.millis(Math.max(0, finishedAt - startedAt)),
            hostCalls: outcome.value.hostCalls,
            logBytes: outcome.value.logBytes,
            resultBytes: outcome.value.resultBytes,
          }),
        });
      }
      case "source-invalid": {
        return yield* CodeSourceError.make({
          implementation: dynamicWorkerImplementation,
          reason: "invalid",
          message: outcome.value.message.slice(0, 8_000),
        });
      }
      case "source-not-a-function": {
        return yield* CodeSourceError.make({
          implementation: dynamicWorkerImplementation,
          reason: "not-a-function",
          message: `The source expression evaluated to ${outcome.value.actual}; it must evaluate to one async function`,
        });
      }
      case "program-failed": {
        return yield* CodeProgramFailedError.make({
          implementation: dynamicWorkerImplementation,
          reason: outcome.value.reason,
          thrown: outcome.value.thrown,
          message: outcome.value.message.slice(0, 8_000),
          logs: outcome.value.logs,
        });
      }
      case "log-limit": {
        return yield* CodeOutputLimitError.make({
          implementation: dynamicWorkerImplementation,
          surface: "logs",
          limit: request.limits.maxLogBytes,
          observed: outcome.value.observed,
          logs: outcome.value.logs,
        });
      }
      case "argument-limit": {
        return yield* CodeOutputLimitError.make({
          implementation: dynamicWorkerImplementation,
          surface: "host-call-argument",
          limit: request.limits.maxHostCallArgumentBytes,
          observed: outcome.value.observed,
          logs: outcome.value.logs,
        });
      }
      case "result-limit": {
        return yield* CodeOutputLimitError.make({
          implementation: dynamicWorkerImplementation,
          surface: "result",
          limit: request.limits.maxResultBytes,
          observed: outcome.value.observed,
          logs: outcome.value.logs,
        });
      }
      case "host-call-limit": {
        return yield* CodeHostCallLimitError.make({
          implementation: dynamicWorkerImplementation,
          limit: request.limits.maxHostCalls,
          logs: outcome.value.logs,
        });
      }
      case "protocol": {
        return yield* CodeExecutionProtocolError.make({
          implementation: dynamicWorkerImplementation,
          message: outcome.value.message.slice(0, 8_000),
        });
      }
    }
  });

/**
 * Expected worker-level failures map into the typed union with bounded
 * diagnostics; anything unrecognized stays a start/termination error rather
 * than a fabricated program result.
 */
const classifyWorkerFailure = (
  cause: unknown,
  maxWallTime: Duration.Duration,
):
  | CodeExecutionTimeoutError
  | CodeExecutorTerminatedError
  | CodeExecutorStartError
  | CodeSourceError => {
  const text = (() => {
    try {
      return cause instanceof Error ? `${cause.name}: ${cause.message}` : String(cause);
    } catch {
      return "[unserializable worker failure]";
    }
  })();
  // `WorkerLoader.load()` is lazy, so a module-compile error in the generated
  // program surfaces here at first use. Blame the program's source ONLY on a
  // genuine compile diagnostic (a `SyntaxError` or an explicit compile
  // failure) — the fixed harness is valid, so the fault is in program.js. A
  // bare "failed to start Worker" without a compile diagnostic is an
  // infrastructure start failure, not the model's fault, so it must NOT be
  // misclassified as a source error.
  if (/syntaxerror|failed to (compile|parse)/i.test(text)) {
    return CodeSourceError.make({
      implementation: dynamicWorkerImplementation,
      reason: "invalid",
      message: text.slice(0, 8_000),
    });
  }
  if (/cpu/i.test(text)) {
    return CodeExecutionTimeoutError.make({
      implementation: dynamicWorkerImplementation,
      kind: "cpu",
      maxWallTime,
      logs: [],
    });
  }
  if (/failed to start worker/i.test(text)) {
    return CodeExecutorStartError.make({
      implementation: dynamicWorkerImplementation,
      message: text.slice(0, 8_000),
      cause,
    });
  }
  return CodeExecutorTerminatedError.make({
    implementation: dynamicWorkerImplementation,
    message: text.slice(0, 8_000),
  });
};

/** Layer building the Dynamic Worker `CodeExecutor` from resolved bindings. */
export const dynamicWorkerCodeExecutorLayer = (
  options: DynamicWorkerCodeExecutorOptions,
): Layer.Layer<CodeExecutor> =>
  Layer.succeed(CodeExecutor)(CodeExecutor.of({ execute: makeExecute(options) }));
