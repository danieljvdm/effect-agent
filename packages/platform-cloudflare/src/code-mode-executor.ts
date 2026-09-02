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
import { RpcTarget } from "cloudflare:workers";
import { Clock, Duration, Effect, Exit, Fiber, Layer, Option, Queue, Schema } from "effect";

import { safeCauseDiagnostic, safeCauseMessage } from "./boundary.ts";

/**
 * The Cloudflare Dynamic Worker `CodeExecutor` adapter (C4 of ADR-0017;
 * DEPLOY-011). Each pass loads one fresh Worker through the Worker Loader
 * with `globalOutbound: null`, so generated code has no ambient network,
 * bindings, or secrets; its only authority is the pass-scoped RPC target that
 * routes back into the owning event's `CodeExecutionHost` service. Platform
 * CPU limits stop synchronous runaway programs; the executor-owned wall-clock
 * deadline interrupts asynchronously suspended passes. Deployment class `E`
 * only: the adapter records no persistent state and a later pass may run in a
 * completely different isolate.
 */
export const dynamicWorkerImplementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "cloudflare-dynamic-worker",
});

interface CodeModePassHost extends Rpc.RpcTargetBranded {
  readonly call: (hostCall: unknown) => Promise<unknown>;
}

interface CodeModeHarnessEntrypoint extends Rpc.WorkerEntrypointBranded {
  readonly run: (host: CodeModePassHost) => Promise<unknown>;
}

/**
 * One object-capability endpoint for one execution pass. Workers RPC invokes
 * the target in the request context where it was created, so the native
 * Promise returned by `dispatch` and the Effect fiber that settles it share
 * one I/O owner. Passing the target as `run()`'s argument also scopes the
 * remote stub to that RPC call; no request state lives at module scope.
 */
class CodeModePassHostTarget extends RpcTarget implements CodeModePassHost {
  readonly #dispatch: (hostCall: unknown) => Promise<unknown>;

  constructor(dispatch: (hostCall: unknown) => Promise<unknown>) {
    super();
    this.#dispatch = dispatch;
  }

  call(hostCall: unknown): Promise<unknown> {
    return this.#dispatch(hostCall);
  }
}

/**
 * The fixed harness loaded as the dynamic worker's main module. The generated
 * source becomes `program.mjs` (`export default (<expression>);`) — a module,
 * never `eval`. The harness installs namespace globals and a bounded console,
 * imports the program, invokes it exactly once, and returns one envelope the
 * host validates through Effect Schema.
 */
const HARNESS_MODULE = String.raw`
import { WorkerEntrypoint } from "cloudflare:workers";
import programDefault from "./program.js";

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
  async run(host) {
    const config = this.env.CODE_MODE_PASS;
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
      const outcome = await host.call({
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

    // program.js is imported statically at the top of this module, so a
    // syntactically invalid program fails the whole harness at load (mapped
    // to a source error by the host). Using a static import keeps this module
    // free of dynamic-import expressions, which single-script Miniflare hosts
    // reject. The isolation boundary does NOT depend on the ordering of this
    // import versus the console/namespace shims installed below: the loaded
    // Worker has globalOutbound: null and no bindings, secrets, or env from
    // the Worker Loader config BEFORE any module in the graph evaluates, so
    // module-level program code has no ambient authority regardless. The
    // shims below are usability wrappers (bounded console, namespace globals),
    // and the accepted program is a single async-function expression whose
    // body runs only when invoked here — after the shims exist.
    const program = programDefault;
    if (typeof program !== "function") {
      return { _tag: "source-not-a-function", actual: typeof program };
    }
    try {
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

const HarnessCompleted = Schema.TaggedStruct("completed", {
  value: Schema.Json,
  logs: BoundedLogs,
  hostCalls: Schema.Natural,
  logBytes: Schema.Natural,
  resultBytes: Schema.Natural,
});

const HarnessSourceInvalid = Schema.TaggedStruct("source-invalid", {
  message: Schema.String,
});

const HarnessNotAFunction = Schema.TaggedStruct("source-not-a-function", {
  actual: Schema.String,
});

const HarnessProgramFailed = Schema.TaggedStruct("program-failed", {
  reason: Schema.Literals(["threw", "rejected", "non-json-result"]),
  thrown: Schema.Json,
  message: Schema.String,
  logs: BoundedLogs,
});

const HarnessLogLimit = Schema.TaggedStruct("log-limit", {
  observed: Schema.Natural,
  logs: BoundedLogs,
});

const HarnessArgumentLimit = Schema.TaggedStruct("argument-limit", {
  observed: Schema.Natural,
  logs: BoundedLogs,
});

const HarnessResultLimit = Schema.TaggedStruct("result-limit", {
  observed: Schema.Natural,
  logs: BoundedLogs,
});

const HarnessHostCallLimit = Schema.TaggedStruct("host-call-limit", {
  logs: BoundedLogs,
});

const HarnessProtocol = Schema.TaggedStruct("protocol", {
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

const HarnessPassConfig = Schema.Struct({
  namespaces: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString,
      methods: Schema.Array(Schema.NonEmptyString).check(Schema.isMaxLength(64)),
    }),
  ).check(Schema.isMaxLength(32)),
  limits: Schema.Struct({
    maxLogBytes: Schema.Natural,
    maxResultBytes: Schema.Natural,
    maxHostCalls: Schema.Natural,
    maxHostCallArgumentBytes: Schema.Natural,
  }),
});

const encodeHarnessPassConfig = Schema.encodeSync(HarnessPassConfig);
const encodeJsonPayload = Schema.encodeSync(Schema.fromJsonString(Schema.Json));
const decodeJsonPayload = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Json));

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

/** Dispose a Cloudflare RPC handle when the runtime supplies its untyped disposal hook. @internal */
export const disposeRpcHandle = (handle: unknown): Effect.Effect<void> =>
  Effect.try({
    try: () => {
      if ((typeof handle !== "object" && typeof handle !== "function") || handle === null) return;
      if (!(Symbol.dispose in handle)) return;
      const dispose = Reflect.get(handle, Symbol.dispose);

      if (typeof dispose === "function") {
        Reflect.apply(dispose, handle, []);
      }
    },
    catch: (cause) =>
      safeCauseDiagnostic(cause, "The Cloudflare RPC disposal hook failed without a diagnostic"),
  }).pipe(
    Effect.catch((diagnostic) =>
      Effect.logWarning(`Cloudflare RPC handle disposal failed: ${diagnostic}`).pipe(
        Effect.ignoreCause,
      ),
    ),
  );

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
    const encodedPayload = encodeJsonPayload(payload);

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

interface QueuedHostCall {
  readonly call: CodeHostCall;
  readonly resolve: (value: unknown) => void;
  readonly reject: (reason: unknown) => void;
}

type HostWork =
  | { readonly _tag: "call"; readonly queued: QueuedHostCall }
  | { readonly _tag: "limit" };

type HostDispatchError =
  | CodeExecutionTimeoutError
  | CodeOutputLimitError
  | CodeExecutionProtocolError
  | CodeHostCallLimitError;

/** Reserved global names the harness owns inside the dynamic worker. */
const reservedHarnessGlobals = new Set(["console"]);

export interface DynamicWorkerCodeExecutorOptions {
  /** The `worker_loader` binding. */
  readonly loader: WorkerLoader;
  /** Compatibility date for dynamic workers; defaults to `2025-05-01`. */
  readonly compatibilityDate?: string | undefined;
}

const makeExecute = (
  options: DynamicWorkerCodeExecutorOptions,
  clock: Clock.Clock,
): CodeExecutorExecute =>
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

    // This synchronous clock access is confined to callbacks that must compute a timeout
    // immediately. The Clock service remains the authority, so tests and hosts can replace it.
    const startedAt = clock.monotonicTimeNanosUnsafe();
    const passDeadline = startedAt + Duration.toNanosUnsafe(request.limits.maxWallTime);

    const remainingPassWallTime = (): Duration.Duration => {
      const now = clock.monotonicTimeNanosUnsafe();

      return Duration.nanos(passDeadline > now ? passDeadline - now : 0n);
    };

    let issuedHostCalls = 0;
    let passOpen = true;
    const queuedHostCalls: Array<QueuedHostCall> = [];
    let passFailure: HostDispatchError | undefined;

    const failPass = (error: HostDispatchError): void => {
      if (passFailure === undefined) passFailure = error;
    };

    const rejectQueuedHostCalls = (reason: Error): void => {
      for (const queued of queuedHostCalls.splice(0)) {
        queued.reject(reason);
      }
    };

    const queue = yield* Queue.unbounded<HostWork>();

    const deliverHostOutcome = (
      queued: QueuedHostCall,
      outcome: CodeHostCallResult,
    ): Effect.Effect<void, CodeExecutionProtocolError | CodeOutputLimitError> =>
      Effect.gen(function* () {
        const decoded = decodeHostCallResult(outcome);

        if (Option.isNone(decoded)) {
          const error = CodeExecutionProtocolError.make({
            implementation: dynamicWorkerImplementation,
            message: "The execution host returned a value outside the CodeHostCallResult schema",
          });

          failPass(error);

          return yield* error;
        }
        const encoded = encodeHostResultPayload(decoded.value);

        if (encoded === undefined || encoded.resultBytes > request.limits.maxHostCallResultBytes) {
          const error = CodeOutputLimitError.make({
            implementation: dynamicWorkerImplementation,
            surface: "host-call-result",
            limit: request.limits.maxHostCallResultBytes,
            observed: encoded?.resultBytes ?? 0,
            logs: [],
          });

          failPass(error);

          return yield* error;
        }
        const normalizedPayload = decodeJsonPayload(encoded.encodedPayload);

        if (Option.isNone(normalizedPayload)) {
          const error = CodeExecutionProtocolError.make({
            implementation: dynamicWorkerImplementation,
            message: "The execution host returned a result that could not cross the JSON boundary",
          });

          failPass(error);

          return yield* error;
        }
        queued.resolve(
          decoded.value._tag === "CodeHostCallSuccess"
            ? { _tag: "CodeHostCallSuccess", value: normalizedPayload.value }
            : { _tag: "CodeHostCallFailure", error: normalizedPayload.value },
        );
      });

    // Workers RPC into the loader isolate cannot settle on the fiber blocked
    // in `entrypoint.run()`. A Scope-owned sibling fiber keeps that
    // independence while inheriting the pass Context and dying with the Scope.
    const serveHostCalls = Effect.gen(function* () {
      while (true) {
        const work = yield* Queue.take(queue);

        if (work._tag === "limit") {
          const error = CodeHostCallLimitError.make({
            implementation: dynamicWorkerImplementation,
            limit: request.limits.maxHostCalls,
            logs: [],
          });

          failPass(error);

          return yield* error;
        }
        const queued = work.queued;

        yield* host.call(queued.call).pipe(
          Effect.timeoutOrElse({
            duration: remainingPassWallTime(),
            orElse: () => {
              const error = CodeExecutionTimeoutError.make({
                implementation: dynamicWorkerImplementation,
                kind: "wall-clock",
                maxWallTime: request.limits.maxWallTime,
                logs: [],
              });

              failPass(error);

              return error;
            },
          }),
          Effect.flatMap((outcome) => deliverHostOutcome(queued, outcome)),
          Effect.tapError(() =>
            Effect.sync(() => queued.reject(new Error("Code Mode host call failed"))),
          ),
          Effect.onInterrupt(() =>
            Effect.sync(() => queued.reject(new Error("Code Mode pass is closing"))),
          ),
        );
      }
    });

    const server = yield* serveHostCalls.pipe(Effect.forkScoped);

    const dispatch = (hostCall: unknown): Promise<unknown> => {
      if (!passOpen) {
        return Promise.reject(new Error("Code Mode pass is closing"));
      }
      issuedHostCalls += 1;
      if (issuedHostCalls > request.limits.maxHostCalls) {
        const error = CodeHostCallLimitError.make({
          implementation: dynamicWorkerImplementation,
          limit: request.limits.maxHostCalls,
          logs: [],
        });

        failPass(error);
        Queue.offerUnsafe(queue, { _tag: "limit" });

        return Promise.reject(new Error("host-call limit exceeded"));
      }
      const decoded = decodeHostCall(hostCall);

      if (Option.isNone(decoded)) {
        return Promise.reject(new TypeError("host calls must match the CodeHostCall schema"));
      }

      return new Promise((resolve, reject) => {
        const queued = { call: decoded.value, resolve, reject };

        queuedHostCalls.push(queued);
        Queue.offerUnsafe(queue, { _tag: "call", queued });
      });
    };

    const closeAdmission = Effect.sync(() => {
      passOpen = false;
      rejectQueuedHostCalls(new Error("Code Mode pass is closing"));
    });

    yield* Effect.addFinalizer(() => closeAdmission.pipe(Effect.andThen(Fiber.interrupt(server))));

    // No `allowExperimental`: the runtime only accepts it when the CALLING
    // worker carries the `experimental` compatibility flag, which deployed
    // consumers cannot set — the option would reject every pass in
    // production. The harness needs no experimental runtime features.
    const workerCode: WorkerLoaderWorkerCode = {
      compatibilityDate: options.compatibilityDate ?? "2025-05-01",
      mainModule: "harness.js",
      modules: {
        "harness.js": HARNESS_MODULE,
        "program.js": `export default (\n${request.source}\n);`,
      },
      env: {
        CODE_MODE_PASS: encodeHarnessPassConfig({
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
        try: () => options.loader.load(workerCode),
        catch: (cause) => {
          const text = safeCauseMessage(cause, "The Worker Loader failed without a diagnostic");

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
      disposeRpcHandle,
    );

    const entrypoint = yield* Effect.acquireRelease(
      Effect.try({
        try: () => worker.getEntrypoint<CodeModeHarnessEntrypoint>(),
        catch: (cause) => classifyWorkerFailure(cause, request.limits.maxWallTime),
      }),
      disposeRpcHandle,
    );

    const rpc = Effect.tryPromise({
      try: () => entrypoint.run(new CodeModePassHostTarget(dispatch)),
      catch: (cause) => classifyWorkerFailure(cause, request.limits.maxWallTime),
    });

    const exit = yield* Effect.raceFirst(
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
      Fiber.join(server),
    ).pipe(Effect.exit);

    yield* closeAdmission;
    yield* Fiber.interrupt(server);
    if (passFailure !== undefined) {
      return yield* passFailure;
    }
    if (Exit.isFailure(exit)) {
      return yield* Effect.failCause(exit.cause);
    }
    const raw = exit.value;
    const finishedAt = clock.monotonicTimeNanosUnsafe();

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
            wallTime: Duration.nanos(finishedAt > startedAt ? finishedAt - startedAt : 0n),
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
  const text = safeCauseDiagnostic(cause, "[unserializable worker failure]");

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
  Layer.effect(
    CodeExecutor,
    Effect.gen(function* () {
      const clock = yield* Clock.Clock;

      return CodeExecutor.of({ execute: makeExecute(options, clock) });
    }),
  );

/** Assemble Code Mode handlers with the isolated Dynamic Worker executor. */
export const CloudflareCodeMode = {
  /**
   * Provide the selected tool handlers at construction, where Code Mode captures them.
   * Their errors and remaining dependencies stay visible. The definition still owns the
   * allowlist and limits; the runtime supplies the live Tool broker for each scoped pass.
   */
  layer: <A, E, R, Handlers, HandlerError, HandlerRequirements>(
    definition: { readonly handlers: Layer.Layer<A, E, R> },
    options: DynamicWorkerCodeExecutorOptions & {
      readonly handlers: Layer.Layer<Handlers, HandlerError, HandlerRequirements>;
    },
  ) =>
    definition.handlers.pipe(
      Layer.provide(options.handlers),
      Layer.provide(dynamicWorkerCodeExecutorLayer(options)),
    ),
};
