import {
  CodeExecutionHost,
  CodeExecutionRequest,
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
} from "@effect-agent/sandbox";
import { WorkerEntrypoint } from "cloudflare:workers";
import { Clock, Duration, Effect, Fiber, Layer, Option, Schema } from "effect";

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

/**
 * Project a host outcome to the plain JSON envelope the harness reads. A
 * `CodeExecutionHost` may return either real `CodeHostCallResult` instances
 * (the substitute and conformance kit) or plain-object equivalents (the Code
 * Mode capability's broker route), so this reads the shared fields rather than
 * `Schema.encodeSync`, which would reject a plain object.
 */
const hostResultEnvelope = (outcome: CodeHostCallResult): Record<string, unknown> =>
  outcome._tag === "CodeHostCallSuccess"
    ? { _tag: "CodeHostCallSuccess", value: outcome.value }
    : { _tag: "CodeHostCallFailure", error: outcome.error };

const utf8ByteLength = (value: string): number => {
  let total = 0;
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }
  return total;
};

const encodedJsonByteLength = (value: unknown): number | undefined => {
  try {
    const encoded = JSON.stringify(value);
    return encoded === undefined ? undefined : utf8ByteLength(encoded);
  } catch {
    return undefined;
  }
};

interface PendingHostCall {
  readonly hostCall: unknown;
  readonly resolve: (value: unknown) => void;
  readonly reject: (reason: unknown) => void;
}

/** Reserved global names the harness owns inside the dynamic worker. */
const reservedHarnessGlobals = new Set(["console"]);

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

    // Promise-side host calls bridge into the Effect world through a pending
    // list served by a scoped fiber, exactly like the deterministic
    // substitute: interruption reaches in-flight host calls, and pass-fatal
    // conditions fail the pass by failing the server.
    const pending: Array<PendingHostCall> = [];
    let wake: (() => void) | undefined;
    let issuedHostCalls = 0;
    const dispatch = (hostCall: unknown): Promise<unknown> =>
      new Promise((resolve, reject) => {
        issuedHostCalls += 1;
        if (issuedHostCalls > request.limits.maxHostCalls + 1) {
          reject(new Error("host-call limit exceeded"));
          return;
        }
        pending.push({ hostCall, resolve, reject });
        wake?.();
      });

    yield* Effect.acquireRelease(
      Effect.sync(() => {
        passRegistry.set(passId, { dispatch });
      }),
      () =>
        Effect.sync(() => {
          passRegistry.delete(passId);
        }),
    );

    const nextPending = Effect.suspend(() => {
      const item = pending.shift();
      if (item !== undefined) {
        return Effect.succeed(item);
      }
      return Effect.callback<PendingHostCall>((resume) => {
        wake = () => {
          wake = undefined;
          const next = pending.shift();
          if (next !== undefined) {
            resume(Effect.succeed(next));
          }
        };
      });
    });

    const serveHostCalls = Effect.gen(function* () {
      let served = 0;
      while (true) {
        const item = yield* nextPending;
        served += 1;
        if (served > request.limits.maxHostCalls) {
          return yield* CodeHostCallLimitError.make({
            implementation: dynamicWorkerImplementation,
            limit: request.limits.maxHostCalls,
            logs: [],
          });
        }
        const decoded = decodeHostCall(item.hostCall);
        if (Option.isNone(decoded)) {
          item.reject(new TypeError("host calls must match the CodeHostCall schema"));
          continue;
        }
        const outcome = yield* host.call(decoded.value);
        if (outcome._tag === "CodeHostCallSuccess") {
          const bytes = encodedJsonByteLength(outcome.value);
          if (bytes === undefined || bytes > request.limits.maxHostCallResultBytes) {
            return yield* CodeOutputLimitError.make({
              implementation: dynamicWorkerImplementation,
              surface: "host-call-result",
              limit: request.limits.maxHostCallResultBytes,
              observed: bytes ?? 0,
              logs: [],
            });
          }
        }
        item.resolve(hostResultEnvelope(outcome));
      }
    });

    const workerCode = {
      compatibilityDate: options.compatibilityDate ?? "2025-05-01",
      allowExperimental: true,
      mainModule: "harness.js",
      modules: {
        "harness.js": HARNESS_MODULE,
        "program.js": `export default (\n${request.source}\n);`,
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

    const startedAt = yield* Clock.currentTimeMillis;
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

    const server = yield* serveHostCalls.pipe(Effect.forkScoped);

    const rpc = Effect.tryPromise({
      try: async () => {
        const entrypoint = worker.getEntrypoint() as unknown as {
          run(): Promise<unknown>;
        };
        return await entrypoint.run();
      },
      catch: (cause) => classifyWorkerFailure(cause, request.limits.maxWallTime),
    });

    const raw = yield* Effect.raceFirst(rpc, Fiber.join(server)).pipe(
      Effect.timeoutOrElse({
        duration: request.limits.maxWallTime,
        orElse: () =>
          CodeExecutionTimeoutError.make({
            implementation: dynamicWorkerImplementation,
            kind: "wall-clock",
            maxWallTime: request.limits.maxWallTime,
            logs: [],
          }),
      }),
      Effect.ensuring(Fiber.interrupt(server)),
    );
    const finishedAt = yield* Clock.currentTimeMillis;

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
