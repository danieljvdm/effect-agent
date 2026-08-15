import {
  CodeExecutionHost,
  CodeExecutionLimits,
  CodeExecutionNamespace,
  CodeExecutionProtocolError,
  CodeExecutionRequest,
  CodeExecutionResourceUse,
  CodeExecutionResult,
  CodeExecutionTimeoutError,
  CodeExecutor,
  type CodeExecutorExecute,
  CodeExecutorUnsupportedError,
  CodeHostCall,
  CodeHostCallLimitError,
  CodeHostCallResult,
  CodeOutputLimitError,
  CodeProgramFailedError,
  CodeSourceError,
  SandboxImplementation,
} from "@effect-agent/sandbox";
import { Clock, Duration, Effect, Fiber, Layer, Option, Queue, Schema } from "effect";

/**
 * The deterministic in-process executor substitute (C1 of ADR-0017). It runs
 * the generated program on the host JavaScript engine with best-effort global
 * shadowing only, so it self-identifies as `unisolated` and is never a
 * security boundary (CAP-010, CAP-015). It exists to prove the public
 * `CodeExecutor` contract and to drive deterministic capability tests.
 */
export const inProcessCodeExecutorImplementation = SandboxImplementation.make({
  isolation: "unisolated",
  identity: "in-process-javascript",
});

// These two caps mirror the wire schema bounds (`BoundedLogs` is at most
// 4096 lines of at most 16 KiB each): capture must stay inside what
// `CodeExecutionResult` can carry. A line over the per-line cap is truncated
// with an explicit `…` marker; exceeding either the byte budget or the line
// cap fails the pass typed.
const MAX_LOG_LINES = 4_096;
const MAX_LOG_LINE_CHARACTERS = 16_000;
const MAX_THROWN_CHARACTERS = 4_000;

const utf8ByteLength = (value: string): number => new TextEncoder().encode(value).byteLength;

/**
 * Ambient globals shadowed inside the harness. Shadowing blocks the obvious
 * identifier paths only; a determined program can still escape, which is
 * exactly why this executor reports `unisolated` and the isolated network and
 * CPU enforcement conformance cases run only against isolated adapters.
 */
const shadowedGlobals = [
  "fetch",
  "process",
  "require",
  "module",
  "exports",
  "global",
  "globalThis",
  "XMLHttpRequest",
  "WebSocket",
  "Deno",
  "Bun",
] as const;

class LogLimitSignal {
  constructor(readonly observed: number) {}
}

class EvaluationThrew {
  constructor(readonly inner: unknown) {}
}

class NotAFunction {
  constructor(readonly actual: string) {}
}

interface LogCapture {
  readonly lines: Array<string>;
  bytes: number;
}

/**
 * Total, defect-free rendering of untrusted values: a hostile Proxy can throw
 * from property access, `toString`, and `Symbol.toPrimitive`, and an expected
 * program failure must never escape the typed channel as a defect while its
 * diagnostics are being serialized.
 */
const formatLogValue = (value: unknown): string => {
  try {
    if (typeof value === "string") {
      return value;
    }
    return JSON.stringify(value) ?? String(value);
  } catch {
    try {
      return String(value);
    } catch {
      return "[unprintable value]";
    }
  }
};

const makeConsole = (capture: LogCapture, limits: CodeExecutionLimits) => {
  const write = (...values: ReadonlyArray<unknown>): void => {
    const joined = values.map(formatLogValue).join(" ");
    const line =
      joined.length > MAX_LOG_LINE_CHARACTERS
        ? `${joined.slice(0, MAX_LOG_LINE_CHARACTERS - 1)}…`
        : joined;
    const bytes = utf8ByteLength(line);
    if (capture.lines.length >= MAX_LOG_LINES || capture.bytes + bytes > limits.maxLogBytes) {
      throw new LogLimitSignal(capture.bytes + bytes);
    }
    capture.lines.push(line);
    capture.bytes += bytes;
  };
  return { debug: write, error: write, info: write, log: write, warn: write };
};

interface PendingHostCall {
  readonly namespace: string;
  readonly method: string;
  readonly argument: unknown;
  readonly resolve: (value: unknown) => void;
  readonly reject: (reason: unknown) => void;
}

const buildNamespaceObject = (
  namespace: CodeExecutionNamespace,
  offer: (pending: PendingHostCall) => void,
): Record<string, unknown> => {
  const methods: Record<string, unknown> = {};
  for (const method of namespace.methods) {
    methods[method] = (argument: unknown) =>
      new Promise((resolve, reject) => {
        offer({ namespace: namespace.name, method, argument, resolve, reject });
      });
  }
  return methods;
};

const boundedText = (value: unknown): string => {
  try {
    const text = value instanceof Error ? `${value.name}: ${value.message}` : formatLogValue(value);
    return text.slice(0, MAX_THROWN_CHARACTERS);
  } catch {
    return "[unserializable thrown value]";
  }
};

/** Schema decoding of hostile values may itself throw through trap getters. */
const safeDecodeJson = (value: unknown): Option.Option<Schema.Json> => {
  try {
    return Schema.decodeUnknownOption(Schema.Json)(value);
  } catch {
    return Option.none();
  }
};

const boundedThrown = (value: unknown): Schema.Json => {
  const decoded = safeDecodeJson(value);
  if (Option.isSome(decoded)) {
    try {
      const encoded = JSON.stringify(decoded.value);
      if (encoded !== undefined && encoded.length <= MAX_THROWN_CHARACTERS) {
        return decoded.value;
      }
    } catch {
      // fall through to the bounded string form
    }
  }
  return boundedText(value);
};

const encodedJsonByteLength = (value: Schema.Json): number | undefined => {
  try {
    const encoded = JSON.stringify(value);
    return encoded === undefined ? undefined : utf8ByteLength(encoded);
  } catch {
    return undefined;
  }
};

/** Host outcomes are protocol input; a hostile value must not defect mid-decode. */
const decodeHostOutcome = (value: unknown): Option.Option<CodeHostCallResult> => {
  try {
    return Schema.decodeUnknownOption(CodeHostCallResult)(value);
  } catch {
    return Option.none();
  }
};

const validateRequest = (
  request: CodeExecutionRequest,
): Effect.Effect<void, CodeExecutorUnsupportedError | CodeSourceError> =>
  Effect.gen(function* () {
    if (request.network._tag !== "NetworkDisabled") {
      return yield* CodeExecutorUnsupportedError.make({
        implementation: inProcessCodeExecutorImplementation,
        feature: "network",
        message:
          "The unisolated in-process executor cannot enforce an egress allowlist; only NetworkDisabled is accepted, and even that is shadowed rather than enforced",
      });
    }
    if (request.limits.cpuMillis !== undefined) {
      return yield* CodeExecutorUnsupportedError.make({
        implementation: inProcessCodeExecutorImplementation,
        feature: "cpu-limit",
        message:
          "The unisolated in-process executor shares the host engine and cannot enforce a CPU limit",
      });
    }
    const reservedNames = new Set<string>([...shadowedGlobals, "console"]);
    const seen = new Set<string>();
    for (const namespace of request.namespaces) {
      if (reservedNames.has(namespace.name) || seen.has(namespace.name)) {
        return yield* CodeExecutorUnsupportedError.make({
          implementation: inProcessCodeExecutorImplementation,
          feature: "namespaces",
          message: `Namespace ${namespace.name} collides with a harness binding or another namespace`,
        });
      }
      seen.add(namespace.name);
    }
    const sourceBytes = utf8ByteLength(request.source);
    if (sourceBytes > request.limits.maxSourceBytes) {
      return yield* CodeSourceError.make({
        implementation: inProcessCodeExecutorImplementation,
        reason: "oversized",
        message: `Source is ${sourceBytes} bytes; the request allows ${request.limits.maxSourceBytes}`,
      });
    }
  });

const serveHostCalls = (
  host: CodeExecutionHost["Service"],
  queue: Queue.Queue<PendingHostCall>,
  limits: CodeExecutionLimits,
  capture: LogCapture,
  counter: { calls: number },
): Effect.Effect<
  never,
  CodeHostCallLimitError | CodeOutputLimitError | CodeExecutionProtocolError
> =>
  Effect.gen(function* () {
    while (true) {
      const pending = yield* Queue.take(queue);
      counter.calls += 1;
      if (counter.calls > limits.maxHostCalls) {
        return yield* CodeHostCallLimitError.make({
          implementation: inProcessCodeExecutorImplementation,
          limit: limits.maxHostCalls,
          logs: [...capture.lines],
        });
      }
      const argument = safeDecodeJson(pending.argument);
      if (Option.isNone(argument)) {
        pending.reject(new TypeError("host call arguments must be JSON values"));
        continue;
      }
      const argumentBytes = encodedJsonByteLength(argument.value);
      if (argumentBytes === undefined || argumentBytes > limits.maxHostCallArgumentBytes) {
        return yield* CodeOutputLimitError.make({
          implementation: inProcessCodeExecutorImplementation,
          surface: "host-call-argument",
          limit: limits.maxHostCallArgumentBytes,
          observed: argumentBytes ?? 0,
          logs: [...capture.lines],
        });
      }
      const rawOutcome = yield* host.call(
        CodeHostCall.make({
          namespace: pending.namespace,
          method: pending.method,
          argument: argument.value,
        }),
      );
      const outcome = decodeHostOutcome(rawOutcome);
      if (Option.isNone(outcome)) {
        return yield* CodeExecutionProtocolError.make({
          implementation: inProcessCodeExecutorImplementation,
          message: "The execution host returned a value outside the CodeHostCallResult schema",
        });
      }
      if (outcome.value._tag === "CodeHostCallFailure") {
        pending.reject(outcome.value.error);
        continue;
      }
      const resultBytes = encodedJsonByteLength(outcome.value.value);
      if (resultBytes === undefined || resultBytes > limits.maxHostCallResultBytes) {
        return yield* CodeOutputLimitError.make({
          implementation: inProcessCodeExecutorImplementation,
          surface: "host-call-result",
          limit: limits.maxHostCallResultBytes,
          observed: resultBytes ?? 0,
          logs: [...capture.lines],
        });
      }
      pending.resolve(outcome.value.value);
    }
  });

const classifyProgramFailure = (
  thrown: unknown,
  limits: CodeExecutionLimits,
  capture: LogCapture,
): CodeOutputLimitError | CodeSourceError | CodeProgramFailedError => {
  const inner = thrown instanceof EvaluationThrew ? thrown.inner : thrown;
  if (inner instanceof LogLimitSignal) {
    return CodeOutputLimitError.make({
      implementation: inProcessCodeExecutorImplementation,
      surface: "logs",
      limit: limits.maxLogBytes,
      observed: inner.observed,
      logs: [...capture.lines],
    });
  }
  if (inner instanceof NotAFunction) {
    return CodeSourceError.make({
      implementation: inProcessCodeExecutorImplementation,
      reason: "not-a-function",
      message: `The source expression evaluated to ${inner.actual}; it must evaluate to one async function`,
    });
  }
  // An async function converts a body-level `throw` into a rejection, so the
  // split is by value shape: exception-like values read as `threw`, plain
  // rejection values (an uncaught host failure envelope) read as `rejected`.
  const reason = thrown instanceof EvaluationThrew || inner instanceof Error ? "threw" : "rejected";
  return CodeProgramFailedError.make({
    implementation: inProcessCodeExecutorImplementation,
    reason,
    thrown: boundedThrown(inner),
    message: boundedText(inner),
    logs: [...capture.lines],
  });
};

const executeInProcess: CodeExecutorExecute = Effect.fn("InProcessCodeExecutor.execute")(
  function* (request) {
    yield* validateRequest(request);
    const host = yield* CodeExecutionHost;
    const capture: LogCapture = { lines: [], bytes: 0 };
    const counter = { calls: 0 };
    const queue = yield* Queue.unbounded<PendingHostCall>();

    const factory = yield* Effect.try({
      try: () =>
        new Function(
          ...shadowedGlobals,
          "console",
          ...request.namespaces.map((namespace) => namespace.name),
          `"use strict";\nreturn (\n${request.source}\n);`,
        ),
      catch: (cause) =>
        CodeSourceError.make({
          implementation: inProcessCodeExecutorImplementation,
          reason: "invalid",
          message: boundedText(cause),
        }),
    });

    const harnessConsole = makeConsole(capture, request.limits);
    // Admission is enforced at call creation, not only at the single-consumer
    // dequeue: a burst of unawaited calls can enqueue at most one entry past
    // the cap (the entry the server fails the pass on); everything beyond is
    // rejected synchronously, so the queue stays bounded against hostile
    // programs.
    let issuedHostCalls = 0;
    const namespaceObjects = request.namespaces.map((namespace) =>
      buildNamespaceObject(namespace, (pending) => {
        issuedHostCalls += 1;
        if (issuedHostCalls > request.limits.maxHostCalls + 1) {
          pending.reject(new Error(`host-call limit of ${request.limits.maxHostCalls} exceeded`));
          return;
        }
        Queue.offerUnsafe(queue, pending);
      }),
    );

    const server = yield* serveHostCalls(host, queue, request.limits, capture, counter).pipe(
      Effect.forkScoped,
    );

    const program = Effect.tryPromise({
      try: async () => {
        let candidate: unknown;
        try {
          candidate = factory(
            ...shadowedGlobals.map(() => undefined),
            harnessConsole,
            ...namespaceObjects,
          );
        } catch (cause) {
          throw new EvaluationThrew(cause);
        }
        if (typeof candidate !== "function") {
          throw new EvaluationThrew(new NotAFunction(typeof candidate));
        }
        let outcome: unknown;
        try {
          outcome = (candidate as () => unknown)();
        } catch (cause) {
          throw new EvaluationThrew(cause);
        }
        return await Promise.resolve(outcome);
      },
      catch: (thrown) => classifyProgramFailure(thrown, request.limits, capture),
    });

    const startedAt = yield* Clock.currentTimeMillis;
    // The wall-clock deadline interrupts only at asynchronous suspension
    // points: a synchronous runaway shares the host thread and cannot be
    // stopped in-process — exactly why the platform CPU enforcement cases
    // belong to isolated adapters only (testing spec §8.1). The server fiber
    // is interrupted when the pass settles so no host call outlives the
    // program that issued it.
    const returned = yield* Effect.raceFirst(program, Fiber.join(server)).pipe(
      Effect.timeoutOrElse({
        duration: request.limits.maxWallTime,
        orElse: () =>
          CodeExecutionTimeoutError.make({
            implementation: inProcessCodeExecutorImplementation,
            kind: "wall-clock",
            maxWallTime: request.limits.maxWallTime,
            logs: [...capture.lines],
          }),
      }),
      Effect.ensuring(Fiber.interrupt(server)),
    );
    const finishedAt = yield* Clock.currentTimeMillis;

    // An unawaited burst can outrun the server: the program may return before
    // the over-limit entry is dequeued, so the admission counter is the
    // authority — a pass that ISSUED more calls than the cap fails even when
    // its promise settled first.
    if (issuedHostCalls > request.limits.maxHostCalls) {
      return yield* CodeHostCallLimitError.make({
        implementation: inProcessCodeExecutorImplementation,
        limit: request.limits.maxHostCalls,
        logs: [...capture.lines],
      });
    }

    const value = yield* Schema.decodeUnknownEffect(Schema.Json)(returned).pipe(
      Effect.mapError(() =>
        CodeProgramFailedError.make({
          implementation: inProcessCodeExecutorImplementation,
          reason: "non-json-result",
          thrown: null,
          message: "The program must return a JSON value",
          logs: [...capture.lines],
        }),
      ),
    );
    const resultBytes = encodedJsonByteLength(value);
    if (resultBytes === undefined || resultBytes > request.limits.maxResultBytes) {
      return yield* CodeOutputLimitError.make({
        implementation: inProcessCodeExecutorImplementation,
        surface: "result",
        limit: request.limits.maxResultBytes,
        observed: resultBytes ?? 0,
        logs: [...capture.lines],
      });
    }

    return CodeExecutionResult.make({
      implementation: inProcessCodeExecutorImplementation,
      value,
      logs: [...capture.lines],
      resourceUse: CodeExecutionResourceUse.make({
        wallTime: Duration.millis(Math.max(0, finishedAt - startedAt)),
        hostCalls: counter.calls,
        logBytes: capture.bytes,
        resultBytes,
      }),
    });
  },
);

/**
 * Layer providing the unisolated in-process `CodeExecutor` substitute. The
 * per-pass `CodeExecutionHost` stays in the caller's requirement channel, the
 * same as every real adapter.
 */
export const inProcessCodeExecutorLayer: Layer.Layer<CodeExecutor> = Layer.succeed(CodeExecutor)(
  CodeExecutor.of({ execute: executeInProcess }),
);
