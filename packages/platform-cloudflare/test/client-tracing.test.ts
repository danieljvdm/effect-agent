import {
  AbortCommand,
  ApprovalDecisionCommand,
  CanonicalSequence,
  Receipt,
  UnknownResolutionCommand,
} from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Tracer } from "effect";
import { TestClock } from "effect/testing";

import {
  CloudflareConversationClient,
  conversationNamespaceLayer,
  decodeAwaitProgressRequest,
  decodeCancelProgressRequest,
  type ClientObserveFailure,
  type ConversationClientError,
  type ConversationObjectRpc,
  type HostFailure,
} from "../src/index.ts";
import { decodeConversationId, plannerDefinition, submitOptions } from "./fixtures.ts";

const binding = "TASK_ORCHESTRATORS";
const conversationId = decodeConversationId("private-conversation-not-a-span-name");
const receipt = Schema.decodeUnknownSync(Receipt)({
  receiptId: "receipt-tracing",
  submissionId: "submission-tracing",
  conversationId,
  queueSequence: 1,
});
const zeroSequence = Schema.decodeSync(CanonicalSequence)(0);
const protocolFailure = {
  _tag: "HostFailed",
  failure: { _tag: "HostProtocolError", message: "fixture host refusal" },
};
const observedPage = { _tag: "ObservedPage", records: [] };

const clientMethods = [
  "submitEncoded",
  "awaitSettlementEncoded",
  "awaitProgressEncoded",
  "cancelProgressEncoded",
  "observePage",
  "abortEncoded",
  "resolveApprovalEncoded",
  "resolveUnknownEncoded",
] as const satisfies ReadonlyArray<keyof ConversationObjectRpc>;
type ClientMethod = (typeof clientMethods)[number];
interface NativeCall {
  readonly method: ClientMethod;
  readonly args: ReadonlyArray<unknown>;
}

// A local native transport substitute records the actual argument list, including undefined.
const clientFixture = (
  invoke: (method: ClientMethod, args: ReadonlyArray<unknown>) => Promise<unknown>,
  options: { readonly rpcTracing?: boolean; readonly binding?: string } = {},
) => {
  const calls: Array<NativeCall> = [];
  const spans: Array<Tracer.NativeSpan> = [];
  const stub = Object.fromEntries(
    clientMethods.map((method) => [
      method,
      (...args: Array<unknown>) => {
        calls.push({ method, args });
        return invoke(method, args);
      },
    ]),
  );
  const service = options.binding ?? binding;
  const layer = CloudflareConversationClient.layer.pipe(
    Layer.provide([
      conversationNamespaceLayer(
        { [service]: { idFromName: (name: string) => name, get: () => stub } },
        service,
        options,
      ),
      BrowserCrypto.layer,
    ]),
  );
  const tracer = Tracer.make({
    span(spanOptions) {
      const span = new Tracer.NativeSpan(spanOptions);
      spans.push(span);
      return span;
    },
  });
  return { calls, spans, layer, tracer };
};

const spanContext = (span: Tracer.Span) => ({
  _tag: "effect-cf/RpcTraceContext/v1",
  traceId: span.traceId,
  spanId: span.spanId,
  sampled: span.sampled,
});

describe("DEPLOY-016 opt-in native Conversation RPC tracing", () => {
  it.effect.each([
    { label: "omitted", options: {}, sampled: true, tracerEnabled: true },
    { label: "disabled", options: { rpcTracing: false }, sampled: true, tracerEnabled: true },
    { label: "enabled", options: { rpcTracing: true }, sampled: true, tracerEnabled: true },
    { label: "unsampled", options: { rpcTracing: true }, sampled: false, tracerEnabled: true },
    {
      label: "no tracing span",
      options: { rpcTracing: true },
      sampled: true,
      tracerEnabled: false,
    },
  ])("preserves every host method's native contract with $label tracing", (scenario) => {
    const fixture = clientFixture(() => Promise.resolve(protocolFailure), scenario.options);
    return Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      const outer = yield* Effect.currentSpan;
      const requests: ReadonlyArray<Effect.Effect<unknown, HostFailure | ConversationClientError>> =
        [
          client.submit(
            { definition: plannerDefinition },
            {
              question: "private input https://capability.invalid/?token=secret",
              ref: "private-ref",
            },
            submitOptions(conversationId, "idempotency-tracing"),
          ),
          client.awaitSettlement(receipt),
          client.awaitProgress(conversationId, zeroSequence),
          client.readPage(conversationId),
          client.abort(
            conversationId,
            AbortCommand.make({
              submissionId: receipt.submissionId,
              author: "operator",
              reason: "stop",
            }),
          ),
          client.resolveApproval(
            conversationId,
            Schema.decodeUnknownSync(ApprovalDecisionCommand)({
              submissionId: receipt.submissionId,
              toolCallId: "tool-tracing",
              decision: "approved",
              resolver: "operator",
              reason: "approve",
            }),
          ),
          client.resolveUnknown(
            conversationId,
            Schema.decodeUnknownSync(UnknownResolutionCommand)({
              submissionId: receipt.submissionId,
              toolCallId: "tool-tracing",
              author: "operator",
              reason: "resolve",
              resolution: { _tag: "AbortSubmission" },
            }),
          ),
        ];
      for (const request of requests) {
        const exit = yield* request.pipe(Effect.exit);
        if (!Exit.isFailure(exit)) throw new Error("Expected the host refusal to remain a failure");
        expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject(
          protocolFailure.failure,
        );
      }
      expect(fixture.calls.map((call) => call.method)).toEqual([
        "submitEncoded",
        "awaitSettlementEncoded",
        "awaitProgressEncoded",
        "observePage",
        "abortEncoded",
        "resolveApprovalEncoded",
        "resolveUnknownEncoded",
      ]);
      const rpcSpans = fixture.spans.filter((span) => span.kind === "client");
      const propagate = scenario.options.rpcTracing === true && scenario.tracerEnabled;
      expect(rpcSpans).toHaveLength(propagate ? fixture.calls.length : 0);
      for (const [index, call] of fixture.calls.entries()) {
        expect(call.args).toHaveLength(propagate ? 2 : 1);
        expect(call.args[0]).not.toHaveProperty("traceId");
        expect(call.args[0]).not.toHaveProperty("spanId");
        if (!propagate) continue;
        const span = rpcSpans[index];
        if (span === undefined) throw new Error("Missing native client span");
        expect(span.name).toBe(`${binding}/${call.method}`);
        expect(Option.getOrUndefined(span.parent)?.spanId).toBe(outer.spanId);
        expect(span.traceId).toBe(outer.traceId);
        expect(span.spanId).not.toBe(outer.spanId);
        expect(span.sampled).toBe(scenario.sampled);
        expect(call.args[1]).toEqual(spanContext(span));
        expect(Object.fromEntries(span.attributes)).toEqual({
          "sentry.op": "rpc",
          "rpc.system.name": "cloudflare",
          "rpc.method": `${binding}/${call.method}`,
          "server.address": binding,
        });
      }
    }).pipe(
      Effect.withSpan("application-caller", { sampled: scenario.sampled }),
      Effect.provide(fixture.layer),
      Effect.provideService(Tracer.Tracer, fixture.tracer),
      Effect.withTracerEnabled(scenario.tracerEnabled),
    );
  });

  it.effect("keeps the client span open across native waiting and host-response decoding", () => {
    const started = Deferred.makeUnsafe<void>();
    const response = Deferred.makeUnsafe<unknown>();
    const fixture = clientFixture(
      () => {
        Deferred.doneUnsafe(started, Effect.void);
        return Effect.runPromise(Deferred.await(response));
      },
      { rpcTracing: true, binding: "PERSONA_ADVISORS" },
    );
    return Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      const fiber = yield* client.readPage(conversationId).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      const span = fixture.spans.find((candidate) => candidate.kind === "client");
      if (span === undefined) throw new Error("Missing waiting client span");
      expect(span.name).toBe("PERSONA_ADVISORS/observePage");
      expect(span.status._tag).toBe("Started");
      yield* TestClock.adjust("5 seconds");
      let decodedWhileOpen = false;
      yield* Deferred.succeed(response, {
        get _tag() {
          decodedWhileOpen = span.status._tag === "Started";
          return "ObservedPage";
        },
        records: [],
      });
      expect(yield* Fiber.join(fiber)).toEqual([]);
      expect(decodedWhileOpen).toBe(true);
      expect(span.status._tag).toBe("Ended");
      if (span.status._tag !== "Ended") throw new Error("Client span did not close");
      expect(span.status.endTime - span.startTime).toBe(5_000_000_000n);
      expect(Exit.isSuccess(span.status.exit)).toBe(true);
    }).pipe(
      Effect.provide(fixture.layer),
      Effect.provideService(Tracer.Tracer, fixture.tracer),
      Effect.withTracerEnabled(true),
      Effect.ensuring(Deferred.succeed(response, observedPage)),
    );
  });

  it.effect.each([
    { reason: "DisablePropagation", disabled: true, traceId: "1234567890abcdef1234567890abcdef" },
    { reason: "an invalid trace ID", disabled: false, traceId: "invalid-trace-id" },
  ])("omits native context for $reason without dropping the client span", (scenario) => {
    const fixture = clientFixture(() => Promise.resolve(observedPage), { rpcTracing: true });
    return Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      expect(yield* client.readPage(conversationId)).toEqual([]);
      expect(fixture.calls[0]?.args).toHaveLength(1);
      const spans = fixture.spans.filter((span) => span.kind === "client");
      expect(spans).toHaveLength(1);
      expect(spans[0]?.status._tag).toBe("Ended");
    }).pipe(
      Effect.provide(fixture.layer),
      Effect.provideService(Tracer.Tracer, fixture.tracer),
      Effect.provideService(Tracer.DisablePropagation, scenario.disabled),
      Effect.withParentSpan(
        Tracer.externalSpan({ traceId: scenario.traceId, spanId: "1234567890abcdef" }),
      ),
      Effect.withTracerEnabled(true),
    );
  });

  it.effect.each(["transport", "decode"] as const)(
    "keeps %s failures typed without exporting their payload in the client span",
    (failureMode) => {
      const secret = "private payload https://capability.invalid/?token=secret";
      const rejected = new Error(secret);
      const fixture = clientFixture(
        () =>
          failureMode === "transport"
            ? Promise.reject(rejected)
            : Promise.resolve({ _tag: "ObservedPage", records: [secret] }),
        { rpcTracing: true },
      );
      return Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        const failure: ClientObserveFailure = yield* client
          .readPage(conversationId)
          .pipe(Effect.flip);
        expect(failure._tag).toBe(
          failureMode === "transport" ? "ConversationClientError" : "HostProtocolError",
        );
        if (failure._tag === "ConversationClientError") expect(failure.cause).toBe(rejected);
        const span = fixture.spans.find((candidate) => candidate.kind === "client");
        if (span?.status._tag !== "Ended" || !Exit.isFailure(span.status.exit)) {
          throw new Error("Expected a closed, failed client span");
        }
        expect(Option.getOrUndefined(Cause.findErrorOption(span.status.exit.cause))).toBe(
          "RPC failed",
        );
        expect(Cause.pretty(span.status.exit.cause)).not.toContain(secret);
        expect(span.attributes.get("error.type")).toBe("_OTHER");
      }).pipe(
        Effect.provide(fixture.layer),
        Effect.provideService(Tracer.Tracer, fixture.tracer),
        Effect.withTracerEnabled(true),
      );
    },
  );

  it.effect.each([
    { rpcTracing: false, timeout: false },
    { rpcTracing: true, timeout: false },
    { rpcTracing: true, timeout: true },
  ])("closes progress waits and preserves cancellation arity %#", (options) => {
    const started = Deferred.makeUnsafe<void>();
    const response = Deferred.makeUnsafe<unknown>();
    const fixture = clientFixture((method) => {
      if (method === "awaitProgressEncoded") {
        Deferred.doneUnsafe(started, Effect.void);
        return Effect.runPromise(Deferred.await(response));
      }
      Deferred.doneUnsafe(response, Effect.succeed({ _tag: "ProgressObserved" }));
      return Promise.resolve({ _tag: "ProgressCancelled" });
    }, options);
    return Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      const wait = client.awaitProgress(conversationId, zeroSequence);
      const fiber = yield* (options.timeout ? wait.pipe(Effect.timeout("1 second")) : wait).pipe(
        Effect.forkChild,
      );
      yield* Deferred.await(started);
      if (options.timeout) yield* TestClock.adjust("1 second");
      else yield* Fiber.interrupt(fiber);
      expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
      expect(fixture.calls.map((call) => call.method)).toEqual([
        "awaitProgressEncoded",
        "cancelProgressEncoded",
      ]);
      expect(fixture.calls.map((call) => call.args.length)).toEqual(
        options.rpcTracing ? [2, 2] : [1, 1],
      );
      const awaited = yield* decodeAwaitProgressRequest(fixture.calls[0]?.args[0]);
      const cancelled = yield* decodeCancelProgressRequest(fixture.calls[1]?.args[0]);
      expect(cancelled.waiterId).toBe(awaited.waiterId);
      const rpcSpans = fixture.spans.filter((span) => span.kind === "client");
      expect(rpcSpans).toHaveLength(options.rpcTracing ? 2 : 0);
      for (const [index, span] of rpcSpans.entries()) {
        expect(span.status._tag).toBe("Ended");
        expect(fixture.calls[index]?.args[1]).toEqual(spanContext(span));
      }
    }).pipe(
      Effect.provide(fixture.layer),
      Effect.provideService(Tracer.Tracer, fixture.tracer),
      Effect.withTracerEnabled(true),
      Effect.ensuring(Deferred.succeed(response, { _tag: "ProgressObserved" })),
    );
  });

  it.effect("creates a fresh native context for a reset retry without changing its request", () => {
    const started = Deferred.makeUnsafe<void>();
    let attempts = 0;
    const fixture = clientFixture(
      () => {
        if (attempts++ === 0) {
          Deferred.doneUnsafe(started, Effect.void);
          throw Object.assign(new Error("fixture Object reset"), { retryable: true });
        }
        return Promise.resolve({ _tag: "ProgressObserved" });
      },
      { rpcTracing: true },
    );
    return Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      const fiber = yield* client
        .awaitProgress(conversationId, zeroSequence)
        .pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* TestClock.adjust("10 millis");
      yield* Fiber.join(fiber);
      expect(fixture.calls).toHaveLength(2);
      expect(fixture.calls[0]?.args[0]).toEqual(fixture.calls[1]?.args[0]);
      const spans = fixture.spans.filter((span) => span.kind === "client");
      expect(spans).toHaveLength(2);
      expect(spans[0]?.spanId).not.toBe(spans[1]?.spanId);
      expect(spans[0]?.traceId).toBe(spans[1]?.traceId);
      for (const [index, span] of spans.entries()) {
        expect(fixture.calls[index]?.args).toHaveLength(2);
        expect(fixture.calls[index]?.args[1]).toEqual(spanContext(span));
        expect(span.status._tag).toBe("Ended");
      }
    }).pipe(
      Effect.withSpan("application-caller"),
      Effect.provide(fixture.layer),
      Effect.provideService(Tracer.Tracer, fixture.tracer),
      Effect.withTracerEnabled(true),
    );
  });
});
