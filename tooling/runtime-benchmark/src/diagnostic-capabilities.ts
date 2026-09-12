import {
  ScriptedModel,
  type ScriptedStreamPart,
  type ScriptedTurnInput,
} from "@effect-agent/testing/ScriptedModel";
import {
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Schema,
  Scope,
  Stream,
  Tracer,
} from "effect";
import { Agent, AgentRuntime } from "effect-agent";
import * as EphemeralThreads from "effect-agent/EphemeralThreads";
import { RunId, ThreadId } from "effect-agent/Identifiers";
import { IdGenerator } from "effect-agent/IdGenerator";
import * as Mcp from "effect-agent/Mcp";
import * as McpClient from "effect-agent/McpClient";
import * as Memory from "effect-agent/Memory";
import * as MemoryNamespace from "effect-agent/MemoryNamespace";
import {
  MemoryAttribution,
  MemoryContent,
  MemoryLookup,
  MemoryPassage,
  MemoryRecallLimits,
} from "effect-agent/MemoryReference";
import {
  applyMemoryWrite,
  MemoryKey,
  MemoryReader,
  MemoryScope,
  MemoryWriter,
  type MemoryDocument,
} from "effect-agent/MemoryStore";
import * as Remembering from "effect-agent/Remembering";
import * as Protocol from "effect-agent/RememberingStore";
import { toRunThreadOptions } from "effect-agent/RunHooks";
import * as Subagent from "effect-agent/Subagent";
import * as Reservations from "effect-agent/SubagentReservations";
import { ThreadHistory } from "effect-agent/ThreadHistory";
import { AiError, Model, Prompt, Tool, Toolkit } from "effect/unstable/ai";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { BenchmarkError, check } from "./contracts.js";
import { capabilityCases } from "./diagnostic-cases.js";
import {
  DiagnosticCase,
  DiagnosticProgress,
  type DiagnosticMark,
  type DiagnosticResult,
} from "./diagnostic-contracts.js";

export { capabilityCases } from "./diagnostic-cases.js";

/** Faults are test-only fixture inputs; the public diagnostic inventory always uses "none". */
export const DiagnosticFault = Context.Reference("runtime-benchmark/DiagnosticFault", {
  defaultValue: (): "none" | "mcp-malformed-discovery" | "mcp-call-failure" => "none",
});

/** Every mark uses elapsed monotonic wall time from this sample's operation start. */
const elapsed = (start: bigint) =>
  Clock.monotonicTimeNanos.pipe(Effect.map((now) => Number(now - start) / 1_000_000));

const providerCheck = <A, E>(effect: Effect.Effect<A, E>) =>
  effect.pipe(
    Effect.mapError((error) =>
      AiError.AiError.make({
        module: "capability-diagnostic",
        method: "assertRequest",
        reason: AiError.UnknownError.make({ description: String(error) }),
      }),
    ),
  );

const finalParts: ReadonlyArray<ScriptedStreamPart> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const work = Tool.make("diagnostic_work", {
  parameters: Schema.Struct({ index: Schema.Int }),
  success: Schema.Int,
});

const toolkit = Toolkit.make(work);

const definition = Agent.make("capability-diagnostic", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Return the requested answer.",
  toolkit,
  policy: { maxTurns: 4, maxToolCalls: 4, maxDuration: "10 seconds", toolConcurrency: 2 },
});

const historyPrompt = (count: number) =>
  Prompt.make(
    Array.from({ length: count }, (_, index) => ({
      role: index % 2 === 0 ? ("user" as const) : ("assistant" as const),
      content: `${index}: ${"retained text ".repeat(20)}`,
    })),
  );

const encodedPrompt = Schema.encodeEffect(Schema.fromJsonString(Prompt.Prompt));

const historyCase = Effect.fn("diagnostic.history")(function* (workload: DiagnosticCase) {
  const progress = yield* DiagnosticProgress;

  yield* progress.phase("setup");
  const threads = yield* EphemeralThreads.EphemeralThreads;
  const threadId = ThreadId.make("diagnostic-thread-0");
  const seedRun = RunId.make("diagnostic-seed");
  const runId = RunId.make("diagnostic-operation");
  const prefix = workload.parameters.prefix ?? 256;
  const suffix = workload.parameters.suffix ?? 0;
  const threadCount = workload.parameters.threads ?? 1;
  const original = historyPrompt(prefix);

  for (let index = 0; index < threadCount; index++) {
    const id = ThreadId.make(`diagnostic-thread-${index}`);

    yield* threads.create(id);
    yield* threads.recordHistory(id, seedRun, original);
  }
  // Fresh by-value prefix: the measured API must not depend on object identity.
  const incoming = historyPrompt(prefix + suffix);
  const expected = yield* encodedPrompt(incoming);

  yield* progress.phase("operation");
  const start = yield* Clock.monotonicTimeNanos;
  const snapshot = yield* threads.recordHistory(threadId, runId, incoming);
  const totalMs = yield* elapsed(start);

  yield* progress.phase("verification");
  yield* check(snapshot.nextSequence === prefix + suffix, "History sequence mismatch");
  yield* check(
    (yield* encodedPrompt(EphemeralThreads.threadPrompt(snapshot))) === expected,
    "History native messages changed",
  );
  yield* check(
    snapshot.messages.every(
      (entry, index) =>
        entry.sequence === index && entry.runId === (index < prefix ? seedRun : runId),
    ),
    "History changed prefix ownership or suffix run IDs",
  );
  yield* check(
    snapshot.contentBytes ===
      snapshot.messages.reduce((total, entry) => total + entry.encodedBytes, 0),
    "History byte accounting mismatch",
  );
  for (let index = 1; index < threadCount; index++) {
    yield* check(
      (yield* threads.snapshot(ThreadId.make(`diagnostic-thread-${index}`))).nextSequence ===
        prefix,
      "History mutated another thread",
    );
  }

  return {
    totalMs,
    metrics: [{ name: "recordHistory", value: totalMs }],
    counters: [
      { name: "prefixMessages", value: prefix },
      { name: "suffixMessages", value: suffix },
      { name: "threads", value: threadCount },
      { name: "committedMessages", value: snapshot.nextSequence },
      { name: "contentBytes", value: snapshot.contentBytes },
    ],
  };
});

const historyRunCase = Effect.fn("diagnostic.historyRun")(function* (workload: DiagnosticCase) {
  const progress = yield* DiagnosticProgress;

  yield* progress.phase("setup");
  const threads = yield* EphemeralThreads.EphemeralThreads;
  const threadId = ThreadId.make("diagnostic-run-thread");
  const runId = RunId.make("diagnostic-run");
  const prefix = historyPrompt(workload.parameters.prefix ?? 256);

  const expectedPrefix = yield* Effect.forEach(prefix.content, (message) =>
    Schema.encodeEffect(Schema.fromJsonString(Prompt.Message))(message),
  );

  yield* threads.create(threadId);
  yield* threads.recordHistory(threadId, RunId.make("seed"), prefix);
  const enabled = workload.parameters.enabled === 1;

  const options = enabled
    ? yield* toRunThreadOptions(threadId, runId)
    : { threadId, history: prefix };

  let calls = 0;
  let finalized = 0;
  let tools = 0;
  let start = 0n;
  const entries: Array<number> = [];

  const turns: Array<ScriptedTurnInput> = [0, 1].map((turn) => ({
    _tag: "Stream",
    termination: { _tag: "Complete" },
    assertRequest: (request) =>
      providerCheck(
        Effect.gen(function* () {
          calls++;
          const at = yield* elapsed(start);

          entries.push(at);
          yield* progress.mark({ name: `provider.${turn}`, elapsedMs: at });

          const messages = yield* Effect.forEach(request.prompt.content, (message) =>
            Schema.encodeEffect(Schema.fromJsonString(Prompt.Message))(message),
          );

          // Engine instructions can prepend a system message. Match the exact ordered native prefix.
          const prefixStart = messages.indexOf(expectedPrefix[0] ?? "");

          yield* check(
            prefixStart >= 0 &&
              expectedPrefix.every((message, index) => messages[prefixStart + index] === message),
            "Run lost or changed exact initial history",
          );
          if (turn === 1) {
            const results = request.prompt.content
              .filter((message) => message.role === "tool")
              .flatMap((message) => message.content)
              .filter((part) => part.type === "tool-result");

            yield* check(
              tools === 1 &&
                results.length === 1 &&
                results[0]?.id === "work-0" &&
                results[0]?.result === 7,
              "Second provider lost the exact tool result",
            );
          }
        }),
      ),
    parts:
      turn === 0
        ? [
            {
              type: "tool-call",
              id: "work-0",
              name: "diagnostic_work",
              params: { index: 7 },
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
          ]
        : finalParts,
    onStreamFinalize: Effect.sync(() => {
      finalized++;
    }),
  }));

  const model = Layer.mergeAll(
    ScriptedModel.layer(turns),
    Layer.succeed(Model.ProviderName, "scripted"),
    Layer.succeed(Model.ModelName, "capability-diagnostic"),
  );

  yield* progress.phase("operation");
  start = yield* Clock.monotonicTimeNanos;

  const output = yield* Effect.scoped(
    Effect.gen(function* () {
      const result = yield* AgentRuntime.run(definition, "Answer", { ...options, runId });

      yield* (yield* ScriptedModel).assertExhausted;

      return result;
    }),
  ).pipe(
    Effect.provide(
      Layer.mergeAll(
        model,
        toolkit.toLayer({
          diagnostic_work: ({ index }) =>
            Effect.sync(() => {
              tools++;

              return index;
            }),
        }),
        IdGenerator.layer,
        ThreadHistory.layerTransient,
      ),
    ),
  );

  const totalMs = yield* elapsed(start);

  yield* progress.phase("verification");
  const snapshot = yield* threads.snapshot(threadId);

  yield* check(
    output.output.answer === "done" && calls === 2 && finalized === 2 && tools === 1,
    "History Run work/finalizers mismatch",
  );
  yield* check(
    enabled
      ? snapshot.nextSequence > prefix.content.length
      : snapshot.nextSequence === prefix.content.length,
    "History hook retention mismatch",
  );
  yield* check(
    (yield* encodedPrompt(
      Prompt.fromMessages(
        snapshot.messages.slice(0, prefix.content.length).map((entry) => entry.message),
      ),
    )) === (yield* encodedPrompt(prefix)),
    "History Run changed the committed prefix",
  );

  return {
    totalMs,
    metrics: entries.map((value, index) => ({ name: `providerEntry.${index}`, value })),
    counters: [
      { name: "modelCalls", value: calls },
      { name: "modelFinalizers", value: finalized },
      { name: "toolCalls", value: tools },
      { name: "retainedMessages", value: snapshot.nextSequence },
    ],
  };
});

const memoryContent = MemoryContent.make({
  text: "The diagnostic preference is a concise answer.",
  attributions: [
    MemoryAttribution.make({
      originId: "diagnostic-source",
      speaker: "Fixture",
      observers: [],
      locator: "fixture://message/1",
      activityAt: 10,
      interpretation: "reported preference",
    }),
  ],
  metadata: { topic: "diagnostic" },
  recordedAt: 20,
  extractedAt: 30,
});

const memoryLookup = MemoryLookup.make({
  _tag: "Found",
  passages: [
    MemoryPassage.make({
      version: 1,
      source: { id: "diagnostic", locator: "fixture://message/1", revision: "r1" },
      passageId: "document",
      content: memoryContent,
    }),
  ],
});

const recallLimits = MemoryRecallLimits.make({
  maxSources: 1,
  maxItems: 1,
  maxBytes: 16_384,
  maxTokens: 16_384,
  timeoutMillis: 1_000,
});

const memoryRunCase = Effect.fn("diagnostic.memoryRun")(function* (workload: DiagnosticCase) {
  const progress = yield* DiagnosticProgress;
  const fs = yield* FileSystem.FileSystem;

  yield* progress.phase("setup");
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "capability-memory-" });
  const path = `${directory}/passage.json`;

  yield* fs.writeFileString(
    path,
    yield* Schema.encodeEffect(Schema.fromJsonString(MemoryLookup))(memoryLookup),
  );
  const mode = workload.parameters.mode ?? 0;
  let start = 0n;
  let readers = 0;
  let readerFinalizers = 0;
  let loads = 0;
  let models = 0;
  let modelFinalizers = 0;
  let readerIoMs = 0;
  let recallMs = 0;
  let providerMs = 0;

  const read = Effect.gen(function* () {
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        readers++;
      }),
      () =>
        Effect.gen(function* () {
          readerFinalizers++;
          yield* progress
            .mark({ name: "memory.reader.finalized", elapsedMs: yield* elapsed(start) })
            .pipe(Effect.orDie);
        }),
    );
    const ioStart = yield* Clock.monotonicTimeNanos;
    const json = yield* fs.readFileString(path);

    readerIoMs += yield* elapsed(ioStart);
    yield* progress.mark({ name: "memory.readerIO.complete", elapsedMs: yield* elapsed(start) });

    return yield* Schema.decodeEffect(Schema.fromJsonString(MemoryLookup))(json);
  });

  const load = Effect.gen(function* () {
    loads++;
    if (mode === 1) return "";
    const recallStart = yield* Clock.monotonicTimeNanos;

    const recalled = yield* Memory.recall(
      [{ id: "diagnostic", essential: true, read }],
      recallLimits,
    );

    recallMs = yield* elapsed(recallStart);
    yield* check(recalled.passages.length === 1, "Memory recall lost its passage");

    return recalled.text;
  });

  const model = Layer.mergeAll(
    ScriptedModel.layer([
      {
        _tag: "Stream",
        termination: { _tag: "Complete" },
        parts: finalParts,
        assertRequest: (request) =>
          providerCheck(
            Effect.gen(function* () {
              models++;
              providerMs = yield* elapsed(start);
              yield* progress.mark({ name: "provider.0", elapsedMs: providerMs });
              const encoded = yield* encodedPrompt(request.prompt);

              yield* check(
                encoded.includes(memoryContent.text) === (mode === 2),
                "Memory prompt content mismatch",
              );
            }),
          ),
        onStreamFinalize: Effect.sync(() => {
          modelFinalizers++;
        }),
      },
    ]),
    Layer.succeed(Model.ProviderName, "scripted"),
    Layer.succeed(Model.ModelName, "capability-diagnostic"),
  );

  yield* progress.phase("operation");
  start = yield* Clock.monotonicTimeNanos;

  const output = yield* Effect.scoped(
    Effect.gen(function* () {
      const result = yield* AgentRuntime.run(
        definition,
        "Answer",
        mode === 0 ? {} : { transientContext: { load: () => load } },
      );

      yield* (yield* ScriptedModel).assertExhausted;

      return result;
    }),
  ).pipe(
    Effect.provide(
      Layer.mergeAll(
        model,
        toolkit.toLayer({ diagnostic_work: ({ index }) => Effect.succeed(index) }),
        IdGenerator.layer,
        ThreadHistory.layerTransient,
      ),
    ),
  );

  const totalMs = yield* elapsed(start);

  yield* progress.phase("verification");
  yield* check(
    output.output.answer === "done" && models === 1 && modelFinalizers === 1,
    "Memory Run work/finalizers mismatch",
  );
  yield* check(
    readers === (mode === 2 ? 1 : 0) &&
      readerFinalizers === readers &&
      loads === (mode === 0 ? 0 : 1),
    "Memory optional work or reader finalization mismatch",
  );

  return {
    totalMs,
    metrics: [
      { name: "providerEntry", value: providerMs },
      { name: "recall", value: recallMs },
      { name: "readerIO", value: readerIoMs },
    ],
    counters: [
      { name: "modelCalls", value: models },
      { name: "modelFinalizers", value: modelFinalizers },
      { name: "contextLoads", value: loads },
      { name: "readerCalls", value: readers },
      { name: "readerFinalizers", value: readerFinalizers },
    ],
  };
});

const rpcRequest = Schema.fromJsonString(
  Schema.Struct({
    id: Schema.optionalKey(Schema.Union([Schema.String, Schema.Finite])),
    method: Schema.String,
    params: Schema.optionalKey(Schema.Unknown),
  }),
);

const rpcResponse = Schema.fromJsonString(
  Schema.Struct({
    jsonrpc: Schema.Literal("2.0"),
    id: Schema.Union([Schema.String, Schema.Finite]),
    result: Schema.Json,
  }),
);

/** Real public MCP HTTP transport and protocol, with a bounded in-process HTTP responder.
 * This measures neither network latency nor a remote server. Each connection owns one session.
 */
const mcpCase = Effect.fn("diagnostic.mcp")(function* (workload: DiagnosticCase) {
  const progress = yield* DiagnosticProgress;
  const fault = yield* DiagnosticFault;

  yield* progress.phase("setup");
  const enabled = workload.parameters.enabled === 1;
  let start = 0n;
  let connects = 0;
  let discoveries = 0;
  let calls = 0;
  let closed = 0;
  let modelCalls = 0;
  let modelFinalizers = 0;
  let localCalls = 0;
  let activeSession: string | undefined;
  let activeCredential: string | undefined;
  const metrics: Array<{ name: string; value: number }> = [];

  const client = HttpClient.make((request) =>
    Effect.gen(function* () {
      if (request.method === "DELETE") {
        yield* check(
          activeSession !== undefined && request.headers["mcp-session-id"] === activeSession,
          "MCP closed the wrong session",
        );
        yield* check(
          request.headers["authorization"] === activeCredential,
          "MCP finalizer reused stale credentials",
        );
        closed++;
        activeSession = undefined;
        activeCredential = undefined;
        yield* progress.mark({ name: "mcp.session.closed", elapsedMs: yield* elapsed(start) });

        return HttpClientResponse.fromWeb(request, new globalThis.Response(null, { status: 200 }));
      }
      const web = yield* HttpClientRequest.toWeb(request);

      const body = yield* Effect.tryPromise({
        try: () => web.text(),
        catch: (cause) =>
          BenchmarkError.make({ message: "MCP fixture request body failed", cause }),
      });

      yield* check(body.length <= 16_384, "MCP fixture request exceeded its fixed bound");
      const message = yield* Schema.decodeEffect(rpcRequest)(body);
      let result: Schema.Json = {};

      if (message.method === "initialize") {
        yield* check(activeSession === undefined, "MCP overlapped owned sessions");
        connects++;
        activeSession = `session-${connects}`;
        activeCredential = `fixture-credential-${connects}`;
        result = {
          protocolVersion: "2025-06-18",
          capabilities: { tools: {} },
          serverInfo: { name: "in-process-http-diagnostic", version: "1" },
        };
      }
      yield* check(
        request.headers["authorization"] === activeCredential,
        "MCP reused stale credentials",
      );
      if (message.method !== "initialize")
        yield* check(
          request.headers["mcp-session-id"] === activeSession,
          "MCP lost the owned session",
        );
      if (message.id === undefined)
        return HttpClientResponse.fromWeb(request, new globalThis.Response(null, { status: 202 }));
      if (message.method === "tools/list") {
        discoveries++;
        result =
          fault === "mcp-malformed-discovery"
            ? { tools: [{ name: 12 }] }
            : {
                tools: Array.from({ length: 8 }, (_, index) => ({
                  name: index === 0 ? "echo" : `unused_${index}`,
                  description: "Fixed diagnostic echo tool",
                  inputSchema: {
                    type: "object",
                    properties: { message: { type: "string" } },
                    required: ["message"],
                    additionalProperties: false,
                  },
                })),
              };
      }
      if (message.method === "tools/call") {
        const params = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            name: Schema.Literal("echo"),
            arguments: Schema.Struct({ message: Schema.String }),
          }),
        )(message.params);

        calls++;
        result = {
          content: [{ type: "text", text: params.arguments.message }],
          structuredContent: { echoed: params.arguments.message },
          ...(fault === "mcp-call-failure" ? { isError: true } : {}),
        };
      }

      const json = yield* Schema.encodeEffect(rpcResponse)({
        jsonrpc: "2.0",
        id: message.id,
        result,
      });

      return HttpClientResponse.fromWeb(
        request,
        new globalThis.Response(json, {
          status: 200,
          headers: { "content-type": "application/json", "mcp-session-id": activeSession ?? "" },
        }),
      );
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new globalThis.Response(String(cause), { status: 500 }),
          ),
        ),
      ),
    ),
  );

  const request = Mcp.McpConnectionRequest.make({
    serverId: "fixture",
    maxToolCount: 8,
    maxToolDescriptionBytes: 256,
    maxDiscoveryBytes: 16_384,
    connectTimeoutMillis: 5_000,
  });

  const localToolName: string = "echo";

  const localTool = Tool.make(localToolName, {
    parameters: Schema.Struct({ message: Schema.String }),
    success: Schema.Struct({ echoed: Schema.String }),
  });

  const localToolkit = Toolkit.make(localTool);

  const localHandlers = localToolkit.toLayer({
    echo: ({ message }) =>
      Effect.sync(() => {
        localCalls++;

        return { echoed: message };
      }),
  });

  const model = Layer.mergeAll(
    ScriptedModel.layer(
      [0, 1].map((turn): ScriptedTurnInput => ({
        _tag: "Stream",
        termination: { _tag: "Complete" },
        parts:
          turn === 0
            ? [
                {
                  type: "tool-call",
                  id: "mcp-run-call",
                  name: "echo",
                  params: { message: "run-echo" },
                  providerExecuted: false,
                },
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: { inputTokens: {}, outputTokens: {} },
                },
              ]
            : finalParts,
        assertRequest: (provider) =>
          providerCheck(
            Effect.gen(function* () {
              modelCalls++;
              yield* progress.mark({
                name: `mcp.provider.${turn}`,
                elapsedMs: yield* elapsed(start),
              });
              if (turn === 1) {
                const results = provider.prompt.content
                  .filter((message) => message.role === "tool")
                  .flatMap((message) => message.content)
                  .filter((part) => part.type === "tool-result");

                yield* check(
                  results.length === 1 &&
                    results[0]?.isFailure === false &&
                    (yield* encodedPrompt(provider.prompt)).includes("run-echo"),
                  "MCP Run lost its successful remote tool result",
                );
              }
            }),
          ),
        onStreamFinalize: Effect.sync(() => {
          modelFinalizers++;
        }),
      })),
    ),
    Layer.succeed(Model.ProviderName, "scripted"),
    Layer.succeed(Model.ModelName, "mcp-diagnostic"),
  );

  yield* progress.phase("operation");
  start = yield* Clock.monotonicTimeNanos;
  for (let session = 1; session <= (enabled ? 2 : 1); session++) {
    const connectionStart = yield* Clock.monotonicTimeNanos;
    let closeStart = 0n;

    const connector = McpClient.layer([
      McpClient.McpHttpTransport.make({
        serverId: "fixture",
        url: "http://mcp.test/mcp",
        headers: { authorization: `fixture-credential-${session}` },
      }),
    ]).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, client)));

    yield* Effect.scoped(
      Effect.gen(function* () {
        const connection = enabled
          ? yield* Mcp.connectMcp(request).pipe(Effect.provide(connector))
          : undefined;

        if (enabled) {
          metrics.push({ name: `connect.${session}`, value: yield* elapsed(connectionStart) });
          yield* progress.mark({
            name: `mcp.connected.${session}`,
            elapsedMs: yield* elapsed(start),
          });
        }

        // The general connection port erases dynamic tool types. Check the exact public
        // transport schemas before reconstructing a typed Toolkit from the same Tool objects.
        const remoteTools = Object.values(connection?.toolkit.tools ?? {}).filter(
          (tool): tool is McpClient.McpTool =>
            tool.parametersSchema === Schema.Unknown &&
            tool.successSchema === McpClient.McpToolResult &&
            tool.failureSchema === McpClient.McpToolCallFailed &&
            tool.failureMode === "return",
        );

        if (enabled) yield* check(remoteTools.length === 8, "MCP transport tool contracts changed");

        const executableTools: Array<McpClient.McpTool | typeof localTool> =
          connection === undefined ? [localTool] : remoteTools;

        const tools = Toolkit.make(...executableTools);

        const handlers: Layer.Layer<Tool.Handler<string>> | undefined =
          connection?.handlers ?? (enabled ? undefined : localHandlers);

        if (handlers === undefined)
          return yield* BenchmarkError.make({
            message: "MCP connection did not provide executable handlers",
          });
        if (session === 1) {
          yield* Effect.gen(function* () {
            const ready = yield* tools;

            for (let index = 0; index < 2; index++) {
              yield* progress.mark({
                name: `mcp.call.begin.${index}`,
                elapsedMs: yield* elapsed(start),
              });
              const callStart = yield* Clock.monotonicTimeNanos;

              const results = yield* Stream.runCollect(
                yield* ready.handle("echo", { message: `reuse-${index}` }),
              );

              metrics.push({ name: `reusedCall.${index}`, value: yield* elapsed(callStart) });
              const last = results.at(-1);

              yield* check(last !== undefined && !last.isFailure, "MCP reused call failed");

              const expected = enabled
                ? Schema.decodeUnknownOption(McpClient.McpToolResult)(last?.result)
                : undefined;

              if (expected !== undefined) {
                const content =
                  expected._tag === "Some"
                    ? yield* Schema.decodeUnknownEffect(Schema.Struct({ echoed: Schema.String }))(
                        expected.value.structuredContent,
                      )
                    : undefined;

                yield* check(
                  content?.echoed === `reuse-${index}`,
                  "MCP reused call changed its result",
                );
              }
            }
            const runStart = yield* Clock.monotonicTimeNanos;

            const result = yield* AgentRuntime.run(
              Agent.make("mcp-diagnostic", {
                input: Schema.String,
                output: Schema.Struct({ answer: Schema.String }),
                instructions: "Echo, then answer.",
                toolkit: tools,
                policy: { maxTurns: 3, maxToolCalls: 2, maxDuration: "10 seconds" },
              }),
              "Answer",
            );

            metrics.push({ name: "foregroundRun", value: yield* elapsed(runStart) });
            yield* check(result.output.answer === "done", "MCP Run output mismatch");
            yield* (yield* ScriptedModel).assertExhausted;
          }).pipe(
            Effect.provide(
              Layer.mergeAll(handlers, model, IdGenerator.layer, ThreadHistory.layerTransient),
            ),
          );
        }
        closeStart = yield* Clock.monotonicTimeNanos;
      }),
    );
    if (enabled) metrics.push({ name: `scopeClose.${session}`, value: yield* elapsed(closeStart) });
  }
  const totalMs = yield* elapsed(start);

  yield* progress.phase("verification");
  yield* check(modelCalls === 2 && modelFinalizers === 2, "MCP model work/finalizers mismatch");
  yield* check(
    enabled
      ? connects === 2 &&
          discoveries === 2 &&
          calls === 3 &&
          closed === 2 &&
          activeSession === undefined
      : connects === 0 && calls === 0 && closed === 0 && localCalls === 3,
    "MCP session reuse, work, or owned closure mismatch",
  );

  return {
    totalMs,
    metrics,
    counters: [
      { name: "modelCalls", value: modelCalls },
      { name: "modelFinalizers", value: modelFinalizers },
      { name: "toolCalls", value: enabled ? calls : localCalls },
      { name: "connections", value: connects },
      { name: "discoveries", value: discoveries },
      { name: "sessionFinalizers", value: closed },
    ],
  };
});

const sources = MemoryNamespace.define({
  name: "diagnostic/messages",
  version: 1,
  identity: Schema.String,
});

const targets = MemoryNamespace.define({
  name: "diagnostic/profiles",
  version: 1,
  identity: Schema.String,
});

const intentFor = (id: string) =>
  Protocol.Intent.make({
    version: 1,
    id,
    invocationId: `invocation-${id}`,
    source: {
      key: MemoryKey.make({ namespace: sources.make("tenant"), id }),
      locator: `fixture://message/${id}`,
      revision: "r1",
      position: { authorityGeneration: "fixture", sequence: 1 },
    },
    target: MemoryKey.make({ namespace: targets.make("tenant"), id: "profile" }),
  });

/** Local bounded public-port timing fixture, not evidence about disk durability or host queues.
 * An already-running extraction is held in its host Scope while the foreground admits a new job.
 */
const rememberingCase = Effect.fn("diagnostic.remembering")(function* (workload: DiagnosticCase) {
  const progress = yield* DiagnosticProgress;
  const failpoint = yield* Protocol.MutationFailpoint;

  yield* progress.phase("setup");
  const enabled = workload.parameters.enabled === 1;
  const previous = intentFor("previous");
  const incoming = intentFor("incoming");
  const held = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();
  const jobs = new Map<string, Protocol.Checkpoint>();
  let document: MemoryDocument | null = null;
  let sourceLoads = 0;
  let extractions = 0;
  let incomingExtractions = 0;
  let extractionFinalizers = 0;
  let reads = 0;
  let writes = 0;
  let modelCalls = 0;
  let modelFinalizers = 0;
  let admissions = 0;
  let start = 0n;
  let admissionMs = 0;
  let foregroundMs = 0;
  let backgroundReadyMs = 0;
  let profileReadyMs = 0;

  const store: Protocol.Store = {
    admit: Effect.fn(function* (intent) {
      const existing = jobs.get(intent.id);

      if (existing !== undefined)
        return yield* Protocol.AdmissionError.make({ reason: "conflict" });
      if (jobs.size >= 2) return yield* Protocol.AdmissionError.make({ reason: "capacity" });
      jobs.set(
        intent.id,
        Protocol.Checkpoint.make({
          intent,
          version: 0,
          suppression: null,
          progress: { _tag: "Pending" },
        }),
      );

      return Protocol.Admission.make({ id: intent.id, status: "queued" });
    }),
    read: Effect.fn(function* (intent) {
      const value = jobs.get(intent.id);

      if (value === undefined) return yield* Protocol.CheckpointError.make({ reason: "missing" });

      return value;
    }),
    save: Effect.fn(function* ({ intent, expectedVersion, progress: nextProgress }) {
      const current = yield* store.read(intent);

      if (current.version !== expectedVersion)
        return yield* Protocol.CheckpointError.make({ reason: "fenced" });

      const next = Protocol.Checkpoint.make({
        ...current,
        version: current.version + 1,
        progress: nextProgress,
      });

      jobs.set(intent.id, next);

      return next;
    }),
    // This sample only admits two distinct jobs. Unsupported mutation fails closed.
    invalidate: () => Effect.fail(Protocol.AdmissionError.make({ reason: "conflict" })),
  };

  const reader = MemoryReader.fromAdapter({
    get: () =>
      Effect.sync(() => {
        reads++;

        return document;
      }),
  });

  const writer = MemoryWriter.fromAdapter({
    change: (command) =>
      Effect.gen(function* () {
        const next = yield* applyMemoryWrite(document, command, 100);

        document = next;
        writes++;

        return next;
      }),
  });

  const processor = Remembering.make({
    proposal: Schema.Struct({ text: Schema.String }),
    loadSource: (intent: ReturnType<typeof intentFor>) =>
      Effect.sync(() => {
        sourceLoads++;

        return Remembering.SourceSnapshot.make({
          source: intent.source,
          text: "A fixed diagnostic fact.",
        });
      }),
    extract: (snapshot, intent) =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          extractions++;
          if (intent.id === incoming.id) incomingExtractions++;
        }),
        () =>
          Effect.gen(function* () {
            yield* progress.mark({
              name: `remember.extract.${intent.id}`,
              elapsedMs: yield* elapsed(start),
            });
            if (intent.id === previous.id) {
              yield* Deferred.succeed(held, undefined);
              yield* Deferred.await(release);
            }

            return {
              value: { text: snapshot.text },
              evidence: [
                Protocol.Evidence.make({
                  source: {
                    id: snapshot.source.key.id,
                    locator: snapshot.source.locator,
                    revision: snapshot.source.revision,
                  },
                  quote: snapshot.text,
                  startByte: 0,
                  endByte: new TextEncoder().encode(snapshot.text).byteLength,
                }),
              ],
            };
          }),
        () =>
          Effect.gen(function* () {
            extractionFinalizers++;
            yield* progress
              .mark({
                name: `remember.extract.finalized.${intent.id}`,
                elapsedMs: yield* elapsed(start),
              })
              .pipe(Effect.orDie);
          }),
      ),
    merge: ({ proposal }) =>
      Effect.succeed({
        _tag: "Put" as const,
        locator: "fixture://profile",
        content: MemoryContent.make({ ...memoryContent, text: proposal.value.text }),
        scopes: [MemoryScope.make("private")],
      }),
    cleanup: () => Effect.succeed({ _tag: "NoChange" as const }),
  });

  const rememberTool = Tool.make("remember", {
    parameters: Schema.Struct({ index: Schema.Int }),
    success: Schema.Literal("accepted"),
  });

  const rememberToolkit = Toolkit.make(rememberTool);

  const handlers = rememberToolkit.toLayer({
    remember: () =>
      providerCheck(
        Effect.gen(function* () {
          const begin = yield* Clock.monotonicTimeNanos;

          if (enabled) {
            const admitted = yield* Remembering.admit(store, incoming).pipe(
              Effect.provideService(Protocol.MutationFailpoint, failpoint),
            );

            yield* check(admitted.status === "queued", "Remembering admission was not queued");
            admissions++;
          }
          admissionMs = yield* elapsed(begin);
          if (enabled)
            yield* progress.mark({
              name: "remember.admission.complete",
              elapsedMs: yield* elapsed(start),
            });

          return "accepted" as const;
        }),
      ),
  });

  const model = Layer.mergeAll(
    ScriptedModel.layer(
      [0, 1].map((turn): ScriptedTurnInput => ({
        _tag: "Stream",
        termination: { _tag: "Complete" },
        parts:
          turn === 0
            ? [
                {
                  type: "tool-call",
                  id: "remember-call",
                  name: "remember",
                  params: { index: 0 },
                  providerExecuted: false,
                },
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: { inputTokens: {}, outputTokens: {} },
                },
              ]
            : finalParts,
        assertRequest: (request) =>
          providerCheck(
            Effect.gen(function* () {
              modelCalls++;
              yield* progress.mark({
                name: `remember.provider.${turn}`,
                elapsedMs: yield* elapsed(start),
              });
              if (turn === 1)
                yield* check(
                  (yield* encodedPrompt(request.prompt)).includes("accepted") &&
                    incomingExtractions === 0 &&
                    writes === 0,
                  "Foreground ran remembering extraction or writes",
                );
            }),
          ),
        onStreamFinalize: Effect.sync(() => {
          modelFinalizers++;
        }),
      })),
    ),
    Layer.succeed(Model.ProviderName, "scripted"),
    Layer.succeed(Model.ModelName, "remembering-diagnostic"),
  );

  const host = yield* Scope.make();

  yield* Effect.addFinalizer((exit) => Scope.close(host, exit));
  if (enabled) yield* Remembering.admit(store, previous);
  yield* progress.phase("operation");
  start = yield* Clock.monotonicTimeNanos;

  const worker = enabled
    ? yield* Effect.gen(function* () {
        for (const intent of [previous, incoming]) {
          for (let transition = 0; transition < 3; transition++)
            yield* processor.advance({
              intent,
              store,
              limits: Remembering.Limits.make({
                maxSourceBytes: 1_024,
                maxProposalBytes: 4_096,
                timeoutMillis: 5_000,
              }),
              extractionEnabled: true,
            });
        }
      }).pipe(
        Effect.provide(
          Layer.mergeAll(Layer.succeed(MemoryReader, reader), Layer.succeed(MemoryWriter, writer)),
        ),
        Effect.forkIn(host),
      )
    : undefined;

  if (enabled) yield* Deferred.await(held);
  const foregroundStart = yield* Clock.monotonicTimeNanos;

  const result = yield* Effect.scoped(
    Effect.gen(function* () {
      const output = yield* AgentRuntime.run(
        Agent.make("remembering-diagnostic", {
          input: Schema.String,
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Admit remembering, then answer.",
          toolkit: rememberToolkit,
          policy: { maxTurns: 3, maxToolCalls: 2, maxDuration: "10 seconds" },
        }),
        "Answer",
      );

      yield* (yield* ScriptedModel).assertExhausted;

      return output;
    }),
  ).pipe(
    Effect.provide(
      Layer.mergeAll(handlers, model).pipe(
        Layer.provideMerge(Layer.merge(IdGenerator.layer, ThreadHistory.layerTransient)),
      ),
    ),
  );

  foregroundMs = yield* elapsed(foregroundStart);
  const foregroundExtractions = incomingExtractions;
  const foregroundReads = reads;
  const foregroundWrites = writes;

  yield* check(
    incomingExtractions === 0 &&
      writes === 0 &&
      reads === 0 &&
      (!enabled || jobs.get(incoming.id)?.progress._tag === "Pending"),
    "Foreground awaited background readiness",
  );
  yield* progress.mark({ name: "remember.foreground.complete", elapsedMs: yield* elapsed(start) });
  const resumeStart = yield* Clock.monotonicTimeNanos;

  yield* Deferred.succeed(release, undefined);
  if (worker !== undefined) yield* Fiber.join(worker);
  backgroundReadyMs = yield* elapsed(resumeStart);
  if (enabled) {
    profileReadyMs = yield* elapsed(start);
    yield* progress.mark({ name: "remember.profile.ready", elapsedMs: profileReadyMs });
  }
  yield* Scope.close(host, Exit.void);
  const totalMs = yield* elapsed(start);

  yield* progress.phase("verification");
  yield* check(
    result.output.answer === "done" && modelCalls === 2 && modelFinalizers === 2,
    "Remembering foreground work/finalizers mismatch",
  );
  yield* check(
    enabled
      ? extractions === 2 &&
          extractionFinalizers === 2 &&
          writes === 2 &&
          [...jobs.values()].every(
            (job) => job.progress._tag === "Completed" && job.progress.outcome === "applied",
          )
      : sourceLoads === 0 && extractions === 0 && writes === 0,
    "Remembering background completed work mismatch",
  );

  return {
    totalMs,
    metrics: [
      { name: "foregroundRun", value: foregroundMs },
      ...(enabled
        ? [
            { name: "admission", value: admissionMs },
            { name: "backgroundAfterRelease", value: backgroundReadyMs },
            { name: "profileReadyFromOperationStart", value: profileReadyMs },
          ]
        : []),
    ],
    counters: [
      { name: "modelCalls", value: modelCalls },
      { name: "modelFinalizers", value: modelFinalizers },
      { name: "foregroundExtractions", value: foregroundExtractions },
      { name: "foregroundProfileReads", value: foregroundReads },
      { name: "foregroundProfileWrites", value: foregroundWrites },
      { name: "admissions", value: admissions },
      { name: "sourceLoads", value: sourceLoads },
      { name: "backgroundExtractions", value: extractions },
      { name: "extractionFinalizers", value: extractionFinalizers },
      { name: "profileReads", value: reads },
      { name: "profileWrites", value: writes },
    ],
  };
});

/** Two attached children share one real child slot. The first provider deliberately waits
 * ten milliseconds after both slot requests arrive; slot timing includes that labeled hold.
 * Actual runtime span boundaries precede preparation and admission, unlike SubagentStarted.
 */
const subagentCase = Effect.fn("diagnostic.subagent")(function* (workload: DiagnosticCase) {
  const progress = yield* DiagnosticProgress;
  const clock = yield* Clock.Clock;
  const reservations = yield* Reservations.SubagentReservations;

  yield* progress.phase("setup");
  const enabled = workload.parameters.enabled === 1;
  let start = 0n;
  let entries = 0;
  let joins = 0;
  let slotRequests = 0;
  let releases = 0;
  let releaseRequests = 0;
  let prepared = 0;
  let projected = 0;
  let children = 0;
  let childFinalizers = 0;
  let active = 0;
  let maxActive = 0;
  let parents = 0;
  let parentFinalizers = 0;
  let localCalls = 0;
  let traceOverflow = false;
  const traceMarks: Array<DiagnosticMark> = [];
  const metrics: Array<{ name: string; value: number }> = [];

  const traceMark = (name: string) => {
    if (traceMarks.length >= 64) {
      traceOverflow = true;

      return;
    }
    traceMarks.push({
      name,
      elapsedMs: Number(clock.monotonicTimeNanosUnsafe() - start) / 1_000_000,
    });
  };

  const tracer = Tracer.make({
    span(options) {
      const role =
        options.name === "SubagentRuntime.diag_child"
          ? "invocation"
          : options.name === "SubagentReservations.acquireChildSlot"
            ? "slot"
            : options.name === "SubagentReservations.release"
              ? "release"
              : undefined;

      const index =
        role === "invocation"
          ? entries++
          : role === "slot"
            ? slotRequests++
            : role === "release"
              ? releaseRequests++
              : -1;

      const begin = clock.monotonicTimeNanosUnsafe();

      if (role !== undefined) traceMark(`subagent.${role}.begin.${index}`);

      return new (class extends Tracer.NativeSpan {
        // Do not retain provider prompts, span attributes, events, or linked spans.
        override attribute() {}
        override event() {}
        override addLinks() {}
        override end(endTime: bigint, exit: Exit.Exit<unknown, unknown>) {
          if (role !== undefined) {
            traceMark(`subagent.${role}.end.${index}`);
            if (role === "invocation") joins++;
            if (role === "release") releases++;
            if (metrics.length < 64)
              metrics.push({
                name: `${role}.${index}`,
                value: Number(clock.monotonicTimeNanosUnsafe() - begin) / 1_000_000,
              });
            else traceOverflow = true;
          }
          super.end(endTime, exit);
        }
      })({ ...options, links: [] });
    },
  });

  const child = Agent.make("diagnostic-child", {
    input: Schema.Struct({ question: Schema.String }),
    output: Schema.Struct({ answer: Schema.String }),
    instructions: "Return the diagnostic answer.",
    toolkit: Toolkit.empty,
    policy: { maxTurns: 1, maxToolCalls: 1, maxDuration: "10 seconds" },
  });

  const delegation = Subagent.make("diag_child", {
    description: "Run one bounded diagnostic child.",
    target: child,
    parameters: Schema.Struct({ topic: Schema.String }),
    success: Schema.Struct({ summary: Schema.String }),
    failure: BenchmarkError,
    prepareInput: ({ topic }) =>
      Effect.gen(function* () {
        prepared++;
        yield* progress.mark({
          name: `subagent.prepare.begin.${topic}`,
          elapsedMs: yield* elapsed(start),
        });
        const value = { question: topic };

        yield* progress.mark({
          name: `subagent.prepare.end.${topic}`,
          elapsedMs: yield* elapsed(start),
        });

        return value;
      }),
    projectResult: (output, _context, parameters) =>
      Effect.gen(function* () {
        projected++;
        yield* progress.mark({
          name: `subagent.project.${parameters.topic}`,
          elapsedMs: yield* elapsed(start),
        });

        return { summary: `${parameters.topic}:${output.answer}` };
      }),
    policy: Subagent.SubagentPolicy.make({
      maxChildren: 2,
      maxConcurrency: 1,
      maxTurns: 1,
      maxToolCalls: 1,
      maxDuration: "10 seconds",
    }),
  });

  const childModel = Model.make(
    "scripted",
    "diagnostic-child",
    ScriptedModel.layer([
      {
        _tag: "Stream",
        termination: { _tag: "Complete" },
        parts: finalParts,
        assertRequest: (request) =>
          providerCheck(
            Effect.gen(function* () {
              const index = children++;

              active++;
              maxActive = Math.max(maxActive, active);
              yield* progress.mark({
                name: `subagent.childProvider.${index}`,
                elapsedMs: yield* elapsed(start),
              });
              yield* check(
                (yield* encodedPrompt(request.prompt)).includes(`topic-${index}`),
                "Child preparation changed input or order",
              );
              if (index === 0) {
                yield* Effect.gen(function* () {
                  while (slotRequests < 2) yield* Effect.yieldNow;
                }).pipe(Effect.timeout("2 seconds"));
                yield* progress.mark({
                  name: "subagent.queued.ready",
                  elapsedMs: yield* elapsed(start),
                });
                yield* Effect.sleep("10 millis");
              }
            }),
          ),
        onStreamFinalize: Effect.sync(() => {
          childFinalizers++;
          active--;
        }),
      },
    ]),
  );

  const local = Tool.make("diag_child", {
    parameters: Schema.Struct({ topic: Schema.String }),
    success: Schema.Struct({ summary: Schema.String }),
  });

  const localToolkit = Toolkit.make(local);
  const tools = enabled ? Toolkit.make(delegation.tool) : localToolkit;

  const handlers = enabled
    ? Subagent.SubagentRuntime.layer(delegation, Agent.withModel(child, childModel), {
        mapChildFailure: (failure) =>
          BenchmarkError.make({ message: `Diagnostic child failed: ${failure._tag}` }),
      })
    : localToolkit.toLayer({
        diag_child: ({ topic }) =>
          Effect.sync(() => {
            localCalls++;

            return { summary: `${topic}:done` };
          }),
      });

  const model = Layer.mergeAll(
    ScriptedModel.layer(
      [0, 1].map((turn): ScriptedTurnInput => ({
        _tag: "Stream",
        termination: { _tag: "Complete" },
        parts:
          turn === 0
            ? [
                ...[0, 1].map((index) => ({
                  type: "tool-call" as const,
                  id: `child-call-${index}`,
                  name: "diag_child",
                  params: { topic: `topic-${index}` },
                  providerExecuted: false,
                })),
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: { inputTokens: {}, outputTokens: {} },
                },
              ]
            : finalParts,
        assertRequest: (request) =>
          providerCheck(
            Effect.gen(function* () {
              parents++;
              yield* progress.mark({
                name: `subagent.parentProvider.${turn}`,
                elapsedMs: yield* elapsed(start),
              });
              if (turn === 1) {
                const results = request.prompt.content
                  .filter((message) => message.role === "tool")
                  .flatMap((message) => message.content)
                  .filter((part) => part.type === "tool-result");

                yield* check(
                  results.length === 2 && results.every((part) => !part.isFailure),
                  "Parent did not join two successful child results",
                );
                const encoded = yield* encodedPrompt(request.prompt);

                yield* check(
                  encoded.includes("topic-0:done") && encoded.includes("topic-1:done"),
                  "Parent lost projected child results",
                );
              }
            }),
          ),
        onStreamFinalize: Effect.sync(() => {
          parentFinalizers++;
        }),
      })),
    ),
    Layer.succeed(Model.ProviderName, "scripted"),
    Layer.succeed(Model.ModelName, "diagnostic-parent"),
  );

  yield* progress.phase("operation");
  start = yield* Clock.monotonicTimeNanos;

  const result = yield* Effect.scoped(
    Effect.gen(function* () {
      const output = yield* AgentRuntime.run(
        Agent.make("subagent-diagnostic", {
          input: Schema.String,
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Call both diagnostic children, then answer.",
          toolkit: tools,
          policy: { maxTurns: 3, maxToolCalls: 2, maxDuration: "30 seconds", toolConcurrency: 2 },
        }),
        "Answer",
      );

      yield* (yield* ScriptedModel).assertExhausted;

      return output;
    }),
  ).pipe(
    Effect.provide(
      Layer.mergeAll(handlers, model).pipe(
        Layer.provideMerge(Layer.merge(IdGenerator.layer, ThreadHistory.layerTransient)),
      ),
    ),
    Effect.withTracer(tracer),
    // Flush bounded trace scalars even when the Run fails or is interrupted.
    Effect.ensuring(
      Effect.suspend(() =>
        Effect.forEach(traceMarks, (mark) => progress.mark(mark), { discard: true }),
      ).pipe(Effect.orDie),
    ),
  );

  const totalMs = yield* elapsed(start);

  yield* progress.phase("verification");
  yield* check(
    !traceOverflow && result.output.answer === "done" && parents === 2 && parentFinalizers === 2,
    "Subagent parent work or bounded trace mismatch",
  );
  if (enabled) {
    const snapshot = yield* reservations.parentSnapshot(result.runId);

    yield* check(
      snapshot.totalChildInvocations === 2 &&
        snapshot.reservations.length === 2 &&
        snapshot.reservations.every((reservation) => reservation.status === "released"),
      "Subagent reservations did not settle",
    );
    yield* check(
      entries === 2 &&
        joins === 2 &&
        slotRequests === 2 &&
        releases === 2 &&
        prepared === 2 &&
        projected === 2 &&
        children === 2 &&
        childFinalizers === 2 &&
        active === 0 &&
        maxActive === 1,
      "Subagent work, concurrency, or owned finalizers mismatch",
    );
  } else
    yield* check(
      localCalls === 2 && entries === 0 && children === 0,
      "Disabled subagents performed optional work",
    );

  return {
    totalMs,
    metrics,
    counters: [
      { name: "parentModelCalls", value: parents },
      { name: "parentModelFinalizers", value: parentFinalizers },
      { name: "childModelCalls", value: children },
      { name: "childModelFinalizers", value: childFinalizers },
      { name: "preparations", value: prepared },
      { name: "projections", value: projected },
      { name: "slotRequests", value: slotRequests },
      { name: "releasedReservations", value: releases },
      { name: "joinedInvocations", value: joins },
      { name: "maxActiveChildren", value: maxActive },
      { name: "inducedProviderHoldMillis", value: enabled ? 10 : 0 },
      { name: "toolCalls", value: enabled ? entries : localCalls },
    ],
  };
});

/** Each call builds fresh service state; no process-global cache survives a sample. */
export const runCapabilityCase = Effect.fn("diagnostic.runCapabilityCase")(
  function* (workload: DiagnosticCase) {
    const selected = capabilityCases.find((item) => item.name === workload.name);
    const encodeCase = Schema.encodeEffect(Schema.fromJsonString(DiagnosticCase));

    yield* check(
      selected !== undefined && (yield* encodeCase(selected)) === (yield* encodeCase(workload)),
      "Unknown or modified capability diagnostic case",
    );

    const result: DiagnosticResult = yield* Effect.scoped(
      Effect.gen(function* () {
        if (workload.name.startsWith("history-run-")) return yield* historyRunCase(workload);
        if (workload.family === "history") return yield* historyCase(workload);
        if (workload.family === "mcp") return yield* mcpCase(workload);
        if (workload.name.startsWith("remembering-run-"))
          return yield* rememberingCase(workload).pipe(
            Effect.provide(Protocol.MutationFailpoint.layer),
          );
        if (workload.family === "subagent")
          return yield* subagentCase(workload).pipe(
            Effect.provide(Reservations.SubagentReservationsMemoryLive),
          );

        return yield* memoryRunCase(workload);
      }),
    ).pipe(Effect.provide(EphemeralThreads.EphemeralThreadsLive));

    return result;
  },
  Effect.mapError((error) =>
    Schema.is(BenchmarkError)(error)
      ? error
      : BenchmarkError.make({
          message: `Capability diagnostic failed: ${String(error)}`,
          cause: error,
        }),
  ),
);
