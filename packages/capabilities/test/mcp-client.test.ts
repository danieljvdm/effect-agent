import { Agent, AgentPolicy, IdGenerator, RunId, ThreadId, TurnId } from "@effect-agent/core";
import {
  AgentRuntime,
  RunContextPreparationPassthrough,
  ThreadHistory,
  ToolExecutionClass,
} from "@effect-agent/engine";
import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import {
  LanguageModel,
  McpProtocol,
  McpServer,
  Model,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
  HttpRouter,
} from "effect/unstable/http";
import type { ChildProcessSpawner } from "effect/unstable/process";

import {
  connectMcp,
  McpClient,
  McpConnectionRequest,
  McpConnector,
  McpHttpTransport,
  McpStdioTransport,
  McpToolCallFailed,
  McpToolResult,
  type McpServerTransport,
} from "../src/index.ts";

// ---------------------------------------------------------------------------
// The HTTP suite runs Effect AI's native `McpServer` in-process behind an
// `HttpClient` whose fetch is the router's web handler, so the client speaks
// real Streamable HTTP (session header, JSON-RPC framing, notifications)
// without a socket. The stdio suite spawns a real Node process running the
// same server family over newline-delimited JSON-RPC.
// ---------------------------------------------------------------------------

const Echo = Tool.make("echo", {
  description: "Echo a message back to the caller.",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.Struct({ echoed: Schema.String }),
}).annotate(Tool.Readonly, true);

class Rejected extends Schema.TaggedError<Rejected>()("Rejected", {
  message: Schema.String,
}) {}

const Reject = Tool.make("reject", {
  description: "Always fails with a tool error.",
  parameters: Schema.Struct({ reason: Schema.String }),
  success: Schema.String,
  failure: Rejected,
});

const FixtureToolkit = Toolkit.make(Echo, Reject);

const FixtureServer = McpServer.toolkit(FixtureToolkit).pipe(
  Layer.provideMerge(
    FixtureToolkit.toLayer({
      echo: ({ message }) => Effect.succeed({ echoed: message }),
      reject: ({ reason }) => Effect.fail(new Rejected({ message: reason })),
    }),
  ),
  Layer.provide(
    McpServer.layerHttp({
      name: "effect-agent-http-fixture",
      version: "1.0.0",
      path: "/mcp",
      protocols: [McpProtocol.v2025_11_25, McpProtocol.v2025_06_18],
    }),
  ),
);

const request = McpConnectionRequest.make({
  serverId: "fixture",
  maxToolCount: 8,
  maxToolDescriptionBytes: 256,
  maxDiscoveryBytes: 16_384,
  connectTimeoutMillis: 5_000,
});

/** `HttpClient` whose fetch dispatches straight into the fixture server's router. */
const inProcessHttpClient = Layer.unwrap(
  Effect.gen(function* () {
    const { dispose, handler } = HttpRouter.toWebHandler(FixtureServer, { disableLogger: true });

    yield* Effect.addFinalizer(() =>
      Effect.tryPromise({ try: () => dispose(), catch: () => undefined }).pipe(Effect.ignore),
    );
    const fetch: typeof globalThis.fetch = Object.assign(
      (input: string | URL | Request, init?: RequestInit) =>
        handler(input instanceof Request ? input : new Request(input, init)),
      { preconnect() {} },
    );

    return FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetch)));
  }),
);

const connectorFor = (transport: McpServerTransport<HttpClient.HttpClient>) =>
  McpClient.layer([transport]).pipe(Layer.provide(inProcessHttpClient));

const httpTransport = (trustToolAnnotations: boolean) =>
  McpHttpTransport.make({
    serverId: "fixture",
    url: "http://mcp.test/mcp",
    trustToolAnnotations,
  });

/** Runs one Tool through the connection's Toolkit and returns its final handler result. */
const callHandler = (
  connection: { readonly toolkit: Toolkit.Any },
  name: string,
  params: unknown,
) =>
  Effect.gen(function* () {
    const withHandler = yield* Toolkit.make(...Object.values(connection.toolkit.tools));
    const results = yield* Stream.runCollect(yield* withHandler.handle(name, params));

    return results[results.length - 1]!;
  });

describe("MCP client", () => {
  it.effect("discovers, validates, and calls tools over Streamable HTTP", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* connectMcp(request);

        expect(connection.discovery.identity.implementation.name).toBe("effect-agent-http-fixture");
        expect(connection.discovery.tools.map((tool) => tool.name).sort()).toEqual([
          "echo",
          "reject",
        ]);
        expect(connection.handlers).toBeDefined();

        const echo = connection.toolkit.tools["echo"]!;

        expect(Tool.getJsonSchema(echo)).toEqual(
          connection.discovery.tools.find((tool) => tool.name === "echo")!.inputSchema,
        );
        // Server hints are recorded as Effect AI metadata but never trusted for
        // the execution class unless the transport opts in.
        expect(Context.get(echo.annotations, Tool.Readonly)).toBe(true);
        expect(Context.get(echo.annotations, ToolExecutionClass)).toBe("uncertain");

        const handlers = connection.handlers!;
        const echoed = yield* callHandler(connection, "echo", { message: "hello" }).pipe(
          Effect.provide(handlers),
        );

        expect(echoed.isFailure).toBe(false);
        expect(echoed.result).toBeInstanceOf(McpToolResult);
        expect((echoed.result as McpToolResult).structuredContent).toEqual({ echoed: "hello" });

        const rejected = yield* callHandler(connection, "reject", { reason: "nope" }).pipe(
          Effect.provide(handlers),
        );

        expect(rejected.isFailure).toBe(true);
        expect(rejected.result).toBeInstanceOf(McpToolCallFailed);
        expect((rejected.result as McpToolCallFailed).reason).toBe("tool-error");
        expect((rejected.result as McpToolCallFailed).message).toContain("nope");
      }),
    ).pipe(Effect.provide(Layer.merge(connectorFor(httpTransport(false)), NodeCrypto.layer))),
  );

  it.effect("adopts trusted server hints into the execution class", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* connectMcp(request);
        const echo = connection.toolkit.tools["echo"]!;
        const reject = connection.toolkit.tools["reject"]!;

        expect(Context.get(echo.annotations, ToolExecutionClass)).toBe("readonly");
        // No hints at all stays fail-closed even when hints are trusted.
        expect(Context.get(reject.annotations, ToolExecutionClass)).toBe("uncertain");
      }),
    ).pipe(Effect.provide(Layer.merge(connectorFor(httpTransport(true)), NodeCrypto.layer))),
  );

  it.effect("fails closed when the pinned discovery digest does not match", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const first = yield* connectMcp(request);
        const pinned = McpConnectionRequest.make({
          ...request,
          expectedToolkitSchemaDigest: first.discovery.toolkitSchemaDigest,
        });
        const drifted = McpConnectionRequest.make({
          ...request,
          expectedToolkitSchemaDigest: `sha256:${"0".repeat(64)}`,
        });

        const repeated = yield* connectMcp(pinned);

        expect(repeated.discovery.toolkitSchemaDigest).toBe(first.discovery.toolkitSchemaDigest);

        const error = yield* connectMcp(drifted).pipe(Effect.flip);

        expect(error._tag).toBe("McpToolkitMismatch");
      }),
    ).pipe(Effect.provide(Layer.merge(connectorFor(httpTransport(false)), NodeCrypto.layer))),
  );

  it.effect("runs an agent turn through a discovered MCP tool", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* connectMcp(request);
        const observed = yield* Ref.make<ReadonlyArray<Response.ToolResultPartEncoded>>([]);
        const usage = { inputTokens: {}, outputTokens: {} };
        const model = Model.make(
          "scripted",
          "mcp",
          Layer.effect(
            LanguageModel.LanguageModel,
            Effect.gen(function* () {
              const turn = yield* Ref.make(0);

              return yield* LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: ({ prompt }) =>
                  Stream.unwrap(
                    Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                      Effect.tap(() =>
                        Ref.update(observed, (all) => [
                          ...all,
                          ...prompt.content
                            .filter((message) => message.role === "tool")
                            .flatMap((message) => message.content)
                            .filter((part) => part.type === "tool-result"),
                        ]),
                      ),
                      Effect.map((value) =>
                        Stream.fromIterable<Response.StreamPartEncoded>(
                          value === 0
                            ? [
                                {
                                  type: "tool-call",
                                  id: "echo-1",
                                  name: "echo",
                                  params: { message: "via-mcp" },
                                  providerExecuted: false,
                                },
                                { type: "finish", reason: "tool-calls", usage },
                              ]
                            : [
                                { type: "text-start", id: "answer" },
                                { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
                                { type: "text-end", id: "answer" },
                                { type: "finish", reason: "stop", usage },
                              ],
                        ),
                      ),
                    ),
                  ),
              });
            }),
          ),
        );
        const agent = Agent.make("mcp-host", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Use the echo tool.",
          toolkit: Toolkit.make(...Object.values(connection.toolkit.tools)),
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const result = yield* AgentRuntime.run(
          Agent.withModel(agent, model),
          { question: "go" },
          {},
        ).pipe(Effect.provide(connection.handlers!));

        expect(result.output).toEqual({ answer: "done" });

        const toolResults = yield* Ref.get(observed);

        expect(toolResults).toHaveLength(1);
        expect(toolResults[0]!.isFailure).toBe(false);
        expect(toolResults[0]!.result).toMatchObject({ structuredContent: { echoed: "via-mcp" } });
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          connectorFor(httpTransport(false)),
          NodeCrypto.layer,
          Layer.succeed(IdGenerator, {
            nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("thread-mcp")),
            nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-mcp")),
            nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-mcp")),
          }),
          ThreadHistory.layerTransient,
          RunContextPreparationPassthrough,
        ),
      ),
    ),
  );

  it.effect("rejects a server that is not configured", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const connector = yield* McpConnector;
        const exit = yield* connector
          .connect(McpConnectionRequest.make({ ...request, serverId: "unknown" }))
          .pipe(Effect.exit);

        expect(Exit.isFailure(exit)).toBe(true);
      }),
    ).pipe(Effect.provide(connectorFor(httpTransport(false)))),
  );

  it.effect(
    "connects to a stdio server process and stops it with the Scope",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* connectMcp(request);

          expect(connection.discovery.identity.implementation.name).toBe(
            "effect-agent-stdio-fixture",
          );
          const echoed = yield* callHandler(connection, "echo", { message: "stdio" }).pipe(
            Effect.provide(connection.handlers!),
          );

          expect((echoed.result as McpToolResult).structuredContent).toEqual({ echoed: "stdio" });
        }),
      ).pipe(
        Effect.provide(
          Layer.merge(
            McpClient.layer([
              McpStdioTransport.make({
                serverId: "fixture",
                command: process.execPath,
                args: [
                  "--experimental-transform-types",
                  new URL("./fixtures/mcp-stdio-server.ts", import.meta.url).pathname,
                ],
              }),
            ]).pipe(Layer.provide(NodeServices.layer)),
            NodeCrypto.layer,
          ),
        ),
      ),
    30_000,
  );
});

describe("MCP client requirements", () => {
  // An HTTP transport needs `HttpClient`, a stdio transport needs
  // `ChildProcessSpawner`, and the connector Layer keeps them in `R`.
  it("keeps platform services visible in the connector Layer", () => {
    const httpLayer: Layer.Layer<McpConnector, never, HttpClient.HttpClient> = McpClient.layer([
      httpTransport(false),
    ]);
    const stdioLayer: Layer.Layer<McpConnector, never, ChildProcessSpawner.ChildProcessSpawner> =
      McpClient.layer([McpStdioTransport.make({ serverId: "fixture", command: process.execPath })]);

    const mixedLayer: Layer.Layer<
      McpConnector,
      never,
      HttpClient.HttpClient | ChildProcessSpawner.ChildProcessSpawner
    > = McpClient.layer([
      httpTransport(false),
      McpStdioTransport.make({ serverId: "local", command: process.execPath }),
    ]);

    expect(Layer.isLayer(httpLayer)).toBe(true);
    expect(Layer.isLayer(stdioLayer)).toBe(true);
    expect(Layer.isLayer(mixedLayer)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// A scripted server that answers every request as `text/event-stream`, mints
// a session, and negotiates an older protocol revision. Effect's own server
// replies with JSON, so this is the only coverage of the SSE response path,
// session propagation, notification framing, and session teardown.
// ---------------------------------------------------------------------------

const JsonRpcRequest = Schema.fromJsonString(
  Schema.Struct({
    id: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])),
    method: Schema.String,
    params: Schema.optionalKey(Schema.Unknown),
  }),
);
const decodeJsonRpcRequest = Schema.decodeUnknownEffect(JsonRpcRequest);

interface ObservedRequest {
  readonly httpMethod: string;
  readonly method: string | undefined;
  readonly hasId: boolean;
  readonly sessionId: string | undefined;
  readonly protocolVersion: string | undefined;
}

/** One SSE event carrying a JSON-RPC message, with the non-data fields a resumable server emits. */
const sseBody = (message: unknown): string =>
  `event: message\nid: 7\nretry: 1000\ndata: ${JSON.stringify(message)}\n\n`;

const sseResponse = (
  request: HttpClientRequest.HttpClientRequest,
  message: unknown,
  headers?: Record<string, string>,
) =>
  HttpClientResponse.fromWeb(
    request,
    new globalThis.Response(sseBody(message), {
      status: 200,
      headers: { "content-type": "text/event-stream", ...headers },
    }),
  );

const sseResult = (
  request: HttpClientRequest.HttpClientRequest,
  id: string | number,
  result: unknown,
  headers?: Record<string, string>,
) => sseResponse(request, { jsonrpc: "2.0", id, result }, headers);

const scriptedSseServer = Effect.gen(function* () {
  const observed = yield* Ref.make<ReadonlyArray<ObservedRequest>>([]);
  const client = HttpClient.make((request) =>
    Effect.gen(function* () {
      const web = yield* HttpClientRequest.toWeb(request);
      const record = (method: string | undefined, hasId: boolean) =>
        Ref.update(observed, (all) => [
          ...all,
          {
            httpMethod: request.method,
            method,
            hasId,
            sessionId: request.headers["mcp-session-id"],
            protocolVersion: request.headers["mcp-protocol-version"],
          },
        ]);

      if (request.method === "DELETE") {
        yield* record(undefined, false);

        return HttpClientResponse.fromWeb(request, new globalThis.Response(null, { status: 200 }));
      }
      const body = yield* Effect.tryPromise({ try: () => web.text(), catch: () => undefined });
      const message = yield* decodeJsonRpcRequest(body);

      yield* record(message.method, message.id !== undefined);
      if (message.id === undefined) {
        return HttpClientResponse.fromWeb(request, new globalThis.Response(null, { status: 202 }));
      }
      switch (message.method) {
        case "initialize": {
          return sseResult(
            request,
            message.id,
            {
              protocolVersion: "2025-06-18",
              capabilities: { tools: {} },
              serverInfo: { name: "scripted-sse", version: "0.1.0" },
            },
            { "mcp-session-id": "session-1" },
          );
        }
        case "tools/list": {
          return sseResult(request, message.id, {
            tools: [
              {
                name: "echo",
                description: "Echo over SSE.",
                inputSchema: { type: "object", properties: { message: { type: "string" } } },
              },
              {
                name: "strict",
                description: "Rejects every call with a JSON-RPC error.",
                inputSchema: { type: "object" },
              },
              {
                name: "silent",
                description: "Accepts the request and never answers it.",
                inputSchema: { type: "object" },
              },
            ],
          });
        }
        case "tools/call": {
          const params = Schema.decodeUnknownOption(Schema.Struct({ name: Schema.String }))(
            message.params,
          );

          if (Option.isSome(params) && params.value.name === "silent") {
            return HttpClientResponse.fromWeb(
              request,
              new globalThis.Response(null, { status: 202 }),
            );
          }
          if (Option.isSome(params) && params.value.name === "strict") {
            return sseResponse(request, {
              jsonrpc: "2.0",
              id: message.id,
              error: { code: -32602, message: "Invalid params: strict rejects everything" },
            });
          }

          return sseResult(request, message.id, {
            content: [{ type: "text", text: "echoed" }],
            structuredContent: { echoed: "sse" },
          });
        }
        default: {
          return HttpClientResponse.fromWeb(
            request,
            new globalThis.Response(null, { status: 404 }),
          );
        }
      }
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

  return { client, observed };
});

describe("MCP client over server-sent events", () => {
  it.effect(
    "decodes SSE responses, propagates the session, and ends it with the caller's Scope",
    () =>
      Effect.gen(function* () {
        const server = yield* scriptedSseServer;
        const connector = McpClient.layer([httpTransport(false)]).pipe(
          Layer.provide(Layer.succeed(HttpClient.HttpClient, server.client)),
        );

        // The connector Layer stays alive for the whole test; only the
        // connection's own Scope closes, and that alone must end the session.
        yield* Effect.gen(function* () {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const connection = yield* connectMcp(request);

              expect(connection.discovery.identity.implementation.name).toBe("scripted-sse");

              const echoed = yield* callHandler(connection, "echo", { message: "x" }).pipe(
                Effect.provide(connection.handlers!),
              );

              expect((echoed.result as McpToolResult).structuredContent).toEqual({ echoed: "sse" });

              // A JSON-RPC error outside the MCP error union still reaches the
              // model as a failed tool result instead of aborting the batch.
              const strict = yield* callHandler(connection, "strict", {}).pipe(
                Effect.provide(connection.handlers!),
              );

              expect(strict.isFailure).toBe(true);
              expect(strict.result).toBeInstanceOf(McpToolCallFailed);
              expect((strict.result as McpToolCallFailed).reason).toBe("protocol-error");

              // An exchange that ends without a response for the request fails
              // that call promptly instead of waiting for an outer timeout.
              const silent = yield* callHandler(connection, "silent", {}).pipe(
                Effect.provide(connection.handlers!),
              );

              expect(silent.isFailure).toBe(true);
              expect((silent.result as McpToolCallFailed).message).toContain("without answering");
            }),
          );

          const observed = yield* Ref.get(server.observed);
          const methods = observed.map((entry) => entry.method ?? entry.httpMethod);

          expect(methods).toEqual([
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/call",
            "tools/call",
            "tools/call",
            "DELETE",
          ]);
          expect(observed[0]!.sessionId).toBeUndefined();
          expect(observed[1]!.hasId).toBe(false);
          for (const entry of observed.slice(1)) {
            expect(entry.sessionId).toBe("session-1");
            expect(entry.protocolVersion).toBe("2025-06-18");
          }
        }).pipe(Effect.provide(Layer.merge(connector, NodeCrypto.layer)));
      }),
  );
});

describe("MCP client over a stdio process that exits", () => {
  it.effect(
    "fails the connection instead of hanging when the server process exits",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const error = yield* connectMcp(request).pipe(Effect.flip);

          expect(error._tag).toBe("McpConnectionError");
        }),
      ).pipe(
        Effect.provide(
          Layer.merge(
            McpClient.layer([
              McpStdioTransport.make({
                serverId: "fixture",
                command: process.execPath,
                args: ["-e", "process.exit(0)"],
              }),
            ]).pipe(Layer.provide(NodeServices.layer)),
            NodeCrypto.layer,
          ),
        ),
      ),
    15_000,
  );
});
