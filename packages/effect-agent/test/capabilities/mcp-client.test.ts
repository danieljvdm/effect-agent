import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import type { McpConnector } from "effect-agent/mcp";
import { connectMcp, McpConnectionRequest } from "effect-agent/mcp";
import * as McpClient from "effect-agent/mcp-client";
import type { McpToolCallFailed, McpToolResult } from "effect-agent/mcp-client";
import { McpHttpTransport, McpStdioTransport } from "effect-agent/mcp-client";
import { Toolkit } from "effect/unstable/ai";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import type { ChildProcessSpawner } from "effect/unstable/process";

const request = McpConnectionRequest.make({
  serverId: "fixture",
  maxToolCount: 8,
  maxToolDescriptionBytes: 256,
  maxDiscoveryBytes: 16_384,
  connectTimeoutMillis: 5_000,
});

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

          expect(methods.at(-1)).toBe("DELETE");
          expect(observed[0]!.sessionId).toBeUndefined();
          expect(observed[1]!.hasId).toBe(false);
          for (const entry of observed.slice(1)) {
            expect(entry.sessionId).toBe("session-1");
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

export const verifyKeepsPlatformServicesVisibleInTheConnectorLayer = () => {
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

  void [httpLayer, stdioLayer, mixedLayer];
};
