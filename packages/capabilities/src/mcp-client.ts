import { ToolExecutionClass, type ToolExecutionClassValue } from "@effect-agent/engine";
import {
  Cause,
  Effect,
  Encoding,
  Layer,
  Option,
  Predicate,
  Queue,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import * as McpProtocol from "effect/unstable/ai/McpProtocol";
import * as McpSchema from "effect/unstable/ai/McpSchema";
import { Sse } from "effect/unstable/encoding";
import {
  Headers,
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { RpcClient, RpcClientError, type RpcMessage, RpcSerialization } from "effect/unstable/rpc";

import {
  McpConnectionError,
  McpConnector,
  type McpConnectedServer,
  type McpConnectionRequest,
  McpServerIdentity,
  McpToolOutputSchema,
} from "./mcp.ts";

// ---------------------------------------------------------------------------
// MCP client: native Effect AI MCP schemas and protocol adapters, the Effect
// RPC client, and Effect HTTP / child-process services connect an Agent to a
// remote MCP server. Discovered tools become dynamic Effect AI Tools whose
// handlers forward `tools/call`; `connectMcp` still applies every discovery
// bound before the Toolkit reaches Agent context.
// ---------------------------------------------------------------------------

/** Largest single JSON-RPC message a transport accepts from a server by default. */
const DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024;
/** Bounded `tools/list` pagination: a server cannot keep a client walking cursors forever. */
const MAX_TOOL_LIST_PAGES = 64;
const MCP_SESSION_ID_HEADER = "mcp-session-id";
const MCP_PROTOCOL_VERSION_HEADER = "mcp-protocol-version";
const JSON_RPC_METHOD_NOT_FOUND = -32601;

/**
 * The protocol revisions this client negotiates, as implemented by Effect AI's
 * native MCP protocol adapters. The newest revision is offered first; a server
 * answering with any other listed revision is accepted, anything else fails.
 */
export const McpClientProtocolVersion = Schema.Literals([
  McpProtocol.v2025_11_25.protocolVersion,
  McpProtocol.v2025_06_18.protocolVersion,
  McpProtocol.v2025_03_26.protocolVersion,
  McpProtocol.v2024_11_05.protocolVersion,
]);
export type McpClientProtocolVersion = typeof McpClientProtocolVersion.Type;

const OFFERED_PROTOCOL_VERSION: McpClientProtocolVersion = McpProtocol.v2025_11_25.protocolVersion;

/** Identity this client presents to servers during `initialize`. */
export const defaultMcpClientInfo = McpSchema.Implementation.make({
  name: "effect-agent",
  version: "1.0.0",
});

/**
 * The model-visible result of a remote MCP tool call: the server's content
 * blocks and, when the tool declares an `outputSchema`, its structured result.
 * A result with `isError` never becomes this value; it fails the Tool Call.
 */
export class McpToolResult extends Schema.Class<McpToolResult>(
  "@effect-agent/capabilities/McpToolResult",
)({
  content: Schema.Array(McpSchema.ContentBlock),
  structuredContent: Schema.optionalKey(Schema.Json),
}) {}

/**
 * Typed failure of one remote MCP tool call. `tool-error` carries the server's
 * error content; the other reasons describe why no trustworthy result exists.
 * None of them claims the remote effect did not happen.
 */
export class McpToolCallFailed extends Schema.TaggedError<McpToolCallFailed>()(
  "McpToolCallFailed",
  {
    serverId: Schema.NonEmptyString,
    tool: Schema.NonEmptyString,
    reason: Schema.Literals(["tool-error", "invalid-arguments", "protocol-error", "transport"]),
    message: Schema.String,
    content: Schema.optionalKey(Schema.Array(McpSchema.ContentBlock)),
  },
) {}

/**
 * One reachable MCP server. `protocol` acquires a JSON-RPC transport in the
 * caller's Scope; `R` names the platform services that transport needs so the
 * connector Layer keeps them in its requirements.
 */
export interface McpServerTransport<R = never> {
  readonly serverId: string;
  /**
   * Whether the server's `readOnlyHint`, `idempotentHint`, and
   * `destructiveHint` may set the framework execution class of its tools.
   * Hints are server-declared and untrusted, so the default leaves every
   * remote tool `uncertain`.
   */
  readonly trustToolAnnotations: boolean;
  readonly protocol: Effect.Effect<
    RpcClient.Protocol["Service"],
    McpConnectionError,
    Scope.Scope | R
  >;
}

/** Streamable HTTP server address. Header values may carry credentials the application owns. */
export interface McpHttpTransportOptions {
  readonly serverId: string;
  readonly url: string;
  readonly headers?: Headers.Input | undefined;
  /** Largest accepted response message. Defaults to 4 MiB. */
  readonly maxMessageBytes?: number | undefined;
  readonly trustToolAnnotations?: boolean | undefined;
}

/** A local MCP server process speaking newline-delimited JSON-RPC on stdio. */
export interface McpStdioTransportOptions {
  readonly serverId: string;
  readonly command: string;
  readonly args?: ReadonlyArray<string> | undefined;
  readonly cwd?: string | undefined;
  readonly env?: Readonly<Record<string, string>> | undefined;
  /** Largest accepted stdout frame. Defaults to 4 MiB. */
  readonly maxMessageBytes?: number | undefined;
  readonly trustToolAnnotations?: boolean | undefined;
}

const isNotificationTag = (tag: string): boolean => tag.startsWith("notifications/");
const isRpcClientError = Schema.is(RpcClientError.RpcClientError);

const protocolDefect = (message: string, cause?: unknown): RpcClientError.RpcClientError =>
  new RpcClientError.RpcClientError({
    reason: new RpcClientError.RpcClientDefect({ message, cause }),
  });

const InitializeProtocolVersion = Schema.Struct({ protocolVersion: Schema.String });
const decodeInitializeProtocolVersion = Schema.decodeUnknownOption(InitializeProtocolVersion);

const ServerRequest = Schema.Struct({ _tag: Schema.Literal("Request"), tag: Schema.String });
const isServerRequest = Schema.is(ServerRequest);
const isJsonRpcRequest = (message: unknown): message is RpcMessage.RequestEncoded =>
  isServerRequest(message);

/** JSON-RPC error a client returns for a server-initiated request it does not serve. */
const declineServerRequest = (
  request: RpcMessage.RequestEncoded,
): RpcMessage.ResponseExitEncoded => ({
  _tag: "Exit",
  requestId: request.id,
  exit: {
    _tag: "Failure",
    cause: [
      {
        _tag: "Fail",
        error: {
          code: JSON_RPC_METHOD_NOT_FOUND,
          message: `Client does not serve ${request.tag}`,
        },
      },
    ],
  },
});

/**
 * Routes one decoded JSON-RPC message. Responses reach the RPC client; server
 * notifications are logged; server requests (sampling, elicitation, roots)
 * are declined fail-closed through `respond` because this client serves none.
 */
const routeMessage = (
  message: unknown,
  options: {
    readonly clientId: number;
    readonly writeResponse: (
      clientId: number,
      response: RpcMessage.FromServerEncoded,
    ) => Effect.Effect<void>;
    readonly respond: (response: RpcMessage.ResponseExitEncoded) => Effect.Effect<void>;
    readonly onExit: (exit: RpcMessage.ResponseExitEncoded) => void;
  },
): Effect.Effect<void> => {
  if (isJsonRpcRequest(message)) {
    if (message.isNotification === true) {
      return Effect.logDebug("Ignoring MCP server notification").pipe(
        Effect.annotateLogs({ method: message.tag }),
      );
    }

    return options.respond(declineServerRequest(message));
  }
  if (Predicate.hasProperty(message, "_tag") && message._tag === "Exit") {
    options.onExit(message as RpcMessage.ResponseExitEncoded);
  }

  return options.writeResponse(options.clientId, message as RpcMessage.FromServerEncoded);
};

const byteLength = (text: string): number => Encoding.encodeHex(text).length / 2;

/** Reads a response body as text while refusing bodies above `maxBytes`. */
const readBoundedText = Effect.fn("McpHttpTransport.readBoundedText")(function* (
  response: HttpClientResponse.HttpClientResponse,
  maxBytes: number,
) {
  const declared = Number(response.headers["content-length"]);

  if (Number.isFinite(declared) && declared > maxBytes) {
    return yield* protocolDefect(
      `MCP response of ${declared} bytes exceeds the ${maxBytes} byte message bound`,
    );
  }
  const chunks: Array<string> = [];
  let received = 0;

  yield* response.stream.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) => {
      received += byteLength(chunk);
      if (received > maxBytes) {
        return Effect.fail(
          protocolDefect(`MCP response exceeds the ${maxBytes} byte message bound`),
        );
      }
      chunks.push(chunk);

      return Effect.void;
    }),
    Effect.mapError((error) =>
      isRpcClientError(error)
        ? error
        : protocolDefect("Could not read the MCP HTTP response", error),
    ),
  );

  return chunks.join("");
});

const isSseRetry = (error: unknown): boolean =>
  Predicate.hasProperty(error, "_tag") && error._tag === "Retry";

/**
 * Delivers the `data` of each `text/event-stream` event as it arrives through
 * Effect's SSE decoder, retaining at most `maxBytes` of a pending event. A
 * server retry directive ends the stream without failing the request.
 */
const consumeEventStream = Effect.fn("McpHttpTransport.consumeEventStream")(function* (
  response: HttpClientResponse.HttpClientResponse,
  maxBytes: number,
  deliver: (data: string) => Effect.Effect<void>,
) {
  yield* response.stream.pipe(
    Stream.decodeText(),
    Stream.pipeThroughChannelOrFail(Sse.decode({ maxEventSize: maxBytes })),
    Stream.runForEach((event) => (event.data.length === 0 ? Effect.void : deliver(event.data))),
    Effect.catch((error) =>
      isSseRetry(error)
        ? Effect.void
        : Effect.fail(protocolDefect("Could not read the MCP event stream", error)),
    ),
  );
});

const makeHttpProtocol = Effect.fn("McpHttpTransport.protocol")(function* (
  options: McpHttpTransportOptions,
) {
  const client = yield* HttpClient.HttpClient;
  const serialization = RpcSerialization.jsonRpc();
  const parser = serialization.makeUnsafe();
  const maxMessageBytes = options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES;
  const configuredHeaders = Headers.fromInput(options.headers);
  const url = options.url;
  let sessionId: string | undefined;
  let protocolVersion: string | undefined;
  let initializeRequestId: string | number | undefined;

  const transportHeaders = (): Headers.Headers =>
    Headers.merge(
      configuredHeaders,
      Headers.fromInput({
        accept: "application/json, text/event-stream",
        ...(sessionId === undefined ? {} : { [MCP_SESSION_ID_HEADER]: sessionId }),
        ...(protocolVersion === undefined
          ? {}
          : { [MCP_PROTOCOL_VERSION_HEADER]: protocolVersion }),
      }),
    );

  const post = (body: string) =>
    client
      .execute(
        HttpClientRequest.post(url).pipe(
          HttpClientRequest.setHeaders(transportHeaders()),
          HttpClientRequest.bodyText(body, "application/json"),
        ),
      )
      .pipe(
        Effect.mapError((error) =>
          protocolDefect(`MCP HTTP request to ${options.serverId} failed`, error),
        ),
      );

  // Ending the session is a courtesy to the server; the Scope owns the client.
  yield* Effect.addFinalizer(() =>
    sessionId === undefined
      ? Effect.void
      : client
          .execute(
            HttpClientRequest.delete(url).pipe(HttpClientRequest.setHeaders(transportHeaders())),
          )
          .pipe(Effect.ignore),
  );

  return yield* RpcClient.Protocol.make((writeResponse) =>
    Effect.sync(() => {
      const encode = (message: unknown): Effect.Effect<string, RpcClientError.RpcClientError> =>
        Effect.suspend(() => {
          const encoded = parser.encode(message);

          return typeof encoded === "string"
            ? Effect.succeed(encoded)
            : Effect.fail(protocolDefect("Could not encode an MCP JSON-RPC message"));
        });

      const deliver = (clientId: number, data: string): Effect.Effect<void> =>
        Effect.suspend(() => {
          let messages: ReadonlyArray<unknown>;

          try {
            messages = parser.decode(data);
          } catch (cause) {
            return writeResponse(clientId, {
              _tag: "ClientProtocolError",
              error: protocolDefect("Could not decode an MCP JSON-RPC message", cause),
            });
          }

          return Effect.forEach(
            messages,
            (message) =>
              routeMessage(message, {
                clientId,
                writeResponse,
                respond: (response) => encode(response).pipe(Effect.flatMap(post), Effect.ignore),
                onExit: (exit) => {
                  if (exit.requestId !== initializeRequestId || exit.exit._tag !== "Success") {
                    return;
                  }
                  const decoded = decodeInitializeProtocolVersion(exit.exit.value);

                  if (Option.isSome(decoded)) protocolVersion = decoded.value.protocolVersion;
                },
              }),
            { discard: true },
          );
        });

      const dispatch = Effect.fn("McpHttpTransport.dispatch")(function* (
        clientId: number,
        message: unknown,
      ) {
        const response = yield* post(yield* encode(message));
        const responseSession = response.headers[MCP_SESSION_ID_HEADER];

        if (responseSession !== undefined) sessionId = responseSession;
        if (response.status === 202 || response.status === 204) return;
        if (response.status < 200 || response.status >= 300) {
          return yield* protocolDefect(
            `MCP server ${options.serverId} answered HTTP ${response.status}`,
          );
        }
        const contentType = response.headers["content-type"] ?? "";

        if (contentType.startsWith("text/event-stream")) {
          return yield* consumeEventStream(response, maxMessageBytes, (data) =>
            deliver(clientId, data),
          );
        }
        const text = yield* readBoundedText(response, maxMessageBytes);

        if (text.length > 0) yield* deliver(clientId, text);
      });

      const send: RpcClient.Protocol["Service"]["send"] = (clientId, request) => {
        switch (request._tag) {
          case "Request": {
            if (isNotificationTag(request.tag)) {
              return dispatch(clientId, { ...request, isNotification: true });
            }
            if (request.tag === "initialize") initializeRequestId = request.id;

            return dispatch(clientId, request);
          }
          case "Interrupt": {
            return dispatch(clientId, {
              _tag: "Request",
              id: request.requestId,
              tag: "notifications/cancelled",
              payload: { requestId: request.requestId },
              headers: [],
              isNotification: true,
            } satisfies RpcMessage.RequestEncoded);
          }
          case "Ack":
          case "Ping":
          case "Eof": {
            return Effect.void;
          }
        }
      };

      return { send, supportsAck: false, supportsTransferables: false };
    }),
  );
});

const makeStdioProtocol = Effect.fn("McpStdioTransport.protocol")(function* (
  options: McpStdioTransportOptions,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const maxMessageBytes = options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES;
  const serialization = RpcSerialization.ndJsonRpc({ maxBufferSize: maxMessageBytes });
  const parser = serialization.makeUnsafe();
  const handle = yield* spawner
    .spawn(
      ChildProcess.make(options.command, [...(options.args ?? [])], {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        ...(options.env === undefined ? {} : { env: options.env }),
      }),
    )
    .pipe(
      Effect.mapError((cause) =>
        McpConnectionError.make({
          serverId: options.serverId,
          message: `Could not start the MCP server process for ${options.serverId}`,
          cause,
        }),
      ),
    );
  const outbound = yield* Queue.unbounded<string>();

  yield* Stream.fromQueue(outbound).pipe(
    Stream.encodeText,
    Stream.run(handle.stdin),
    Effect.ignore,
    Effect.forkScoped,
  );
  yield* handle.stderr.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.runForEach((line) => Effect.logDebug(line)),
    Effect.ignore,
    Effect.annotateLogs({ mcpServerId: options.serverId, stream: "stderr" }),
    Effect.forkScoped,
  );

  return yield* RpcClient.Protocol.make(
    Effect.fnUntraced(function* (writeResponse, clientIds) {
      const broadcast = (error: RpcClientError.RpcClientError) =>
        Effect.forEach(
          clientIds,
          (clientId) => writeResponse(clientId, { _tag: "ClientProtocolError", error }),
          { discard: true },
        );
      const deliver = (chunk: Uint8Array): Effect.Effect<void> =>
        Effect.suspend(() => {
          let messages: ReadonlyArray<unknown>;

          try {
            messages = parser.decode(chunk);
          } catch (cause) {
            return broadcast(protocolDefect("Could not decode an MCP stdio frame", cause));
          }

          return Effect.forEach(
            messages,
            (message) =>
              Effect.forEach(
                clientIds,
                (clientId) =>
                  routeMessage(message, {
                    clientId,
                    writeResponse,
                    respond: (response) => enqueue(response).pipe(Effect.ignore),
                    onExit: () => {},
                  }),
                { discard: true },
              ),
            { discard: true },
          );
        });
      const enqueue = (message: unknown): Effect.Effect<void, RpcClientError.RpcClientError> =>
        Effect.suspend(() => {
          const encoded = parser.encode(message);

          if (typeof encoded !== "string") {
            return Effect.fail(protocolDefect("Could not encode an MCP JSON-RPC message"));
          }

          return Queue.offer(outbound, encoded).pipe(Effect.asVoid);
        });

      yield* handle.stdout.pipe(
        Stream.runForEach(deliver),
        Effect.matchCauseEffect({
          onSuccess: () =>
            broadcast(protocolDefect(`MCP server process ${options.serverId} closed stdout`)),
          onFailure: (cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : broadcast(
                  protocolDefect(
                    `Reading MCP server process ${options.serverId} failed`,
                    Cause.squash(cause),
                  ),
                ),
        }),
        Effect.forkScoped,
      );

      const send: RpcClient.Protocol["Service"]["send"] = (_clientId, request) => {
        switch (request._tag) {
          case "Request": {
            return enqueue(
              isNotificationTag(request.tag) ? { ...request, isNotification: true } : request,
            );
          }
          case "Interrupt": {
            return enqueue({
              _tag: "Request",
              id: request.requestId,
              tag: "notifications/cancelled",
              payload: { requestId: request.requestId },
              headers: [],
              isNotification: true,
            } satisfies RpcMessage.RequestEncoded);
          }
          case "Ack":
          case "Ping":
          case "Eof": {
            return Effect.void;
          }
        }
      };

      return { send, supportsAck: false, supportsTransferables: false };
    }),
  );
});

/** Streamable HTTP transport for one remote MCP server; requires `HttpClient`. */
export const McpHttpTransport = {
  make: (options: McpHttpTransportOptions): McpServerTransport<HttpClient.HttpClient> => ({
    serverId: options.serverId,
    trustToolAnnotations: options.trustToolAnnotations ?? false,
    protocol: makeHttpProtocol(options),
  }),
} as const;

/** stdio transport for one local MCP server process; requires `ChildProcessSpawner`. */
export const McpStdioTransport = {
  make: (
    options: McpStdioTransportOptions,
  ): McpServerTransport<ChildProcessSpawner.ChildProcessSpawner> => ({
    serverId: options.serverId,
    trustToolAnnotations: options.trustToolAnnotations ?? false,
    protocol: makeStdioProtocol(options),
  }),
} as const;

/**
 * Effect AI Tool derived from one discovered MCP tool. Its parameters are the
 * server's JSON Schema, so handlers receive `unknown` and the server validates
 * arguments; `Tool.getJsonSchema` still advertises the discovered schema. A
 * failed call returns `McpToolCallFailed` to the model as a failed tool result.
 */
export type McpTool = Tool.Tool<
  string,
  {
    readonly parameters: typeof Schema.Unknown;
    readonly success: typeof McpToolResult;
    readonly failure: typeof McpToolCallFailed;
    readonly failureMode: "return";
  }
>;

/** Toolkit of discovered MCP tools; every member forwards `tools/call` to its server. */
export type McpToolkit = Toolkit.Toolkit<Record<string, McpTool>>;

const executionClassFor = (
  annotations: McpSchema.ToolAnnotations | undefined,
): ToolExecutionClassValue => {
  if (annotations === undefined) return "uncertain";
  if (annotations.readOnlyHint) return "readonly";
  if (annotations.idempotentHint && !annotations.destructiveHint) return "idempotent";

  return "uncertain";
};

const makeMcpTool = (tool: McpSchema.Tool, trustToolAnnotations: boolean): McpTool => {
  let dynamic: McpTool = Tool.dynamic(tool.name, {
    ...(tool.description === undefined ? {} : { description: tool.description }),
    parameters: tool.inputSchema,
    success: McpToolResult,
    failure: McpToolCallFailed,
    // MCP reports tool errors inside results so the model can self-correct;
    // `return` hands the typed failure to the model the same way.
    failureMode: "return",
  });
  const title = tool.title ?? tool.annotations?.title;

  if (title !== undefined) dynamic = dynamic.annotate(Tool.Title, title);
  if (tool.outputSchema !== undefined) {
    dynamic = dynamic.annotate(McpToolOutputSchema, tool.outputSchema);
  }
  if (tool.annotations !== undefined) {
    dynamic = dynamic
      .annotate(Tool.Readonly, tool.annotations.readOnlyHint)
      .annotate(Tool.Destructive, tool.annotations.destructiveHint)
      .annotate(Tool.Idempotent, tool.annotations.idempotentHint)
      .annotate(Tool.OpenWorld, tool.annotations.openWorldHint);
  }
  if (trustToolAnnotations) {
    dynamic = dynamic.annotate(ToolExecutionClass, executionClassFor(tool.annotations));
  }

  return dynamic;
};

const ToolArguments = Schema.Record(Schema.String, Schema.Json);
const decodeToolArguments = Schema.decodeUnknownEffect(ToolArguments);

const errorText = (content: ReadonlyArray<typeof McpSchema.ContentBlock.Type>): string =>
  content
    .flatMap((block) => (block.type === "text" ? [block.text] : []))
    .join("\n")
    .slice(0, 4 * 1024);

const connectionError = (serverId: string, message: string) => (cause: unknown) =>
  McpConnectionError.make({ serverId, message, cause });

/**
 * Keeps every non-interrupt outcome of one protocol step typed: expected
 * failures and defects (such as an undecodable server reply) both become the
 * connection error, so a misbehaving server cannot crash the connector.
 */
const failClosed =
  (serverId: string, message: string) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, McpConnectionError, R> =>
    self.pipe(
      Effect.mapError(connectionError(serverId, message)),
      Effect.catchDefect((defect) => Effect.fail(connectionError(serverId, message)(defect))),
    );

const connectServer = Effect.fn("McpClient.connect")(function* <R>(
  transport: McpServerTransport<R>,
  request: McpConnectionRequest,
  clientInfo: McpSchema.Implementation,
): Effect.fn.Return<McpConnectedServer, McpConnectionError, Scope.Scope | R> {
  const serverId = transport.serverId;
  const protocol = yield* transport.protocol;
  const client = yield* RpcClient.make(McpSchema.ClientRpcs, {
    spanPrefix: "McpClient",
    spanAttributes: { "mcp.server_id": serverId },
  }).pipe(Effect.provideService(RpcClient.Protocol, protocol));
  const initialized = yield* client
    .initialize({
      protocolVersion: OFFERED_PROTOCOL_VERSION,
      capabilities: McpSchema.ClientCapabilities.make({}),
      clientInfo,
    })
    .pipe(failClosed(serverId, `MCP initialize with ${serverId} failed`));
  const negotiated = yield* Schema.decodeUnknownEffect(McpClientProtocolVersion)(
    initialized.protocolVersion,
  ).pipe(
    Effect.mapError(
      connectionError(
        serverId,
        `MCP server ${serverId} negotiated unsupported protocol version '${initialized.protocolVersion}'`,
      ),
    ),
  );

  yield* Effect.annotateCurrentSpan({ "mcp.protocol_version": negotiated });
  yield* client["notifications/initialized"](undefined, { discard: true }).pipe(
    failClosed(serverId, `Could not acknowledge MCP initialization with ${serverId}`),
  );

  const tools: Array<McpSchema.Tool> = [];
  let cursor: string | undefined;
  let pages = 0;

  do {
    const page = yield* client["tools/list"](
      cursor === undefined ? undefined : McpSchema.PaginatedRequestMeta.make({ cursor }),
    ).pipe(failClosed(serverId, `MCP tools/list with ${serverId} failed`));

    tools.push(...page.tools);
    cursor = page.nextCursor;
    pages += 1;
    // `validateMcpDiscovery` rejects the count; stop paying for more pages.
    if (tools.length > request.maxToolCount) break;
    if (cursor !== undefined && pages >= MAX_TOOL_LIST_PAGES) {
      return yield* McpConnectionError.make({
        serverId,
        message: `MCP server ${serverId} paginated tools/list beyond ${MAX_TOOL_LIST_PAGES} pages`,
      });
    }
  } while (cursor !== undefined);

  const callTool = Effect.fn("McpClient.callTool")(function* (name: string, params: unknown) {
    const args = yield* decodeToolArguments(params ?? {}).pipe(
      Effect.mapError((cause) =>
        McpToolCallFailed.make({
          serverId,
          tool: name,
          reason: "invalid-arguments",
          message: `MCP tool arguments must be a JSON object: ${cause.message}`,
        }),
      ),
    );
    const result = yield* client["tools/call"]({ name, arguments: args }).pipe(
      Effect.mapError((error) =>
        McpToolCallFailed.make({
          serverId,
          tool: name,
          reason: isRpcClientError(error) ? "transport" : "protocol-error",
          message: error.message,
        }),
      ),
    );

    if (result.isError === true) {
      return yield* McpToolCallFailed.make({
        serverId,
        tool: name,
        reason: "tool-error",
        message: errorText(result.content),
        content: result.content,
      });
    }

    return McpToolResult.make({
      content: result.content,
      ...(result.structuredContent === undefined
        ? {}
        : { structuredContent: result.structuredContent }),
    });
  });

  const toolkit: McpToolkit = Toolkit.make(
    ...tools.map((tool) => makeMcpTool(tool, transport.trustToolAnnotations)),
  );
  const handlers = toolkit.toLayer(
    Object.fromEntries(
      tools.map((tool) => [tool.name, (params: unknown) => callTool(tool.name, params)] as const),
    ),
  );

  return {
    identity: McpServerIdentity.make({ serverId, implementation: initialized.serverInfo }),
    capabilities: initialized.capabilities,
    tools,
    toolkit,
    handlers,
  } satisfies McpConnectedServer;
});

/**
 * `McpConnector` over real transports. Each `connect` acquires the transport
 * in the caller's Scope, negotiates a protocol revision, lists tools within the
 * request bounds, and returns dynamic Effect AI Tools plus the handler Layer
 * that forwards their calls. Platform services stay visible in `R`.
 */
export const McpClient = {
  layer: <R = never>(
    transports: ReadonlyArray<McpServerTransport<R>>,
    options?: { readonly clientInfo?: McpSchema.Implementation | undefined },
  ): Layer.Layer<McpConnector, never, R> =>
    Layer.effect(McpConnector)(
      Effect.gen(function* () {
        const services = yield* Effect.context<R>();
        const byServerId = new Map(transports.map((transport) => [transport.serverId, transport]));
        const clientInfo = options?.clientInfo ?? defaultMcpClientInfo;

        return McpConnector.of({
          connect: (request) => {
            const transport = byServerId.get(request.serverId);

            if (transport === undefined) {
              return Effect.fail(
                McpConnectionError.make({
                  serverId: request.serverId,
                  message: `No MCP transport is configured for server '${request.serverId}'`,
                }),
              );
            }

            return connectServer(transport, request, clientInfo).pipe(Effect.provide(services));
          },
        });
      }),
    ),
} as const;
