import { Context, Crypto, Duration, Effect, Encoding, Schema } from "effect";
import { Tool, type Toolkit } from "effect/unstable/ai";
import * as McpSchema from "effect/unstable/ai/McpSchema";

const MAX_MCP_TOOLS = 128;
const MAX_MCP_DISCOVERY_BYTES = 1024 * 1024;
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const Sha256Digest = Schema.String.check(Schema.isPattern(/^sha256:[a-f0-9]{64}$/));

/** Server identity copied directly from Effect AI's native MCP initialization schema. */
export class McpServerIdentity extends Schema.Class<McpServerIdentity>(
  "@effect-agent/capabilities/McpServerIdentity",
)({
  serverId: Schema.NonEmptyString,
  implementation: McpSchema.Implementation,
}) {}

/** Requested hard limits for an adapter-owned MCP connection and discovery response. */
export class McpConnectionRequest extends Schema.Class<McpConnectionRequest>(
  "@effect-agent/capabilities/McpConnectionRequest",
)({
  serverId: Schema.NonEmptyString,
  maxToolCount: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_MCP_TOOLS)),
  maxToolDescriptionBytes: PositiveInt,
  maxDiscoveryBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_MCP_DISCOVERY_BYTES)),
  connectTimeoutMillis: PositiveInt,
}) {}

/** Typed remote connection failure; no remote execution is claimed exactly-once. */
export class McpConnectionError extends Schema.TaggedErrorClass<McpConnectionError>()(
  "McpConnectionError",
  { serverId: Schema.NonEmptyString, message: Schema.String },
) {}

/** Discovery exceeded an explicit caller or framework hard bound. */
export class McpDiscoveryLimitExceeded extends Schema.TaggedErrorClass<McpDiscoveryLimitExceeded>()(
  "McpDiscoveryLimitExceeded",
  {
    serverId: Schema.NonEmptyString,
    limit: Schema.Literals(["tool-count", "tool-description-bytes", "discovery-bytes"]),
    limitValue: Schema.Natural,
    observedValue: Schema.Natural,
  },
) {}

/** Native dynamic Toolkit names did not match the bounded MCP discovery response. */
export class McpToolkitMismatch extends Schema.TaggedErrorClass<McpToolkitMismatch>()(
  "McpToolkitMismatch",
  {
    serverId: Schema.NonEmptyString,
    message: Schema.String,
  },
) {}

/** Validated discovery metadata; native Effect AI MCP values retain provider detail. */
export class McpDiscovery extends Schema.Class<McpDiscovery>(
  "@effect-agent/capabilities/McpDiscovery",
)({
  identity: McpServerIdentity,
  capabilities: McpSchema.ServerCapabilities,
  tools: Schema.Array(McpSchema.Tool).check(Schema.isMaxLength(MAX_MCP_TOOLS)),
  encodedBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_MCP_DISCOVERY_BYTES)),
  toolkitSchemaDigest: Sha256Digest,
}) {}

/** Raw adapter result before the framework validates count and byte limits. */
export interface McpConnectedServer {
  readonly identity: McpServerIdentity;
  readonly capabilities: McpSchema.ServerCapabilities;
  readonly tools: ReadonlyArray<McpSchema.Tool>;
  readonly toolkit: Toolkit.Any;
}

/** Scoped validated MCP connection with native Effect AI dynamic Toolkit values. */
export interface McpConnection {
  readonly discovery: McpDiscovery;
  readonly toolkit: Toolkit.Any;
}

/** Adapter port for native Effect AI MCP protocol/schema integration. */
export class McpConnector extends Context.Service<
  McpConnector,
  {
    readonly connect: (
      request: McpConnectionRequest,
    ) => Effect.Effect<McpConnectedServer, McpConnectionError, import("effect").Scope.Scope>;
  }
>()("@effect-agent/capabilities/McpConnector") {}

const encodedBytes = (value: string): number => Encoding.encodeHex(value).length / 2;

const canonicalJson = (value: unknown): unknown => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return value.map(canonicalJson);
  if (typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const item = (value as Record<string, unknown>)[key];
      if (item !== undefined) output[key] = canonicalJson(item);
    }
    return output;
  }
  throw new TypeError("MCP schema contains a non-JSON value");
};

const utf8 = (value: string): Uint8Array => {
  const hex = Encoding.encodeHex(value);
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
};

const digestJson = Effect.fn("digestMcpSchema")(function* (serverId: string, value: unknown) {
  const encoded = yield* Effect.try({
    try: () => JSON.stringify(canonicalJson(value)),
    catch: () =>
      McpToolkitMismatch.make({
        serverId,
        message: "MCP Tool schema is not canonical JSON",
      }),
  });
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto.digest("SHA-256", utf8(encoded)).pipe(
    Effect.mapError(() =>
      McpToolkitMismatch.make({
        serverId,
        message: "Could not hash MCP Tool schemas",
      }),
    ),
  );
  return `sha256:${Encoding.encodeHex(digest)}`;
});

/** Validate native discovery data before it enters Agent Tool context. */
export const validateMcpDiscovery = Effect.fn("validateMcpDiscovery")(function* (
  request: McpConnectionRequest,
  server: McpConnectedServer,
) {
  if (server.identity.serverId !== request.serverId) {
    return yield* McpToolkitMismatch.make({
      serverId: request.serverId,
      message: `Connected server identity '${server.identity.serverId}' does not match the requested server`,
    });
  }
  if (server.tools.length > request.maxToolCount) {
    return yield* McpDiscoveryLimitExceeded.make({
      serverId: request.serverId,
      limit: "tool-count",
      limitValue: request.maxToolCount,
      observedValue: server.tools.length,
    });
  }
  for (const tool of server.tools) {
    const descriptionBytes = encodedBytes(tool.description ?? "");
    if (descriptionBytes > request.maxToolDescriptionBytes) {
      return yield* McpDiscoveryLimitExceeded.make({
        serverId: request.serverId,
        limit: "tool-description-bytes",
        limitValue: request.maxToolDescriptionBytes,
        observedValue: descriptionBytes,
      });
    }
  }
  const discoveredNames = server.tools.map((tool) => tool.name).sort();
  const toolkitNames = Object.keys(server.toolkit.tools).sort();
  if (
    discoveredNames.length !== toolkitNames.length ||
    discoveredNames.some((name, index) => name !== toolkitNames[index])
  ) {
    return yield* McpToolkitMismatch.make({
      serverId: request.serverId,
      message: "Native Effect AI Toolkit names do not match MCP tool discovery",
    });
  }
  const discoverySchemas = server.tools
    .map((tool) => ({ name: tool.name, inputSchema: tool.inputSchema }))
    .sort((left, right) => left.name.localeCompare(right.name));
  const toolkitSchemas = yield* Effect.try({
    try: () =>
      Object.values(server.toolkit.tools)
        .map((tool) => ({ name: tool.name, inputSchema: Tool.getJsonSchema(tool) }))
        .sort((left, right) => left.name.localeCompare(right.name)),
    catch: () =>
      McpToolkitMismatch.make({
        serverId: request.serverId,
        message: "Could not derive JSON Schema from the native Effect AI Toolkit",
      }),
  });
  const discoveryDigest = yield* digestJson(request.serverId, discoverySchemas);
  const toolkitDigest = yield* digestJson(request.serverId, toolkitSchemas);
  if (discoveryDigest !== toolkitDigest) {
    return yield* McpToolkitMismatch.make({
      serverId: request.serverId,
      message: "Native Effect AI Toolkit schemas do not match MCP tool discovery",
    });
  }
  const encoded = yield* Schema.encodeEffect(
    Schema.Struct({
      identity: McpServerIdentity,
      capabilities: McpSchema.ServerCapabilities,
      tools: Schema.Array(McpSchema.Tool),
    }),
  )({
    identity: server.identity,
    capabilities: server.capabilities,
    tools: server.tools,
  }).pipe(
    Effect.mapError((error) =>
      McpConnectionError.make({
        serverId: request.serverId,
        message: `Could not encode MCP discovery response: ${error.message}`,
      }),
    ),
  );
  const discoveryBytes = encodedBytes(JSON.stringify(encoded));
  if (discoveryBytes > request.maxDiscoveryBytes) {
    return yield* McpDiscoveryLimitExceeded.make({
      serverId: request.serverId,
      limit: "discovery-bytes",
      limitValue: request.maxDiscoveryBytes,
      observedValue: discoveryBytes,
    });
  }
  return McpDiscovery.make({
    identity: server.identity,
    capabilities: server.capabilities,
    tools: server.tools,
    encodedBytes: discoveryBytes,
    toolkitSchemaDigest: toolkitDigest,
  });
});

/**
 * Acquire a native connection in Scope, enforce a Clock-controlled timeout,
 * then validate discovery before exposing its Toolkit.
 */
export const connectMcp = Effect.fn("connectMcp")(function* (request: McpConnectionRequest) {
  const connector = yield* McpConnector;
  const server = yield* connector.connect(request).pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(request.connectTimeoutMillis),
      orElse: () =>
        Effect.fail(
          McpConnectionError.make({
            serverId: request.serverId,
            message: "MCP connection timed out",
          }),
        ),
    }),
  );
  const discovery = yield* validateMcpDiscovery(request, server);
  return {
    discovery,
    toolkit: server.toolkit,
  } satisfies McpConnection;
});
