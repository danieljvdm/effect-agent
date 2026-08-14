import {
  McpConnectionRequest,
  McpConnector,
  McpServerIdentity,
  McpToolkitMismatch,
  type McpConnection,
} from "@effect-agent/capabilities";
import { Effect, Layer, Schema } from "effect";
import { Tool } from "effect/unstable/ai";
import * as McpSchema from "effect/unstable/ai/McpSchema";

import { DocContentToolkit, FetchDocument } from "./definition.ts";

// ---------------------------------------------------------------------------
// Scripted MCP fixture: a deterministic `McpConnector` adapter that serves the
// doc-summarizer's content tool. Discovery entries are DERIVED from the
// authored Tool (`Tool.getJsonSchema`), so `validateMcpDiscovery` digesting
// both sides is a real check, not a tautology; the mismatch and over-limit
// connectors below serve deliberately wrong contracts so tests can pin the
// fail-closed paths (CAP-009, SEC-013).
// ---------------------------------------------------------------------------

/** Framework-side hard bounds one docs-researcher assembly requests. */
export const docsMcpRequest = McpConnectionRequest.make({
  serverId: "docs-content-mcp",
  maxToolCount: 4,
  maxToolDescriptionBytes: 256,
  maxDiscoveryBytes: 16_384,
  connectTimeoutMillis: 1_000,
});

export const docsMcpIdentity = McpServerIdentity.make({
  serverId: docsMcpRequest.serverId,
  implementation: McpSchema.Implementation.make({
    name: "docs-researcher-content-fixture",
    version: "1.0.0",
  }),
});

const discoveredFetchDocument = McpSchema.Tool.make({
  name: FetchDocument.name,
  description: "Fetch one bounded research document by its identifier.",
  inputSchema: Tool.getJsonSchema(FetchDocument),
});

const scriptedConnector = (tools: ReadonlyArray<McpSchema.Tool>): Layer.Layer<McpConnector> =>
  Layer.succeed(McpConnector)({
    connect: () =>
      Effect.acquireRelease(
        Effect.succeed({
          identity: docsMcpIdentity,
          capabilities: McpSchema.ServerCapabilities.make({}),
          tools,
          toolkit: DocContentToolkit,
        }),
        () => Effect.void,
      ),
  });

/** The well-behaved scripted content server. */
export const docsMcpConnectorLayer: Layer.Layer<McpConnector> = scriptedConnector([
  discoveredFetchDocument,
]);

/** Serves a tool description exceeding `maxToolDescriptionBytes` (SEC-013 bound). */
export const docsMcpOversizedConnectorLayer: Layer.Layer<McpConnector> = scriptedConnector([
  McpSchema.Tool.make({
    name: discoveredFetchDocument.name,
    description: "x".repeat(1_024),
    inputSchema: discoveredFetchDocument.inputSchema,
  }),
]);

/** Serves a discovery schema that disagrees with the authored toolkit (drift fails closed). */
export const docsMcpMismatchedConnectorLayer: Layer.Layer<McpConnector> = scriptedConnector([
  McpSchema.Tool.make({
    name: discoveredFetchDocument.name,
    description: discoveredFetchDocument.description,
    inputSchema: { type: "object", properties: { url: { type: "string" } } },
  }),
]);

const isJsonEqual = (left: unknown, right: unknown): boolean =>
  JSON.stringify(left) === JSON.stringify(right);

/**
 * Bind DISCOVERY to AUTHORING: `validateMcpDiscovery` (inside `connectMcp`)
 * already proved the served discovery matches the connection's own Toolkit;
 * this check additionally proves that Toolkit is the exact toolkit the
 * doc-summarizer was AUTHORED against — same tool names, same derived JSON
 * schemas — so a connector cannot substitute a look-alike toolkit. The
 * docs-researcher harness runs it before any worker Binding registration and
 * fails closed on any drift.
 */
export const assertDiscoveryMatchesAuthoredToolkit = Effect.fn(
  "DocsResearcher.assertDiscoveryMatchesAuthoredToolkit",
)(function* (connection: McpConnection): Effect.fn.Return<void, McpToolkitMismatch> {
  const authored = Object.values(DocContentToolkit.tools)
    .map((tool) => ({ name: tool.name, inputSchema: Tool.getJsonSchema(tool) }))
    .sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));
  const discovered = Object.values(connection.toolkit.tools)
    .map((tool) => ({ name: tool.name, inputSchema: Tool.getJsonSchema(tool) }))
    .sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));
  const matches =
    authored.length === discovered.length &&
    authored.every(
      (tool, index) =>
        tool.name === discovered[index]?.name &&
        isJsonEqual(tool.inputSchema, discovered[index]?.inputSchema),
    );
  if (!matches) {
    return yield* McpToolkitMismatch.make({
      serverId: connection.discovery.identity.serverId,
      message:
        "The MCP-discovered toolkit does not match the doc-summarizer's authored content toolkit",
    });
  }
});

/** Round-trip guard for encoded discovery values persisted as fixture evidence. */
export const DocsMcpDiscoveryEvidence = Schema.Struct({
  serverId: Schema.NonEmptyString,
  toolCount: Schema.Natural,
  encodedBytes: Schema.Natural,
  toolkitSchemaDigest: Schema.String,
});
