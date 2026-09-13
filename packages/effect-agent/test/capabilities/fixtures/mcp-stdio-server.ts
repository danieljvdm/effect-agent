/**
 * Minimal MCP server on stdio, built from Effect AI's native `McpServer`, for
 * the stdio transport test. Run with
 * `node --experimental-transform-types packages/capabilities/test/fixtures/mcp-stdio-server.ts`.
 * Logs go to stderr so stdout stays a clean JSON-RPC channel.
 */
import { NodeRuntime, NodeStdio } from "@effect/platform-node";
import { Effect, Layer, Logger, Schema } from "effect";
import { McpProtocol, McpServer, Tool, Toolkit } from "effect/unstable/ai";

const Echo = Tool.make("echo", {
  description: "Echo a message back to the caller.",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.Struct({ echoed: Schema.String }),
}).annotate(Tool.Readonly, true);

const ServerLayer = McpServer.toolkit(Toolkit.make(Echo)).pipe(
  Layer.provideMerge(
    Toolkit.make(Echo).toLayer({
      echo: ({ message }) => Effect.succeed({ echoed: message }),
    }),
  ),
  Layer.provide(
    McpServer.layerStdio({
      name: "effect-agent-stdio-fixture",
      version: "1.0.0",
      protocols: [McpProtocol.v2025_11_25],
    }),
  ),
  Layer.provide(NodeStdio.layer),
  Layer.provide(Logger.layer([Logger.consolePretty({ stderr: true })])),
);

Layer.launch(ServerLayer).pipe(NodeRuntime.runMain);
