import { createFileRoute } from "@tanstack/react-router";

/** Raw Start route that delegates the framed response to Effect RPC. */
export const Route = createFileRoute("/api/rpc")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { demoRunRpcWebHandler } = await import("@/demo/run-rpc.server");
        return demoRunRpcWebHandler.handler(request);
      },
    },
  },
});
