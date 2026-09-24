import { expect, it } from "vite-plus/test";

import { diagnosticDetail } from "../src/server/diagnostics.ts";

it("retains safe nested metadata without evaluating getters or exposing reasoning and credentials", () => {
  let evaluated = false;

  const error = new Error("provider failed", {
    cause: { type: "reasoning", text: "PRIVATE REASONING" },
  });

  Object.defineProperty(error, "dangerous", {
    get() {
      evaluated = true;
      throw new Error("getter");
    },
  });

  const detail = diagnosticDetail({
    error,
    response: {
      headers: { "x-request-id": "req-1", cookie: "PRIVATE COOKIE" },
      body: '{"token":"PRIVATE TOKEN","error":"timeout"}',
      sessionToken: "PRIVATE SESSION",
      assertion: "PRIVATE ASSERTION",
      url: "https://example.com/page?q=hotel&X-Amz-Security-Token=PRIVATE&Policy=PRIVATE",
      usage: { inputTokens: 120 },
    },
  });

  expect(detail.text).toContain("provider failed");
  expect(detail.text).not.toContain("PRIVATE");
  expect(evaluated).toBe(false);
});
