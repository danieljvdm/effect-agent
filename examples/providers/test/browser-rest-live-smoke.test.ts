import { browserRestCaptureLayer } from "@effect-agent/platform-cloudflare/browser-rest-capture";
import {
  CapturePageMarkdown,
  PageCapture,
  PageCaptureLimits,
  PageCaptureRequest,
  PageUrlTarget,
} from "@effect-agent/sandbox";
import { phase7LiveProfileEnabled } from "@effect-agent/testing";
import { Config, Effect } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { describe, expect, it } from "vite-plus/test";

const ACCOUNT = "CLOUDFLARE_ACCOUNT_ID";
const TOKEN = "CLOUDFLARE_API_TOKEN";
const enabled =
  phase7LiveProfileEnabled(process.env) && !!process.env[ACCOUNT] && !!process.env[TOKEN];

describe.skipIf(!enabled)("Browser Run REST Kitesurf smoke (opt-in)", () => {
  it("captures bounded Markdown through the production REST Layer", { timeout: 90_000 }, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const accountId = yield* Config.nonEmptyString(ACCOUNT);
        const apiToken = yield* Config.redacted(TOKEN);
        return yield* Effect.gen(function* () {
          const capture = yield* PageCapture;
          const result = yield* capture.capture(
            PageCaptureRequest.make({
              target: PageUrlTarget.make({ url: "https://example.com/" }),
              action: CapturePageMarkdown.make({}),
              engine: "kitesurf",
              limits: PageCaptureLimits.make({ maxOutputBytes: 16 * 1_024 }),
            }),
          );
          expect(result.output._tag).toBe("PageMarkdownCaptured");
          if (result.output._tag === "PageMarkdownCaptured") {
            expect(result.output.markdown).toContain("Example Domain");
          }
        }).pipe(Effect.provide(browserRestCaptureLayer({ accountId, apiToken })));
      }).pipe(Effect.provide(FetchHttpClient.layer)),
    ),
  );
});
