import { WebCaptureFailure } from "@effect-agent/capabilities/WebCapture";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import {
  PageCapture,
  PageCaptureNavigationError,
  PageCaptureRateLimitedError,
  PageCaptureResult,
  PageMarkdownCaptured,
  type PageCaptureCapture,
} from "@effect-agent/sandbox/PageCapture";
import { SandboxImplementation } from "@effect-agent/sandbox/Sandbox";
import { it } from "@effect/vitest";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Tool, Toolkit } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";
import { expect, expectTypeOf } from "vite-plus/test";

import {
  ReadTravelPage,
  ReadTravelPageLive,
  ReadTravelPageParameters,
  ReadTravelPageResult,
} from "../src/research.ts";

const implementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "test-capture",
});

const page = (markdown: string) =>
  PageCaptureResult.make({
    implementation,
    output: PageMarkdownCaptured.make({ markdown }),
    resourceUse: {},
  });

const parameters = {
  url: "https://www.tahoegetaways.com/vacation-rentals/example",
  focus: "private hot tub and bedrooms",
};

const read = Effect.fn("test.readTravelPage")(function* (input = parameters) {
  const toolkit = yield* Toolkit.make(ReadTravelPage);
  const results = yield* Stream.runCollect(yield* toolkit.handle("read_travel_page", input));
  const result = results[0];

  if (result === undefined) return yield* Effect.die("The tool returned no result");

  return result;
});

const provideCapture = (capture: PageCaptureCapture) =>
  Effect.provide(ReadTravelPageLive.pipe(Layer.provide(Layer.succeed(PageCapture, { capture }))));

it.effect("keeps deep amenity evidence from a large page within the encoded result budget", () =>
  Effect.gen(function* () {
    const markdown = `# Alpine Cabin\n${"Navigation and photographs.\n".repeat(8_000)}\nPrivate hot tub overlooking the forest. Three bedrooms and two bathrooms.\n${'其他資訊 😀 \\"\n'.repeat(8_000)}`;
    const calls = yield* Ref.make(0);

    const capture: PageCaptureCapture = (request) =>
      Effect.gen(function* () {
        yield* Ref.update(calls, (count) => count + 1);
        expect(request.limits.maxOutputBytes).toBe(512 * 1_024);
        expect(request.navigation).toMatchObject({
          waitUntil: "networkidle2",
          timeoutMillis: 20_000,
        });
        const allowed = request.resourcePolicy?.allowRequestPatterns ?? [];

        for (const url of [
          "https://a0.muscache.com/script.js",
          "https://www.recreation.gov/camping/campgrounds/233389",
          "https://www.visitcostarica.com/",
          "https://cdn.independent-hotel.com/main.js",
        ])
          expect(allowed.some((pattern) => new RegExp(pattern).test(url))).toBe(true);
        for (const denied of [
          "http://www.airbnb.com/",
          "https://127.0.0.1/",
          "https://[::1]/",
          "https://metadata.google.internal/",
          "https://local/",
          "https://user:password@www.airbnb.com/",
          "https://www.airbnb.com:8443/",
        ])
          expect(allowed.some((pattern) => new RegExp(pattern).test(denied))).toBe(false);

        return page(markdown);
      });

    const first = yield* read().pipe(provideCapture(capture));
    const second = yield* read().pipe(provideCapture(capture));

    expect(first.isFailure).toBe(false);
    const result = yield* Schema.decodeUnknownEffect(ReadTravelPageResult)(first.result);

    expect(result.title).toBe("Alpine Cabin");
    expect(result.excerpts.join("\n")).toContain(
      "Private hot tub overlooking the forest. Three bedrooms",
    );
    expect(result.truncated).toBe(true);
    expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThanOrEqual(
      12 * 1_024,
    );
    expect(second.result).toEqual(first.result);
    expect(yield* Ref.get(calls)).toBe(2);
  }),
);

it.effect("inspects newly discovered sites and CDNs without a travel-site allowlist", () =>
  Effect.gen(function* () {
    for (const url of [
      "https://www.recreation.gov/camping/campgrounds/233389",
      "https://www.visitcostarica.com/",
      "https://independent-hotel.example/stays/cabin",
      "https://a0.muscache.com/",
    ]) {
      const requested = yield* Ref.make<string | null>(null);

      const capture: PageCaptureCapture = (request) =>
        Ref.set(
          requested,
          request.target._tag === "PageUrlTarget" ? request.target.url : null,
        ).pipe(Effect.as(page("# A researched place\nUseful details from this source.")));

      const result = yield* read({ ...parameters, url }).pipe(provideCapture(capture));

      expect(result.isFailure).toBe(false);
      expect(yield* Ref.get(requested)).toBe(url);
    }
  }),
);

it.effect("denies unsupported targets before invoking the capture port", () =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);

    const capture: PageCaptureCapture = () =>
      Ref.update(calls, (count) => count + 1).pipe(Effect.as(page("unused")));

    for (const url of [
      "http://www.airbnb.com/rooms/1",
      "https://user:password@www.airbnb.com/rooms/1",
      "https://www.airbnb.com:8443/",
      "https://127.0.0.1/",
      "https://0x7f000001/",
      "https://2130706433/",
      "https://[::1]/",
      "https://[::ffff:127.0.0.1]/",
      "https://localhost/",
      "https://localhost./",
      "https://router.local/",
      "https://metadata.google.internal/",
      "https://unqualified/",
      "not a URL",
    ]) {
      const result = yield* read({ ...parameters, url }).pipe(provideCapture(capture));

      expect(result.isFailure).toBe(true);
      expect(result.result).toMatchObject({ errorTag: "WebCaptureUrlDenied" });
    }
    expect(yield* Ref.get(calls)).toBe(0);
  }),
);

it.effect(
  "extracts only bounded source photos after the title, retaining gallery URLs and amenity evidence",
  () =>
    Effect.gen(function* () {
      const source =
        "https://a0.muscache.com/im/pictures/prohost-api/Hosting-24912220/original/gallery.jpeg?im_w=720&crop=(1,2)";

      const markdown = [
        "![Navigation](https://images.example.com/navigation.jpg)",
        "# Tahoe POV - Hot Tub, Lake Views w Private Beach",
        "![Airbnb logo](https://images.example.com/logo.jpg)",
        "![Host](https://a0.muscache.com/im/pictures/user/host.jpg)",
        "![Untrusted](http://127.0.0.1/private.jpg)",
        "![Untrusted](https://name:secret@images.example.com/private.jpg)",
        "![Untrusted](data:image/png;base64,aaaa)",
        `![Private \\[hot tub\\] and lake](<${source.replaceAll("&", "&amp;")}> "Source caption")`,
        `![Duplicate](<${source}>)`,
        "![](/images/uncaptioned.jpg)",
        ...Array.from(
          { length: 7 },
          (_, index) => `![Living room ${index}](/images/living-${index}.jpg)`,
        ),
        "Navigation and photographs.\n".repeat(8_000),
        "Private hot tub overlooking the forest. Three bedrooms and two bathrooms.",
        "## Meet your host",
        "![Portrait](https://images.example.com/portrait.jpg)",
      ].join("\n");

      const calls = yield* Ref.make(0);

      const result = yield* read().pipe(
        provideCapture(() =>
          Ref.update(calls, (count) => count + 1).pipe(Effect.as(page(markdown))),
        ),
      );

      const inspected = yield* Schema.decodeUnknownEffect(ReadTravelPageResult)(result.result);

      expect(inspected.photos).toHaveLength(4);
      expect(inspected.photos[0]).toEqual({ url: source, caption: "Private [hot tub] and lake" });
      expect(inspected.photos[1]).toEqual({
        url: "https://www.tahoegetaways.com/images/uncaptioned.jpg",
        caption: "Photo from listing",
      });
      expect(inspected.photos[2]?.url).toBe("https://www.tahoegetaways.com/images/living-0.jpg");
      expect(JSON.stringify(inspected.photos)).not.toMatch(
        /navigation|logo|\/user\/|portrait|private\.jpg/,
      );
      expect(inspected.excerpts.join("\n")).toContain("Private hot tub overlooking the forest");
      expect(new TextEncoder().encode(JSON.stringify(inspected)).byteLength).toBeLessThanOrEqual(
        12 * 1_024,
      );
      expect(yield* Ref.get(calls)).toBe(1);
    }),
);

it.effect("returns typed access failures and preserves the provider rate-limit hint", () =>
  Effect.gen(function* () {
    for (const markdown of [
      "# Verify you are human\nComplete the security check.",
      "# Error 403 Forbidden\nForbidden\nVarnish cache server",
      "# 403 Forbidden\nYou do not have access to this resource.",
      "# Page not found\nWe're sorry, we can't find the page you're looking for.\nError code: 404",
    ]) {
      const blocked = yield* read().pipe(provideCapture(() => Effect.succeed(page(markdown))));

      expect(blocked).toMatchObject({
        isFailure: true,
        result: { errorTag: "WebCapturePageUnavailable" },
      });
    }

    const inaccessible = yield* read().pipe(
      provideCapture(() =>
        Effect.fail(PageCaptureNavigationError.make({ implementation, message: "HTTP 403" })),
      ),
    );

    expect(inaccessible).toMatchObject({
      isFailure: true,
      result: { errorTag: "PageCaptureNavigationError" },
    });

    const limited = yield* read().pipe(
      provideCapture(() =>
        Effect.fail(
          PageCaptureRateLimitedError.make({
            implementation,
            reason: "rate",
            retryAfterMillis: 12_000,
            message: "Rate limited",
          }),
        ),
      ),
    );

    expect(limited).toMatchObject({
      isFailure: true,
      result: { errorTag: "PageCaptureRateLimitedError", retryAfterMillis: 12_000 },
    });

    const small = yield* read().pipe(
      provideCapture(() => Effect.succeed(page("# Cabin\nPrivate hot tub."))),
    );

    expect(small.result).toMatchObject({ title: "Cabin", truncated: false });
  }),
);

it.effect("times out a stalled capture and completes its finalizer", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const closed = yield* Ref.make(false);

    const capture: PageCaptureCapture = () =>
      Effect.acquireUseRelease(
        Deferred.succeed(entered, undefined),
        () => Effect.never,
        () => Ref.set(closed, true),
      );

    const fiber = yield* read().pipe(provideCapture(capture), Effect.forkChild);

    yield* Deferred.await(entered);
    yield* TestClock.adjust("26 seconds");
    expect(yield* Fiber.join(fiber)).toMatchObject({
      isFailure: true,
      result: { errorTag: "WebCaptureTimeout" },
    });
    expect(yield* Ref.get(closed)).toBe(true);
  }),
);

it.effect("preserves interruption and defects while completing capture cleanup", () =>
  Effect.gen(function* () {
    for (const mode of ["interrupt", "defect"] as const) {
      const entered = yield* Deferred.make<void>();
      const closed = yield* Ref.make(false);

      const capture: PageCaptureCapture = () =>
        Effect.acquireUseRelease(
          Deferred.succeed(entered, undefined),
          () => (mode === "defect" ? Effect.die("capture defect") : Effect.never),
          () => Ref.set(closed, true),
        );

      const fiber = yield* read().pipe(provideCapture(capture), Effect.exit, Effect.forkChild);

      yield* Deferred.await(entered);
      if (mode === "interrupt") yield* Fiber.interrupt(fiber);
      else expect(Exit.isFailure(yield* Fiber.join(fiber))).toBe(true);
      expect(yield* Ref.get(closed)).toBe(true);
    }
  }),
);

it("keeps native tool schemas and the capture dependency explicit", () => {
  expectTypeOf<Layer.Services<typeof ReadTravelPageLive>>().toEqualTypeOf<PageCapture>();
  expect(ReadTravelPage.failureSchema).toBe(WebCaptureFailure);
  expect(Context.get(ReadTravelPage.annotations, ToolExecutionClass)).toBe("uncertain");
  expect(Context.get(ReadTravelPage.annotations, Tool.Readonly)).toBe(false);
  expect(Tool.getJsonSchema(ReadTravelPage, { transformer: toCodecOpenAI })).toMatchObject({
    type: "object",
  });
  expect(Schema.is(ReadTravelPageParameters)({ url: parameters.url })).toBe(false);
});
