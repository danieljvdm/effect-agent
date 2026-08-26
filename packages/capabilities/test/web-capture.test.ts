import { Agent, AgentPolicy, ConversationId, IdGenerator, RunId, TurnId } from "@effect-agent/core";
import { AgentRuntime, ToolExecutionClass } from "@effect-agent/engine";
import {
  PageCapture,
  PageCaptureInferencePolicyError,
  PageCaptureProtocolError,
  PageCaptureRateLimitedError,
  PageCaptureResult,
  PageLinksCaptured,
  PageMarkdownCaptured,
  PageStructuredCaptured,
  SandboxImplementation,
  type PageCaptureError,
  type PageCaptureRequest,
} from "@effect-agent/sandbox";
import { describe, expect, it, layer } from "@effect/vitest";
import { Context, Effect, Layer, Ref, Schema, SchemaGetter, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

import type { WebCaptureFailure, WebCaptureSuccess } from "../src/index.ts";
import { CodeMode, WebCapture } from "../src/index.ts";

describe("WebCapture construction", () => {
  it("classifies browser JavaScript as uncertain and excludes capture from readonly Code Mode", () => {
    const definition = WebCapture.make("read_webpage", {
      description: "Read documentation pages.",
      urls: ["docs.example.com", "*.Effect.website"],
      actions: ["markdown", "links"],
    });
    expect(Context.get(definition.tool.annotations, Tool.Readonly)).toBe(false);
    expect(Context.get(definition.tool.annotations, ToolExecutionClass)).toBe("uncertain");
    expect(definition.tool.failureMode).toBe("return");
    expect(definition.urlPatterns).toEqual(["docs.example.com", "*.effect.website"]);
    expect(definition.engine).toBe("chromium");
    expect(definition.description).toContain("Allowed actions: markdown, links");
    expect(definition.description).toContain("docs.example.com");
    expect(() =>
      CodeMode.make("run_javascript", {
        description: "Read a page through Code Mode",
        tools: { browser: { capture: definition.tool } },
      }),
    ).toThrow(/uncertain/);
  });

  it("snapshots and freezes security-sensitive host and action policies", () => {
    const urls = ["docs.example.com"];
    const actions: Array<"markdown" | "content" | "links"> = ["markdown"];
    const definition = WebCapture.make("read_webpage", {
      description: "Read documentation pages.",
      urls,
      actions,
    });

    urls.push("attacker.example");
    actions.push("links");

    expect(definition.urlPatterns).toEqual(["docs.example.com"]);
    expect(definition.actions).toEqual(["markdown"]);
    expect(Object.isFrozen(definition.urlPatterns)).toBe(true);
    expect(Object.isFrozen(definition.actions)).toBe(true);
    expect(() =>
      Reflect.apply(Array.prototype.push, definition.urlPatterns, ["attacker.example"]),
    ).toThrow(/extensible|read.only|frozen/i);
    expect(() => Reflect.apply(Array.prototype.push, definition.actions, ["links"])).toThrow(
      /extensible|read.only|frozen/i,
    );
  });

  it("fails closed on an empty or malformed host allowlist", () => {
    expect(() => WebCapture.make("read_webpage", { description: "d", urls: [] })).toThrow(
      /deny-by-default/,
    );
    for (const pattern of [
      "https://docs.example.com",
      "docs.example.com/path",
      "docs.example.com:8443",
      "*",
      "example",
    ]) {
      expect(() => WebCapture.make("read_webpage", { description: "d", urls: [pattern] })).toThrow(
        /bare host name/,
      );
    }
  });

  it("fails closed on an invalid response byte budget or empty action set", () => {
    for (const bytes of [0, 512, 2 * 1024 * 1024, Number.NaN, 1.5]) {
      expect(() =>
        WebCapture.make("read_webpage", {
          description: "d",
          urls: ["docs.example.com"],
          maxResponseBytes: bytes,
        }),
      ).toThrow(/maxResponseBytes/);
    }
    expect(() =>
      WebCapture.make("read_webpage", {
        description: "d",
        urls: ["docs.example.com"],
        actions: [],
      }),
    ).toThrow(/at least one/);
  });

  it("derives the extraction response format from the Effect Schema at construction", () => {
    const definition = WebCapture.makeExtract("extract_pricing", {
      description: "Extract pricing plans.",
      urls: ["docs.example.com"],
      schema: Schema.Struct({
        plans: Schema.Array(Schema.Struct({ name: Schema.String, monthlyUsd: Schema.Number })),
      }),
    });
    expect(definition.responseFormat).toMatchObject({ type: "object" });
    expect(JSON.stringify(definition.responseFormat)).toContain("monthlyUsd");
    expect(definition.tool.failureMode).toBe("return");
    expect(Context.get(definition.tool.annotations, Tool.Readonly)).toBe(false);
    expect(Context.get(definition.tool.annotations, ToolExecutionClass)).toBe("uncertain");
    expect(Object.isFrozen(definition.urlPatterns)).toBe(true);
  });

  it("rejects extraction schemas that cannot produce a bounded object response format", () => {
    expect(() =>
      WebCapture.makeExtract("extract_text", {
        description: "Extract text.",
        urls: ["docs.example.com"],
        schema: Schema.String,
      }),
    ).toThrow(/bounded object JSON response format/);
  });
});

// ---------------------------------------------------------------------------
// Handler behavior through a full Run with a scripted PageCapture port and a
// scripted model, so the URL policy, request projection, typed failure
// envelopes, and schema decoding are observable at the public seam.
// ---------------------------------------------------------------------------

const scriptedImplementation = SandboxImplementation.make({
  isolation: "unisolated",
  identity: "scripted-page-capture",
});

const PricingSchema = Schema.Struct({
  plans: Schema.Array(Schema.Struct({ name: Schema.String, monthlyUsd: Schema.Number })),
});

class ExtractionDecoderService extends Context.Service<
  ExtractionDecoderService,
  { readonly normalize: (value: string) => string }
>()("@effect-agent/capabilities/test/ExtractionDecoderService") {}

const ServicePricingSchema = Schema.Struct({
  plans: Schema.Array(
    Schema.Struct({
      name: Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.transformOrFail((value) =>
            Effect.map(ExtractionDecoderService, (service) => service.normalize(value)),
          ),
          encode: SchemaGetter.transform((value) => value),
        }),
      ),
      monthlyUsd: Schema.Number,
    }),
  ),
});

/**
 * A scripted PageCapture: the target URL's path selects the outcome so each
 * scenario controls the port without a real browser.
 */
const makeScriptedPort = Effect.gen(function* () {
  const requests = yield* Ref.make<ReadonlyArray<PageCaptureRequest>>([]);
  const port = Layer.succeed(PageCapture)(
    PageCapture.of({
      capture: (request) =>
        Ref.update(requests, (all) => [...all, request]).pipe(
          Effect.flatMap((): Effect.Effect<PageCaptureResult, PageCaptureError> => {
            const url = request.target._tag === "PageUrlTarget" ? request.target.url : "";
            if (url.includes("/rate-limited")) {
              return Effect.fail(
                PageCaptureRateLimitedError.make({
                  implementation: scriptedImplementation,
                  reason: "rate",
                  retryAfterMillis: 7_000,
                  message: "429 Too many requests",
                }),
              );
            }
            if (url.includes("/protocol-failure")) {
              return Effect.fail(
                PageCaptureProtocolError.make({
                  implementation: scriptedImplementation,
                  message: "The browser RPC failed",
                  cause: new Error("secret-token=host-only-diagnostic"),
                }),
              );
            }
            if (url.includes("/inference-policy")) {
              return Effect.fail(
                PageCaptureInferencePolicyError.make({
                  implementation: scriptedImplementation,
                  provider: "cloudflare-workers-ai",
                  reason: "authorization",
                  message: "Workers AI extraction was not authorized",
                  cause: new Error("secret tenant budget detail"),
                }),
              );
            }
            const finish = (output: PageCaptureResult["output"]) =>
              Effect.succeed(
                PageCaptureResult.make({
                  implementation: scriptedImplementation,
                  output,
                  resourceUse: { browserMillis: 42 },
                }),
              );
            if (url.includes("/mismatch")) {
              return finish(PageLinksCaptured.make({ links: ["https://docs.example.com/a"] }));
            }
            switch (request.action._tag) {
              case "CapturePageMarkdown": {
                return finish(PageMarkdownCaptured.make({ markdown: "# Pricing" }));
              }
              case "CapturePageLinks": {
                return finish(
                  PageLinksCaptured.make({ links: ["https://docs.example.com/plans"] }),
                );
              }
              case "CapturePageStructured": {
                return finish(
                  PageStructuredCaptured.make({
                    value: url.includes("/malformed")
                      ? {
                          plans: [{ name: "Pro", monthlyUsd: "secret-token=attacker-controlled" }],
                        }
                      : { plans: [{ name: "Pro", monthlyUsd: 20 }] },
                  }),
                );
              }
              case "CapturePageContent": {
                return finish(PageMarkdownCaptured.make({ markdown: "unused" }));
              }
            }
          }),
        ),
    }),
  );
  return { requests, port };
});

const usage = { inputTokens: {}, outputTokens: {} };

const identifiers = Layer.succeed(IdGenerator, {
  nextConversationId: Effect.succeed(Schema.decodeSync(ConversationId)("conversation-web-capture")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-web-capture")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-web-capture")),
});

interface ToolResultPart {
  readonly result: unknown;
  readonly isFailure: boolean;
}

/** One scripted Run: turn 0 declares the tool call, turn 1 answers. */
const scriptedModel = (
  toolName: string,
  params: Record<string, unknown>,
  toolResults: Ref.Ref<ReadonlyArray<ToolResultPart>>,
) =>
  Model.make(
    "scripted",
    "web-capture",
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
                  Ref.update(toolResults, (all) => [
                    ...all,
                    ...prompt.content
                      .filter((message) => message.role === "tool")
                      .flatMap((message) => message.content)
                      .filter((part) => part.type === "tool-result")
                      .map((part) => ({
                        result: part.result,
                        isFailure: part.isFailure === true,
                      })),
                  ]),
                ),
                Effect.map((value) =>
                  Stream.fromIterable<Response.StreamPartEncoded>(
                    value === 0
                      ? [
                          {
                            type: "tool-call",
                            id: "capture-1",
                            name: toolName,
                            params,
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

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 2,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

interface ScenarioOutcome {
  readonly toolResults: ReadonlyArray<ToolResultPart>;
  readonly requests: ReadonlyArray<PageCaptureRequest>;
}

const runCapture = (
  params: Record<string, unknown>,
  options?: { readonly actions?: ReadonlyArray<"markdown" | "content" | "links"> },
): Effect.Effect<ScenarioOutcome, unknown, IdGenerator> =>
  Effect.gen(function* () {
    const definition = WebCapture.make("read_webpage", {
      description: "Read documentation pages.",
      urls: ["docs.example.com", "*.effect.website"],
      ...(options?.actions === undefined ? {} : { actions: options.actions }),
    });
    const agent = Agent.define("web-capture-host", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Use read_webpage.",
      toolkit: Toolkit.make(definition.tool),
      policy,
    });
    const toolResults = yield* Ref.make<ReadonlyArray<ToolResultPart>>([]);
    const scripted = yield* makeScriptedPort;
    yield* AgentRuntime.run(
      Agent.withModel(agent, scriptedModel("read_webpage", params, toolResults)),
      { question: "go" },
      {},
    ).pipe(Effect.provide(definition.handlers.pipe(Layer.provide(scripted.port))), Effect.scoped);
    return {
      toolResults: yield* Ref.get(toolResults),
      requests: yield* Ref.get(scripted.requests),
    };
  });

const runExtract = (
  params: Record<string, unknown>,
): Effect.Effect<ScenarioOutcome, unknown, IdGenerator> =>
  Effect.gen(function* () {
    const definition = WebCapture.makeExtract("extract_pricing", {
      description: "Extract pricing plans.",
      urls: ["docs.example.com"],
      schema: PricingSchema,
      prompt: "Extract every plan",
    });
    const agent = Agent.define("web-extract-host", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Use extract_pricing.",
      toolkit: Toolkit.make(definition.tool),
      policy,
    });
    const toolResults = yield* Ref.make<ReadonlyArray<ToolResultPart>>([]);
    const scripted = yield* makeScriptedPort;
    yield* AgentRuntime.run(
      Agent.withModel(agent, scriptedModel("extract_pricing", params, toolResults)),
      { question: "go" },
      {},
    ).pipe(Effect.provide(definition.handlers.pipe(Layer.provide(scripted.port))), Effect.scoped);
    return {
      toolResults: yield* Ref.get(toolResults),
      requests: yield* Ref.get(scripted.requests),
    };
  });

layer(identifiers)("WebCapture handlers through a scripted port", (it) => {
  it.effect("projects an allowed markdown capture onto the port and returns the page", () =>
    Effect.gen(function* () {
      const outcome = yield* runCapture({
        url: "https://docs.example.com/pricing",
        action: "markdown",
      });
      expect(outcome.requests).toHaveLength(1);
      expect(outcome.requests[0].engine).toBe("chromium");
      expect(outcome.requests[0].limits.maxOutputBytes).toBe(128 * 1024);
      expect(outcome.requests[0].action._tag).toBe("CapturePageMarkdown");
      const requestPatterns = outcome.requests[0].resourcePolicy?.allowRequestPatterns;
      expect(requestPatterns).toEqual([
        "^https://docs\\.example\\.com(?::[0-9]+)?(?:[/?#]|$)",
        "^https://(?:[a-z0-9-]+\\.)*effect\\.website(?::[0-9]+)?(?:[/?#]|$)",
      ]);
      const allowsRequest = (url: string): boolean =>
        requestPatterns?.some((pattern) => new RegExp(pattern).test(url)) ?? false;
      for (const allowed of [
        "https://docs.example.com/pricing",
        "https://docs.example.com:8443/redirect-target",
        "https://effect.website/",
        "https://nested.docs.effect.website/resource.js",
      ]) {
        expect(allowsRequest(allowed)).toBe(true);
      }
      for (const denied of [
        "https://docs.example.com.attacker.test/",
        "https://effect.website.attacker.test/",
        "https://evil.example.net/redirect-target",
        "https://docs.example.com@attacker.test/",
        "http://docs.example.com/resource.js",
      ]) {
        expect(allowsRequest(denied)).toBe(false);
      }
      expect(outcome.toolResults).toHaveLength(1);
      expect(outcome.toolResults[0].isFailure).toBe(false);
      expect(outcome.toolResults[0].result).toMatchObject({
        url: "https://docs.example.com/pricing",
        action: "markdown",
        markdown: "# Pricing",
      });
    }),
  );

  it.effect("denies an off-allowlist host fail-closed before the port is reached", () =>
    Effect.gen(function* () {
      const outcome = yield* runCapture({ url: "https://evil.example.net/x", action: "markdown" });
      expect(outcome.requests).toHaveLength(0);
      expect(outcome.toolResults[0].isFailure).toBe(true);
      expect(outcome.toolResults[0].result).toMatchObject({
        _tag: "WebCaptureFailure",
        errorTag: "WebCaptureUrlDenied",
      });
    }),
  );

  it.effect("allows wildcard apexes and subdomains while rejecting hostname-boundary attacks", () =>
    Effect.gen(function* () {
      for (const url of ["https://effect.website/", "https://docs.effect.website/pricing"]) {
        const allowed = yield* runCapture({ url, action: "markdown" });
        expect(allowed.requests).toHaveLength(1);
        expect(allowed.toolResults[0].isFailure).toBe(false);
      }

      const denied = yield* runCapture({
        url: "https://effect.website.attacker.test/pricing",
        action: "markdown",
      });
      expect(denied.requests).toHaveLength(0);
      expect(denied.toolResults[0].result).toMatchObject({ errorTag: "WebCaptureUrlDenied" });
    }),
  );

  it.effect("denies a non-https target and a disabled action without a port call", () =>
    Effect.gen(function* () {
      const insecure = yield* runCapture({
        url: "http://docs.example.com/pricing",
        action: "markdown",
      });
      expect(insecure.requests).toHaveLength(0);
      expect(insecure.toolResults[0].result).toMatchObject({ errorTag: "WebCaptureUrlDenied" });

      const disabled = yield* runCapture(
        { url: "https://docs.example.com/pricing", action: "links" },
        { actions: ["markdown"] },
      );
      expect(disabled.requests).toHaveLength(0);
      expect(disabled.toolResults[0].result).toMatchObject({
        errorTag: "WebCaptureActionDenied",
      });
    }),
  );

  it.effect("returns the platform backoff hint as a typed model-visible failure", () =>
    Effect.gen(function* () {
      const outcome = yield* runCapture({
        url: "https://docs.example.com/rate-limited",
        action: "markdown",
      });
      expect(outcome.toolResults[0].isFailure).toBe(true);
      expect(outcome.toolResults[0].result).toMatchObject({
        _tag: "WebCaptureFailure",
        errorTag: "PageCaptureRateLimitedError",
        retryAfterMillis: 7_000,
      });
    }),
  );

  it.effect("keeps foreign protocol failure causes out of the model-visible result", () =>
    Effect.gen(function* () {
      const outcome = yield* runCapture({
        url: "https://docs.example.com/protocol-failure",
        action: "markdown",
      });

      expect(outcome.toolResults[0].isFailure).toBe(true);
      expect(outcome.toolResults[0].result).toMatchObject({
        _tag: "WebCaptureFailure",
        errorTag: "PageCaptureProtocolError",
        message: "The browser RPC failed",
      });
      expect(outcome.toolResults[0].result).not.toHaveProperty("cause");
      expect(JSON.stringify(outcome.toolResults[0].result)).not.toContain("secret-token");
    }),
  );

  it.effect("returns inference policy failures without their host-only cause", () =>
    Effect.gen(function* () {
      const outcome = yield* runCapture({
        url: "https://docs.example.com/inference-policy",
        action: "markdown",
      });

      expect(outcome.toolResults[0].result).toMatchObject({
        _tag: "WebCaptureFailure",
        errorTag: "PageCaptureInferencePolicyError",
        message: "Workers AI extraction was not authorized",
      });
      expect(outcome.toolResults[0].result).not.toHaveProperty("cause");
      expect(JSON.stringify(outcome.toolResults[0].result)).not.toContain("secret tenant");
    }),
  );

  it.effect("fails typed when the adapter answers with the wrong output kind", () =>
    Effect.gen(function* () {
      const outcome = yield* runCapture({
        url: "https://docs.example.com/mismatch",
        action: "markdown",
      });
      expect(outcome.toolResults[0].isFailure).toBe(true);
      expect(outcome.toolResults[0].result).toMatchObject({
        errorTag: "WebCaptureProtocolMismatch",
      });
    }),
  );

  it.effect("sends the derived response format and decodes the extraction through the Schema", () =>
    Effect.gen(function* () {
      const outcome = yield* runExtract({ url: "https://docs.example.com/pricing" });
      expect(outcome.requests).toHaveLength(1);
      const action = outcome.requests[0].action;
      expect(action._tag).toBe("CapturePageStructured");
      if (action._tag === "CapturePageStructured") {
        expect(JSON.stringify(action.responseFormat)).toContain("monthlyUsd");
        expect(action.prompt).toBe("Extract every plan");
      }
      expect(outcome.toolResults[0].isFailure).toBe(false);
      expect(outcome.toolResults[0].result).toEqual({
        plans: [{ name: "Pro", monthlyUsd: 20 }],
      });
    }),
  );

  it.effect("an extraction outside the Schema fails typed, never a fabricated success", () =>
    Effect.gen(function* () {
      const outcome = yield* runExtract({ url: "https://docs.example.com/malformed" });
      expect(outcome.toolResults[0].isFailure).toBe(true);
      expect(outcome.toolResults[0].result).toMatchObject({
        _tag: "WebCaptureFailure",
        errorTag: "WebCaptureDecodeFailed",
        message: "The extracted value did not match the expected schema",
      });
      expect(JSON.stringify(outcome.toolResults[0].result)).not.toContain("secret-token");
    }),
  );

  it.effect("keeps decoder services visible and supplies them when validating extraction", () =>
    Effect.gen(function* () {
      const definition = WebCapture.makeExtract("extract_normalized_pricing", {
        description: "Extract normalized pricing plans.",
        urls: ["docs.example.com"],
        schema: ServicePricingSchema,
      });
      const agent = Agent.define("web-service-extract-host", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Use extract_normalized_pricing.",
        toolkit: Toolkit.make(definition.tool),
        policy,
      });
      const toolResults = yield* Ref.make<ReadonlyArray<ToolResultPart>>([]);
      const scripted = yield* makeScriptedPort;
      const decoderLayer = Layer.succeed(ExtractionDecoderService, {
        normalize: (value) => value.toUpperCase(),
      });

      yield* AgentRuntime.run(
        Agent.withModel(
          agent,
          scriptedModel(
            "extract_normalized_pricing",
            { url: "https://docs.example.com/pricing" },
            toolResults,
          ),
        ),
        { question: "go" },
        {},
      ).pipe(
        Effect.provide(
          definition.handlers.pipe(Layer.provideMerge(Layer.merge(scripted.port, decoderLayer))),
        ),
        Effect.scoped,
      );

      expect(yield* Ref.get(toolResults)).toMatchObject([
        {
          isFailure: false,
          result: { plans: [{ name: "PRO", monthlyUsd: 20 }] },
        },
      ]);
    }),
  );
});

// ---------------------------------------------------------------------------
// Compile-time E/R proofs (change discipline: type tests whenever Agent or
// Effect AI composition changes).
// ---------------------------------------------------------------------------

type Equal<Left, Right> =
  (<T>() => T extends Left ? 1 : 2) extends <T>() => T extends Right ? 1 : 2 ? true : false;

const typedDefinition = WebCapture.make("typed_web_capture", {
  description: "typed",
  urls: ["docs.example.com"],
});

type LayerContext<L> = L extends Layer.Layer<infer _Out, infer _Error, infer R> ? R : never;
type CaptureLayerRequirements = LayerContext<typeof typedDefinition.handlers>;
type RequiresPageCapture = Equal<CaptureLayerRequirements, PageCapture>;
type FailureIsEnvelope = Equal<
  (typeof typedDefinition.tool.failureSchema)["Type"],
  WebCaptureFailure
>;
type SuccessIsBounded =
  Tool.Success<typeof typedDefinition.tool> extends WebCaptureSuccess ? true : false;

const typedExtract = WebCapture.makeExtract("typed_extract", {
  description: "typed",
  urls: ["docs.example.com"],
  schema: PricingSchema,
});
type ExtractLayerRequirements = LayerContext<typeof typedExtract.handlers>;
type ExtractRequiresPageCapture = Equal<ExtractLayerRequirements, PageCapture>;
type ExtractSuccessIsDecoded = Equal<
  Tool.Success<typeof typedExtract.tool>,
  typeof PricingSchema.Type
>;

const typedServiceExtract = WebCapture.makeExtract("typed_service_extract", {
  description: "typed",
  urls: ["docs.example.com"],
  schema: ServicePricingSchema,
});
type ServiceExtractLayerRequirements = LayerContext<typeof typedServiceExtract.handlers>;
type ServiceExtractKeepsDecoderRequirement = Equal<
  ServiceExtractLayerRequirements,
  PageCapture | ExtractionDecoderService
>;
type ServiceExtractToolKeepsDecoderRequirement = Equal<
  Tool.HandlerServices<typeof typedServiceExtract.tool>,
  ExtractionDecoderService
>;

describe("WebCapture type proofs", () => {
  it("pins the PageCapture requirement, envelope failure, and decoded success", () => {
    const requirementProof: RequiresPageCapture = true;
    const failureProof: FailureIsEnvelope = true;
    const successProof: SuccessIsBounded = true;
    const extractRequirementProof: ExtractRequiresPageCapture = true;
    const extractSuccessProof: ExtractSuccessIsDecoded = true;
    const decoderRequirementProof: ServiceExtractKeepsDecoderRequirement = true;
    const decoderInvocationProof: ServiceExtractToolKeepsDecoderRequirement = true;
    expect(
      requirementProof &&
        failureProof &&
        successProof &&
        extractRequirementProof &&
        extractSuccessProof &&
        decoderRequirementProof &&
        decoderInvocationProof,
    ).toBe(true);
  });
});
