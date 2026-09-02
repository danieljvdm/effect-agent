import {
  CloudflareBrowserRest,
  browserRestWorkersAiCaptureLayer,
  type browserRestCaptureLayer,
} from "@effect-agent/platform-cloudflare/browser-rest-capture";
import type { PageCapture } from "@effect-agent/sandbox";
import { Redacted, Layer } from "effect";
import type { HttpClient } from "effect/unstable/http";
import { expect, it } from "vite-plus/test";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type Requirements<Value> =
  Value extends Layer.Layer<infer _Output, infer _Error, infer R> ? R : never;

const options = { accountId: "account", apiToken: Redacted.make("token") };

type RestRequirement = Equal<
  Requirements<ReturnType<typeof browserRestCaptureLayer>>,
  HttpClient.HttpClient
>;
type RestOutput = Equal<
  ReturnType<typeof browserRestCaptureLayer> extends Layer.Layer<
    PageCapture,
    never,
    HttpClient.HttpClient
  >
    ? true
    : false,
  true
>;

it("loads and composes the REST subpath in a Node-only consumer", async () => {
  const module = await import("@effect-agent/platform-cloudflare/browser-rest-capture");
  const requirement: RestRequirement = true;
  const output: RestOutput = true;

  expect(module.browserRestCaptureLayer(options)).toBeDefined();
  expect(requirement && output).toBe(true);
  // This source imports no Worker globals; a cloudflare: runtime dependency would fail this Node test.
  expect(browserRestWorkersAiCaptureLayer(options)).toBeDefined();
  expect(CloudflareBrowserRest.layer({ handlers: Layer.empty }, options)).toBeDefined();
});
