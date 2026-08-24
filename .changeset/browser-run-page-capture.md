---
"@effect-agent/sandbox": patch
"@effect-agent/capabilities": patch
"@effect-agent/platform-cloudflare": patch
---

Add the schema-first `PageCapture` port and conservative `WebCapture.make`/`WebCapture.makeExtract` Tools over an immutable, deny-by-default browser-request allowlist. Add native `BrowserRun` Quick Action Layers with bounded response streaming and a typed Workers AI authorization and accounting failure for structured extraction.

```ts
const readDocs = WebCapture.make("read_webpage", {
  description: "Read documentation pages.",
  urls: ["docs.example.com", "*.effect.website"],
});
// worker: browserQuickActionCaptureLayer().pipe(
//   Layer.provide(BrowserQuickActionBrowserBinding.layer({ browser: env.BROWSER })),
// )
```
