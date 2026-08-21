---
"@effect-agent/sandbox": patch
"@effect-agent/capabilities": patch
"@effect-agent/platform-cloudflare": patch
---

Add the schema-first `PageCapture` port and conservative `WebCapture.make`/`WebCapture.makeExtract` Tools over an immutable, deny-by-default browser-request allowlist. Add `browserQuickActionCaptureLayer` with bounded response streaming and explicit Workers AI authorization for structured extraction.

```ts
const readDocs = WebCapture.make("read_webpage", {
  description: "Read documentation pages.",
  urls: ["docs.example.com", "*.effect.website"],
});
// worker: readDocs.handlers.pipe(Layer.provide(browserQuickActionCaptureLayer({ browser: env.BROWSER })))
```
