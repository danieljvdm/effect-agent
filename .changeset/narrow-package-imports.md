---
"@effect-agent/testing": minor
"@effect-agent/platform-cloudflare": minor
"@effect-agent/session": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-cloudflare": minor
---

Import specialized testing utilities and fixtures from their documented subpaths, and use failpoint controls from `/testing` with `TestControl.layer` in place of `Failpoint.layerTest`; keep migration loaders internal.
Import Browser Run adapters from their dedicated Cloudflare subpaths and install `@cloudflare/puppeteer` explicitly when using `/interactive-browser`.
