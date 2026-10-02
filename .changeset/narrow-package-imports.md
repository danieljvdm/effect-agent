---
"@yielded/agent-testing": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent": minor
---

Import specialized testing utilities and fixtures from their documented subpaths, and use failpoint controls from `/testing` with `TestControl.layer` in place of `Failpoint.layerTest`; keep migration loaders internal.
Import Browser Run adapters from their dedicated Cloudflare subpaths and install `@cloudflare/puppeteer` explicitly when using `/interactive-browser`.
