---
"@effect-agent/sandbox": minor
"@effect-agent/platform-cloudflare": patch
---

Add an explicit interactive browser network policy and reject `PublicWeb` with a typed unsupported error before Cloudflare launches a browser.

BEHAVIOR CHANGE: Move `allowedHosts` into `network: { _tag: "ExactHosts", allowedHosts }` for existing page-request allowlist workflows; `PublicWeb` remains unsupported on Cloudflare.
