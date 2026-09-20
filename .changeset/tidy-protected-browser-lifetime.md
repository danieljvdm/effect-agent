---
"effect-agent": patch
"@effect-agent/platform-cloudflare": patch
---

Allow callers to set finite browser pass allowances beyond one hour and keep retained protected sessions active with `BrowserRunProtectedHost.keepAlive(sessionId)`. Preserve service-worker and request handling for unrestricted protected passes.
