---
"effect-agent": patch
"@effect-agent/platform-cloudflare": patch
---

Allow callers to set finite browser pass allowances beyond one hour and keep retained protected sessions active with `BrowserRunProtectedHost.keepAlive(sessionId)`. Preserve unrestricted service-worker handling and distinguish failed-resume attachment retirement from uncertain local cleanup.
