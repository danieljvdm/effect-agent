---
"@effect-agent/platform-cloudflare": patch
---

Confirm exact Browser Run session termination before reporting cleanup success, including already-absent sessions. Provide `BrowserRunSessionLifecycle.layer({ accountId, apiToken })` with an account-scoped Browser Rendering Write token when constructing the interactive binding.
