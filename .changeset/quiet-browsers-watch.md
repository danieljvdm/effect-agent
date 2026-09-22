---
"@effect-agent/platform-cloudflare": patch
---

Add `BrowserSession.getReadOnlyLiveView` for provider-enforced read-only viewing of the retained page. Reject URLs unless Cloudflare confirms the requested read-only guardrail and exact target.
