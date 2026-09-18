---
"@effect-agent/platform-cloudflare": patch
---

Add a host-configured `originOverride` to interactive Browser Run bindings for finite HTTP test sessions that keep the browser-facing origin. Reject detached sessions and human handoff while a test-origin override is active.
