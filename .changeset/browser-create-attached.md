---
"@effect-agent/platform-cloudflare": minor
---

Add `BrowserSessions.createAttached` to retain a new browser and use its initial scoped attachment without reconnecting before the first command. Preserve durable ownership, per-command authorization and timeouts, and exact-session cleanup.
