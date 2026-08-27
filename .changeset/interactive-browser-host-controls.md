---
"@effect-agent/sandbox": minor
"@effect-agent/platform-cloudflare": minor
---

Add same-session PNG screenshots, viewport scrolling, and explicit closure to interactive browser handles. Expose host-only Cloudflare Live View, handoff, and cleanup through redacted session identities.

BEHAVIOR CHANGE: Custom browser adapters must implement `screenshot`, `scroll`, and the `close` Effect.
