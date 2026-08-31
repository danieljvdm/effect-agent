---
"@effect-agent/platform-cloudflare": patch
"@effect-agent/thread": patch
"@effect-agent/platform-node": patch
"@effect-agent/testing": patch
---

Compose Cloudflare Thread Objects from application Layers and typed Agent version declarations, preserving initialization failures and scoped dependencies. Resolve durable work from explicit exact-version bindings and reject digest-transparent registrations.

BEHAVIOR CHANGE: Replace `makeConversationObjectClass` with `ThreadObject.make`. Pass a composed `ThreadObject.layer(registrations)` to `ThreadObject.make`, move preparation and Tool authorization into Layers, and use `options.eventLayer` for observability. Pass bindings directly to resolved worker methods and `NodeDurableHost.layer(bindings)` instead of providing `AgentBindingResolver`.
