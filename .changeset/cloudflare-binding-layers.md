---
"@effect-agent/platform-cloudflare": patch
"@effect-agent/session": patch
"@effect-agent/platform-node": patch
"@effect-agent/testing": patch
---

Compose Cloudflare Conversation Objects from application Layers and typed Agent version declarations, preserving initialization failures and scoped dependencies. Resolve durable work from explicit exact-version bindings and reject digest-transparent registrations.

BEHAVIOR CHANGE: Replace `makeConversationObjectClass` with `ConversationObject.make`. Pass a composed `ConversationObject.layer(registrations)` to `ConversationObject.make`, move preparation and Tool authorization into Layers, and use `options.eventLayer` for observability. Pass bindings directly to resolved worker methods and `NodeDurableHost.layer(bindings)` instead of providing `AgentBindingResolver`.
