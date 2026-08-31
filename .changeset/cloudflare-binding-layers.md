---
"@effect-agent/platform-cloudflare": patch
---

Expose `ConversationObject.make`, `.layer`, and `.Options` for Cloudflare hosts, and resolve Agent Bindings through an `AgentBindingResolver` Layer with yielded effect-cf services and typed initialization failures.

BEHAVIOR CHANGE: Replace `makeConversationObjectClass` with `ConversationObject.make` and `CloudflareDurableRuntime.layer` with `ConversationObject.layer`; pass the resolver Layer first instead of a `bindings` callback option.
