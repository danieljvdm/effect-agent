---
"@effect-agent/platform-cloudflare": patch
---

Expose `ConversationObject.make`, `.layer`, and `.Options` for Cloudflare hosts, and accept Effects of Agent registrations with yielded effect-cf services and typed initialization failures.

BEHAVIOR CHANGE: Replace `makeConversationObjectClass` with `ConversationObject.make` and `CloudflareDurableRuntime.layer` with `ConversationObject.layer`; pass the registration Effect first instead of a `bindings` callback option.
