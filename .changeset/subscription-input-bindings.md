---
"@effect-agent/session": minor
"@effect-agent/platform-node": minor
"@effect-agent/platform-cloudflare": minor
---

Bind subscription input preparation to each destination Agent's retained definition version, authorize reconciliation through explicit host policy, and preserve newer delivery retry state.

BEHAVIOR CHANGE: Provide `SubscriptionInputBindings` and `SubscriptionAuthorizer.reconcile` in subscription hosts, and import GitHub integration from `@effect-agent/session/github`.
