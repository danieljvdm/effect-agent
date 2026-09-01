---
"@effect-agent/core": minor
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/thread": minor
---

Derive delegation schemas and mappings from child definitions, inherit omitted policy defaults within shared reservation limits, and accept model Layers directly for subagent execution and durable registration.

BEHAVIOR CHANGE: Durable delegations enforce shared reservation caps; configure identical `parentCaps` when multiple delegation policies share a parent Run.
