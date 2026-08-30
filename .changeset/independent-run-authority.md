---
"@effect-agent/engine": patch
"@effect-agent/session": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
---

Compose prompt preparation and Tool authorization independently in durable hosts, preserving both across recovery.

BEHAVIOR CHANGE: move `RunContextPreparation.toolAuthorization` to a separate `RunToolAuthorization` Layer and provide both services to `DurableAgentRuntime.layerWithContext`.
