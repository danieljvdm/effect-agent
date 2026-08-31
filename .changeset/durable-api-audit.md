---
"@effect-agent/capabilities": patch
"@effect-agent/engine": patch
"@effect-agent/session": minor
"@effect-agent/platform-node": minor
"@effect-agent/platform-cloudflare": patch
"@effect-agent/testing": minor
---

Enforce durable child tool-call allowances across recovery and distinguish passing checks from complete adapter certification. Rename the custom durable assembly to `layerWithServices` and preserve Node extension-layer construction errors and dependencies.

BEHAVIOR CHANGE: Replace `DurableAgentRuntime.layerWithContext` with `layerWithServices`, still supplying both separate services. Regenerate certification reports with the `effect-agent/certification@2` schema and use `fullyCertified` for gates requiring executed real-loss checks; `ok` retains its executed-check meaning. Existing child records without an allowance keep their original definition policy; start a new delegation to apply a limit.
