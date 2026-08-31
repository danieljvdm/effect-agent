---
"@effect-agent/core": minor
"@effect-agent/engine": minor
---

Construct agents with `Agent.make` and execute Definitions with native model Layers through `Effect.provide`. Accept Schema-encoded inputs in `run`, `stream`, and `start`, and decode external data through `runUnknown`, `streamUnknown`, and `startUnknown`.

BEHAVIOR CHANGE: Replace `Agent.define` with `Agent.make`; move inputs typed as `unknown` to the explicit unknown-input operations.
