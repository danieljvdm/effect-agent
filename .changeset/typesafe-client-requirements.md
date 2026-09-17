---
"@effect-agent/ai-typesafe": patch
---

Require `TypeSafeClient.Config` and `HttpClient` when constructing the TypeSafe client.

BEHAVIOR CHANGE: Replace `make(options)`, `layer(options)`, and `layerConfig(options)` with the `make` Effect or `layer` Layer; provide `TypeSafeClient.Config` (or `TypeSafeClient.Config.layer` for environment configuration) and apply HTTP policies to the supplied `HttpClient` instead of `transformClient`.
