---
"@effect-agent/platform-cloudflare": patch
---

Stop requesting `allowExperimental` when loading Code Mode dynamic workers. The runtime only accepts that option when the calling worker carries the `experimental` compatibility flag, which deployed consumers cannot set, so every Code Mode pass was rejected at load in production (`CodeExecutorStartError: The Worker Loader rejected the pass: 'allowExperimental' is only allowed when the calling worker has the 'experimental' compat flag set`). The harness uses no experimental runtime features, and the conformance suite now runs without the `experimental` flag — the same configuration as a deployed consumer — so it guards this regression.
