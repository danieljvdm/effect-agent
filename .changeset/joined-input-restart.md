---
"effect-agent": patch
---

Enable `policy.restartOnJoinedInput` to replace a disposable model call with joined input, at most twice per run, while preserving durable usage and settlement. When enabling it, handle `ModelRestarted` by clearing drafts for its `turnId`; calls exposing provider-defined tools retain seam steering.
