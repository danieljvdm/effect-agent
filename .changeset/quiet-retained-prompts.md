---
"@effect-agent/core": patch
---

Default derived run-status messages to off so transient counters do not replace retained conversation cache boundaries. BEHAVIOR CHANGE: opt into `runStatus: "appended"` when model-visible counters are required; runtime limits and budget events remain enforced.
