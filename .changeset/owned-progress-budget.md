---
"@effect-agent/engine": patch
---

Bound published Tool progress to 8 MiB of JSON per Run and retain owned snapshots for live events and detached replay. Reject invalid or oversized application progress with `ModelProtocolError`, and allow a smaller cumulative limit through `bufferLimits.maxToolProgressBytes`.
