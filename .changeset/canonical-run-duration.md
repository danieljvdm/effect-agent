---
"@effect-agent/engine": patch
"@effect-agent/thread": patch
---

Preserve each durable Run's original start and duration across recovery, report truthful elapsed time, and complete verified child cleanup without allowing execution after expiry. Reject incompatible execution history without canonical start evidence; reset affected private-development data before resuming it.
