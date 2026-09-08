---
"@effect-agent/engine": patch
"@effect-agent/thread": patch
---

Expose host tool scheduling through `RunToolScheduling` so durable agents can run independent tools concurrently while preserving barriers around sequential tools. Capture the host policy across replacement attempts and retain the agent's admitted concurrency bound.
