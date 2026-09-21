---
"@effect-agent/platform-cloudflare": patch
---

Dispatch up to two independent Threads concurrently in a Cloudflare maintenance event so newly ready input can start while another Thread is busy. Preserve per-Thread FIFO, scoped claim cleanup, and durable alarm recovery.
