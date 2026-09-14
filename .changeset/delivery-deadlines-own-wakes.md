---
"@effect-agent/platform-cloudflare": patch
---

Schedule message delivery maintenance from its durable due index without waking source execution recovery for delivery bookkeeping. Preserve prearmed recovery across eviction and wake retained deliveries when they become due.
