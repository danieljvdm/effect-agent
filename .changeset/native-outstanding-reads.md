---
"effect-agent": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/testing": patch
---

Expose bounded outstanding-operation and pending-delivery reads, exact canonical record locators, and native worker/peer admission lookups. Retain uncertain external outcomes after abort and retire worker inputs only after their effects are resolved.
