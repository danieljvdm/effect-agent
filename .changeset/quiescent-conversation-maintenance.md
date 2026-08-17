---
"@effect-agent/platform-cloudflare": patch
"@effect-agent/session": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
---

Make Cloudflare Conversation maintenance durably incremental and quiescent (#93). Stable
externally-driven waits now clear their alarm after acknowledging the observed maintenance
generation, while pre-armed public and routed mutations, restart recovery, and bounded autonomous
rearming preserve liveness. A caught-up forced alarm takes an O(1) maintenance-record path without
recovery, ledger scans, or canonical-history reads. Child settlements also commit the parent's
durable wake before child ledger finalization, preventing eviction from losing a quiescent join.
