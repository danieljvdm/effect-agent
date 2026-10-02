---
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent": patch
---

Make Cloudflare Thread maintenance durably incremental and quiescent (#93). Stable
externally-driven waits now clear their alarm after acknowledging the observed maintenance
generation, while pre-armed public and routed mutations, restart recovery, and bounded autonomous
rearming preserve liveness. A caught-up forced alarm takes an O(1) maintenance-record path without
recovery, ledger scans, or canonical-history reads. Child settlements also commit the parent's
durable wake before child ledger finalization, preventing eviction from losing a quiescent join.
