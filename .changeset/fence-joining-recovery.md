---
"effect-agent": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-cloudflare": patch
---

Prevent recovery from reverting joined input while its host is still processing it. Recheck canonical input after acquiring recovery ownership and fence rollback against the current host.

BEHAVIOR CHANGE: Custom `SubmissionLedger` adapters must validate a supplied `RevertJoiningRequest.guard` atomically with rollback; requests without a guard retain their existing behavior.
