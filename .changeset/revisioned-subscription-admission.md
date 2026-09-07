---
"@effect-agent/thread": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/workflow": patch
---

Add revisioned subscription management, bounded event retention, and explicit recovery of parked admissions. Fence fresh destination admission by host policy and retain one unsettled submission per optional admission group until canonical settlement.

BEHAVIOR CHANGE: Reset incompatible development storage and update custom stores for required configuration revisions and retry generations.
