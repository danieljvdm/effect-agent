---
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-workflow": patch
"@yielded/agent": patch
---

Add revisioned subscription management, bounded event retention, and explicit recovery of parked admissions. Fence fresh destination admission by host policy and retain one unsettled submission per optional admission group until canonical settlement.

BEHAVIOR CHANGE: Reset incompatible development storage and update custom stores for required configuration revisions and retry generations.
