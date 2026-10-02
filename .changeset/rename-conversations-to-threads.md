---
"@yielded/agent": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-pr-review": minor
"@yielded/agent-sandbox-local": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-testing": minor
---

Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.
