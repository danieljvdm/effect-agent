---
"effect-agent": minor
"@effect-agent/capabilities": minor
"@effect-agent/core": minor
"@effect-agent/engine": minor
"@effect-agent/platform-cloudflare": minor
"@effect-agent/platform-node": minor
"@effect-agent/pr-review": minor
"@effect-agent/sandbox": minor
"@effect-agent/sandbox-local": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/testing": minor
"@effect-agent/thread": minor
---

Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.
