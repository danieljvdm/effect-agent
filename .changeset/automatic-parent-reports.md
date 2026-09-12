---
"@effect-agent/capabilities": minor
"@effect-agent/core": minor
"@effect-agent/engine": minor
"@effect-agent/thread": minor
"@effect-agent/platform-node": minor
"@effect-agent/platform-cloudflare": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-cloudflare": minor
"effect-agent": minor
---

Opt into durable typed parent completion messages with `Subagent.background(Research, { start: true, followUp: true, reportToParent: true })`, without an application input union, mapper, or reporting registration. Pass a custom reporting descriptor as `reportToParent` when an application-specific input format is needed.
