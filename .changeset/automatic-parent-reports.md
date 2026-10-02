---
"@yielded/agent-platform-node": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent": minor
---

Opt into durable typed parent completion messages with `Subagent.background(Research, { start: true, followUp: true, reportToParent: true })`, without an application input union, mapper, or reporting registration. Pass a custom reporting descriptor as `reportToParent` when an application-specific input format is needed.
