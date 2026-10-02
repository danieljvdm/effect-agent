---
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent": minor
---

Add durable schedules for typed Agent input with owner authorization, one-shot, interval and cron timing, and recovery through ordinary Submission admission on Node and Cloudflare.

BEHAVIOR CHANGE: Reset older private-development SQLite databases for storage version 5, and provide `effect-cf ^0.37.0` to Cloudflare hosts.
