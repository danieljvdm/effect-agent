---
"@effect-agent/thread": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/platform-node": minor
"@effect-agent/platform-cloudflare": minor
---

Add durable schedules for typed Agent input with owner authorization, one-shot, interval and cron timing, and recovery through ordinary Submission admission on Node and Cloudflare.

BEHAVIOR CHANGE: Reset older private-development SQLite databases for storage version 5, and provide `effect-cf ^0.37.0` to Cloudflare hosts.
