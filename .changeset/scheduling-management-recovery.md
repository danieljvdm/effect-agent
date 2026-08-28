---
"@effect-agent/session": minor
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-node": minor
"@effect-agent/platform-cloudflare": minor
---

Separate scheduling management from driver authority, expose explicit public status, and fix DST delivery, failed-record starvation, and repeated resume. Allow positive host interval minimums and release operational capacity when schedules finish while retaining creation replay guarantees.

BEHAVIOR CHANGE: Cloudflare consumers yield `Scheduling` from `CloudflareSchedulingClient.layer`; local drivers use `ScheduleDriver.layer`. Status omits persisted input and admission internals, and `dueBatchSize` bounds a query page within a sweep.
