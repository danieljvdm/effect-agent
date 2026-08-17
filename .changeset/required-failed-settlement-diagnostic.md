---
"@effect-agent/session": minor
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
---

Require every failed canonical `SubmissionSettled` record to carry the exact bounded generic
`{ errorTag, message }` diagnostic and expose it as `Settlement.failure`. Joined failure fanout,
recovery, durable adapter finalization, and idempotent replay preserve the host's canonical
diagnostic byte-for-byte. Result-less completed joins and aborted settlements remain explicitly
valid; malformed private-development failed records now fail closed at Schema decode.
