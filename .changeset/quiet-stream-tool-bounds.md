---
"@effect-agent/engine": patch
---

Reuse response codecs across streamed chunks and bound pending Tool stream fibers by the configured concurrency. Return owned JSON snapshots from programmatic Tool calls so later handler or redactor mutations cannot exceed the admitted result limit.
