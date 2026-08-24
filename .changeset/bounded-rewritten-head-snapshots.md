---
"@effect-agent/pr-review": patch
---

Keep incremental reviews scoped after amended or force-pushed heads by comparing complete Git tree
snapshots across bounded PR paths, then hydrating changed paths from the current full PR records.
Fall back to a full review with an observable reason when either snapshot is unavailable, malformed,
or truncated.
