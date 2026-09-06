---
"@effect-agent/pr-review": patch
---

Review large changes with a complete change index, paged diffs, caller search, and explicit unread coverage while retaining higher-priority findings when the report fills.
Check counterevidence before recording findings, and continue unread coverage with native context rollover under the same review budget while preserving specific missing-evidence reasons.

BEHAVIOR CHANGE: Implement `searchCode` on custom `ReviewRepository` services, and treat `pendingPaths` as including partially read files.
