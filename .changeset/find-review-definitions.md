---
"@effect-agent/pr-review": minor
---

Add bounded literal searches within source files so reviewers can locate definitions before reading them. Implement `findInFile` in custom `ReviewRepository` layers, using `ReviewLineMatches.fromText` for the shared search limits.
