---
"@effect-agent/pr-review": patch
---

Skip model execution for authenticated, patch-equivalent pull-request rebases while preserving the prior review conclusion. Changeset fingerprints now ignore unified-diff hunk coordinate shifts but remain sensitive to changed diff content and review configuration.
