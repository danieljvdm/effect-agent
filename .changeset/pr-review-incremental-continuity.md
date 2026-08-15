---
"@effect-agent/pr-review": patch
---

Make GitHub Action PR reviews incremental across corrective pushes using authenticated,
lineage-validated review state, preserve unresolved findings and accepted scope, provide an
explicit final full-diff audit, and fail the review check for blocking findings or incomplete
coverage. Align delegated file-review tool-call bounds with the maximum review-unit size so
normal diff and context reads can complete without deterministic policy exhaustion.
