---
"@effect-agent/pr-review": patch
---

Add a `guidance-file` action input (`PR_REVIEW_GUIDANCE_FILE`): the review
guidance can now live as a committed review-profile document instead of
workflow YAML, read at run time and injected before any inline `guidance`.
A configured-but-unreadable file fails typed rather than reviewing without
its profile.
