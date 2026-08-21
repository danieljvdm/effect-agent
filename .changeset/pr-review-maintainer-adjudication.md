---
"@effect-agent/pr-review": minor
---

Let maintainers adjudicate findings from the pull request itself — reply `/adjudicate accepted-risk|refuted|obsolete[: reason]` on a finding's inline thread, or comment `/adjudicate <disposition> "<exact title>"[: reason]` in the conversation for unanchored concerns — and the exact identity leaves active findings, verdict counts, and the check conclusion, renders in a collapsed "Adjudicated" section, and persists in the signed review state; only OWNER/MEMBER/COLLABORATOR comments count, everything else is ignored fail-closed. Inject prior-round findings on re-reviewed paths into incremental reviewer prompts; **BEHAVIOR CHANGE:** direct `PrReview.run` callers now provide `ReviewExecutionContext`, using `fullReviewExecutionContextLayer` for an explicit full review.
