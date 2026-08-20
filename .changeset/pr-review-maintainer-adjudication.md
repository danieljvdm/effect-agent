---
"@effect-agent/pr-review": minor
---

Let maintainers adjudicate findings from the pull request itself — reply `/adjudicate accepted-risk|refuted|obsolete[: reason]` on a finding's inline thread, or comment `/adjudicate <disposition> "<exact title>"[: reason]` in the conversation for unanchored concerns — and the exact identity leaves active findings, verdict counts, and the check conclusion, renders in a collapsed "Adjudicated" section, and persists in the signed review state; only OWNER/MEMBER/COLLABORATOR comments count, everything else is ignored fail-closed. Incremental runs also inject prior-round findings on re-reviewed paths into the reviewer prompt as context, so successive rounds confirm, resolve, or explicitly withdraw them instead of contradicting each other.
