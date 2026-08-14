---
"@effect-agent/pr-review": patch
---

Skip re-reviews of unchanged changesets. Every posted review now embeds an
invisible changeset fingerprint (SHA-256 over the ignore-filtered changeset
plus the prompt signature); the action harness and the CLI's
`--skip-unchanged` compare it against the last posted review through the new
`PriorReviews` port and skip typed when nothing effective changed — so
base-branch auto-merges and equivalent rebases stop re-triggering reviews,
while real changes, conflict resolutions, and configuration changes still
review. Fails open: a fingerprint lookup fault reviews instead of skipping.
`PrReview.make`/`makeFanOut` expose the fingerprint; `runReviewAction` now
takes the reviewer object (`{ run, fingerprint }`) and a `skip-unchanged`
action input (default `true`).
