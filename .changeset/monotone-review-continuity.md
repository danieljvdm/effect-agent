---
"@effect-agent/pr-review": minor
---

Schedule the fan-out review pipeline entirely from host code — no coordinator model, no
delegation tool — retrying each failed pass once, discarding invalid-anchor findings instead of
rejecting their pass, and advancing the authenticated incremental baseline on every completed
run with unsettled scope carried forward as retryable `unreviewedPaths`.

BEHAVIOR CHANGE: incremental reviews now converge instead of looping — a remediation push is
reviewed against its immediate predecessor plus carried leftovers, never the whole
post-baseline scope; blocking findings outrank machinery gaps in the check conclusion, and an
`incomplete` conclusion explicitly names a reviewer-side gap retried automatically, not a
request to change code. Stored review state moved to `state-v2`, so the first run after
upgrading performs one full review. Removed exports: the coordinator agent
(`FanOutReviewer`, `makeFanOutReviewSuite`, `fanOutHandlersLayer`, `DelegateFileReview`,
`FileReviewRequest`, `FileReviewUnitResult`, `FileReviewWorkRejected`, `FileReviewUnitFailed`,
`ListReviewUnits`) and the `usageScope`/`reviewShape` options — fan-out runs now report
whole-run usage via `executeFanOutReview`.
