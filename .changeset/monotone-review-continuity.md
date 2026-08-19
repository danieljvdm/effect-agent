---
"@effect-agent/pr-review": minor
---

Make incremental reviews converge: the authenticated baseline now advances on every completed
run, carrying unsettled or unreviewable scope forward for automatic retry, and fan-out passes
are host-scheduled with one retry each so a flaky pass can no longer reopen the whole
post-baseline scope. BEHAVIOR CHANGE: stored review state moved to `state-v2` (the first run
after upgrading performs one full review), blocking findings now outrank machinery gaps in the
check conclusion, and the coordinator-model exports (`FanOutReviewer`, `makeFanOutReviewSuite`,
`fanOutHandlersLayer`, `DelegateFileReview`, `FileReviewRequest`, `FileReviewUnitResult`,
`FileReviewWorkRejected`, `FileReviewUnitFailed`, `ListReviewUnits`) and the
`usageScope`/`reviewShape` options are removed — fan-out runs report whole-run usage via
`executeFanOutReview`.
