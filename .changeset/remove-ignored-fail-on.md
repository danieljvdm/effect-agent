---
"@effect-agent/pr-review": patch
---

Remove the ignored `failOn` option from `runReviewAction` and the `FailOnPolicy` export; host-derived check conclusions were already unconditional, so the option had no effect. The packaged Action still accepts the deprecated `fail-on` input and continues to ignore it.
