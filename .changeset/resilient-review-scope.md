---
"@effect-agent/pr-review": patch
---

Keep exploratory out-of-scope reads from killing a review run. The read tools already return
typed refusals as model-visible results, but the engine's default 3-consecutive-failure stop
policy aborted the run when one parallel batch probed several paths outside the review scope —
the first incremental delta whose pull-request description named other files died this way before
the model had seen a single refusal. The flat reviewer and per-unit child policies now tolerate an
exploratory batch (`repeatedFailureLimit: 12`, still bounded by their tool-call and duration
budgets), and the reviewer instructions state explicitly that the listed changeset is the complete
readable scope — in incremental reviews a deliberate subset of the pull request's full diff.
