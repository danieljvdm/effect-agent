---
"@effect-agent/pr-review": minor
---

Make review runs visible while they execute. The packaged Action now posts one sticky "review in
progress" issue comment the moment a run starts — naming the scope, head commit, model, and
workflow run — and rewrites that same comment in place with the settled outcome (posted verdict,
blocking/incomplete callout, or run failure). Posting is at-least-once with generation-fenced
writes: a stale run cannot replace a newer run's status, and duplicate comments left by unfenced
overlapping runs are best-effort deleted by the next run. Progress reporting is cosmetic and
fail-open: GitHub faults are logged and never change the review, the check conclusion, or the run
result. Disable with the new `progress-comment` input; dry runs post no progress.

Action logs now render one compact line per event (tool executions, warnings with their cause)
instead of raw OTel-style telemetry dumps. The new `log-level` input (default `Info`) shows the
engine's per-turn telemetry at `Debug` or quiets routine runs at `Warn`.
