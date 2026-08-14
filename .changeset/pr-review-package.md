---
"@effect-agent/pr-review": patch
---

Introduce `@effect-agent/pr-review`: the pull-request reviewer promoted from
`examples/pr-review` into a publishable package (owner decision D-034,
ADR-0016). Schema-first review contracts, `PullRequestSource`/`ReviewPublisher`
ports with GitHub REST adapters, fail-closed anchor validation and publication
planning, flat and S1 fan-out reviewer shapes, the `PrReview` configuration
factory (guidance, policy override, findings bound, ignore globs, extra
read-only tools), a deterministic `./testing` entry, and `./action`/`./cli`
host entrypoints backing the prebuilt node-runtime GitHub Action at `action/`.
Deployment class E; review posting is never claimed exactly-once.
