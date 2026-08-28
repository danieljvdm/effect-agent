---
"@effect-agent/pr-review": minor
---

Replace the reviewer with a provider-neutral, single-pass agent and move GitHub and provider policy
to the private channel. Review large diffs in one bounded four-shard parallel wave, make the
automatic attempt limit consumer-configurable, show when automatic reviews pause, estimate known
GPT-5.6 costs, report cached and uncached input usage, publish one no-model closing review after the
limit is reached, and present findings with severity and category labels plus agent-ready prompts.
