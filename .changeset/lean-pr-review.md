---
"@effect-agent/pr-review": minor
---

Replace the reviewer with a provider-neutral, single-pass agent and move GitHub and provider policy
to the private channel. Review large diffs in one bounded four-shard parallel wave, limit automatic
GitHub waves to two, require a collaborator command for later reviews, and present findings with
severity and category labels plus agent-ready prompts.
