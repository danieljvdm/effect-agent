---
"@effect-agent/core": patch
"@effect-agent/engine": patch
"@effect-agent/session": patch
---

Harden context economics per the reviewer's second pass: the cost budget is
enforced even when a token breach soft-lands the same response; tool results
that cannot serialize become a bounded `UnserializableToolResult` sentinel
instead of passing through unbounded; recovery re-seeds spend and derives
token-exhaustion state (fail mode rejects an already-over-budget resume before
any model call); compaction summarizer usage is staged into the canonical
turn record; staged usage is validated as non-negative finite integers; and a
provider-only breaching stop settles honestly as budget-exhausted.
