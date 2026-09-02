---
"@effect-agent/pr-review": patch
---

Accept complete patches up to the 256,000-character review batch capacity instead of excluding files above 80,000 characters.
Return incomplete token-budget results with unreviewed paths when input exceeds the engine or provider context limit, including before the first paid request.
