---
"@effect-agent/pr-review": patch
---

Reserve a final review response when token, turn, or tool budgets stop investigation, preserving findings and usage.

BEHAVIOR CHANGE: Treat outcomes with `exhausted` as incomplete coverage, even when no findings are returned.
