---
"@effect-agent/engine": patch
---

Release completed turn state before starting the next turn to reduce memory retained during long runs.

BEHAVIOR CHANGE: Resources acquired by `beforeTurn` or `context.prepare` now close with that turn. Move resources needed across turns into a surrounding run Layer or Scope.
