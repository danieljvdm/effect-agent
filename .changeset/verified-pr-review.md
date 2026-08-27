---
"@effect-agent/pr-review": patch
---

Review independent pull-request defects against repository source in one bounded run while keeping incremental findings within the changed scope. BEHAVIOR CHANGE: invoke `reviewer.review` with an authorized, immutable `ReviewRepository` and use the `@effect-agent review` commands; direct definition/binding access, internal policy/sanitizer exports, the `style` category, and legacy slash commands are removed.
