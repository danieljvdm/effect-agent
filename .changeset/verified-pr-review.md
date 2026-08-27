---
"@effect-agent/pr-review": patch
---

Review the exact pull-request delta against repository source, preserving initial findings through an additional check for missed causes. BEHAVIOR CHANGE: invoke `reviewer.review` with an authorized, immutable `ReviewRepository` and use the `@effect-agent review` commands; direct definition/binding access, internal policy/sanitizer exports, the `style` category, and legacy slash commands are removed.
