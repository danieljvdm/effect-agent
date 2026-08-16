---
"@effect-agent/pr-review": minor
---

Richer review presentation, derived host-side from validated data. Every inline
comment now ends with a collapsed "🤖 Prompt for AI agents" copy-paste block
(opening with a fixed untrusted-review-data preamble), and the review body adds
a consolidated all-findings prompt so demoted and carried findings hand an
agent their instruction too. The body opens with a host-derived stats line —
changeset size, severity tally, and a deterministic 1–5 review-effort estimate
— and renders the model's new optional per-file `walkthrough` as a collapsed
table whose paths are validated against the changeset like finding anchors
(fan-out children report `fileSummaries`, projected and merged by the
coordinator). Findings may carry an optional `category` chip rendered beside
the severity; demoted and carried-finding sections collapse into counted
`<details>` blocks. Oversized bodies shed the consolidated prompt first, then
the walkthrough, before any review item, and every omission stays announced.
Stale-review retirement matches both the categorized and the pre-category
inline first-line formats.
