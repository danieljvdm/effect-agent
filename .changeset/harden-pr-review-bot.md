---
"@effect-agent/pr-review": minor
---

Harden full-diff review coverage, host-scoped selection provenance, bounded fan-out retries, and
policy-failure diagnostics so oversized reviews fail closed instead of silently reporting
incomplete results. Selection issuance and verification now require one explicit per-composition
Effect service instead of process-wide mutable authority.
