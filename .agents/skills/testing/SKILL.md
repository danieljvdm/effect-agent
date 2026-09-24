---
name: testing
description: Plan E2E verification and reproducible evidence, audit test value, or test an isolated system before implementation.
license: MIT
---

# Testing

Default to writing no new tests or test infrastructure. Verification remains
required: use existing checks and direct observation of the running workflow.
Strongly prefer E2E as the sole testing mechanism for complex features; running
a workflow and saving evidence usually needs no new automated suite.

Never write unit tests after writing the implementation. Never replace an
unnecessary unit test with a larger integration or E2E test. Read
[test selection](references/selection.md) before writing or expanding any test,
fixture, mock, harness, or reporting code, including temporary scripts.

Before substantial work, identify the requested observable outcomes and the
cheapest sufficient proof for each in working notes. Add acceptance scope only
for a requirement or a concrete risk introduced by the change. Simple edits
need no separate acceptance document or new tests.

- Plan or run E2E verification: [workflows and artifacts](references/e2e.md),
  then use the existing consumer, CLI, adapter, or deployed acceptance entrypoint.
  Every run must finish with a verifiable artifact and enough information to
  repeat it.
- Audit existing tests: [test selection](references/selection.md).
- Isolation is necessary, or a human requests TDD: first read
  [failure-first development](references/tdd.md) and
  [boundaries and placement](references/test-design.md). Write all the ways
  the scoped system could fail, then the failing tests, then the code.

An incident URL or explicit test request does not waive necessity or test-first
order. Keep the final suite green and stop when the requested outcomes have
sufficient evidence. Test count, coverage, and artifact volume are not goals.

TDD attribution is in [NOTICE](NOTICE); terms are in [LICENSE](LICENSE).
