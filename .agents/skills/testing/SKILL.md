---
name: testing
description: Decide whether committed tests are warranted, design regression tests, or follow explicitly requested TDD.
license: MIT
---

# Testing

Follow the repository's testing policy and the user's requested scope. Adding
source code does not by itself justify committed tests. Prefer the cheapest
verification that proves the changed behavior.

- Whether to add, retain, or remove a test: [test selection](references/selection.md).
- A test boundary, substitute, or mocking decision: [test design](references/test-design.md).
- An explicit TDD, red-green, or test-first request: [TDD](references/tdd.md).
  Ordinary feature verification does not require this workflow.

Honor an explicit testing request within its scope. Express expectations
independently of implementation, exercise a stable public boundary, and follow
repository evidence and placement rules. Keep the final suite green.

TDD attribution is in [NOTICE](NOTICE); terms are in [LICENSE](LICENSE).
