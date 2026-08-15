---
name: testing
description: Set an extremely high bar for automated tests. Use whenever software work raises the question of adding, changing, retaining, or removing tests.
---

# Testing

**If in doubt, don't test it.**

Committed tests must prove durable value. Tests are permanent product code, not a reflexive companion to each source file.

Write a test only when every condition holds:

- It protects stable product behavior rather than an implementation detail.
- A regression is plausible and would be costly.
- The behavior is observable through a stable public seam.
- The test will survive likely refactors.
- The expected result is independent of the implementation.
- Existing tests, types, static checks, or a smoke check cannot cover the risk adequately.

If any condition fails, skip the test and use the cheapest sufficient verification instead. Moving MVP behavior often warrants no committed tests.

Test capabilities, not files. Use the fewest tests that protect the risk, and ignore coverage targets unless the user explicitly asks for them.

Never commit an intentionally failing or red-only suite. Keep time-driven tests
deterministic instead of stabilizing them with wall-clock sleeps. A shared fake
or in-memory implementation must fully provide the behavior its name promises;
keep partial substitutes local to the tests that need them.
