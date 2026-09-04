# Test-driven development

Use the repository's testing policy, command authority, and public boundaries.
Reuse the behavior and test boundary the user already named. When those are
unspecified, choose the strongest relevant public boundary from the existing
design; ask only if the choice changes the intended behavior or scope.

Write a failing test that independently expresses the requested behavior.
Confirm the failure is meaningful, implement enough to pass, then refactor
when it improves the code while keeping the test green. Work in small slices
that can inform the next decision.

For a difficult choice of boundary or substitute, read
[test design](test-design.md). Avoid a new abstraction solely to
make implementation details easier to mock.

Keep the final suite green and obey the repository's evidence and placement
rules. A TDD request authorizes test-first work within the requested task; it
does not require a separate confirmation at every cycle.
