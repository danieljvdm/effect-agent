# Failure-first development

This order is mandatory whenever testing a system in isolation, including
integration tests with substitutes. It also applies to an explicit TDD request.
An existing bug permits a reproducer before the fix; it does not permit unit
tests written after the fix. If implementation is already written, use existing
checks and direct workflow verification. Do not manufacture a retrospective
red-green cycle or create a new E2E suite to replace prohibited unit tests.

1. **Write all the ways the scoped system could fail.** Before implementation,
   record inputs, expected outcomes, and observable failures in task or working
   notes. Cover the relevant boundaries: invalid inputs and authority, state
   transitions, duplicates, ordering, partial writes, cancellation, retries,
   timeouts, and recovery. Include only applicable cases; identify uncertainty
   rather than pretending the inventory is exhaustive beyond the task's scope.
2. **Write the tests before the code.** Derive expectations from that inventory
   and the public contract, independently of the proposed implementation.
   Map the inventoried failure modes to existing proof first. Add only the
   necessary missing cases at the strongest boundary; no test-per-item quota.
   Run them and confirm meaningful red failures, not missing infrastructure
   or broken test setup. Existing guarantees may already pass; record those
   separately from the failures the implementation must fix.
3. **Implement, then rerun those tests.** Refactor with the suite green. If a new
   failure mode is discovered, extend the inventory and reproduce it before
   changing the implementation for it. Do not add post-hoc unit assertions
   merely to match code just written.

Use the repository's command authority, [selection rules](selection.md), and
[test boundaries](test-design.md). Reuse the behavior the user named and keep
the final suite green. This workflow does not require additional permission
within the already authorized task.
