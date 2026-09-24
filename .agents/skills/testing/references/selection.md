# Test selection

## Default: write no new tests

A feature request or "test/verify this" authorizes verification, not new
committed automation. Run existing checks or exercise the real workflow and
save [replayable evidence](e2e.md). A missing E2E suite does not oblige you to
create one. Types, static checks, and a focused interaction may already suffice.

Before adding or expanding a committed test of any kind, both must hold:

- You are reproducing a current failure, or a human explicitly requested
  committed automated tests, TDD, or a named test seam.
- You can name the concrete uncovered failure, explain why existing checks or
  direct workflow verification are insufficient, and justify the continuing
  maintenance and runtime cost.

These are necessary conditions, not a test-writing quota. Apply them to added
cases, parameter tables, assertions, fixtures, mocks, and harnesses inside
existing suites as well as new files. An incident citation or allowlist alone
does not establish value. Do not invent failure scenarios to qualify a test.

Prefer the smallest addition at an existing real boundary when automation is
justified. Never turn a rejected unit test into a browser test, larger
integration suite, or "E2E" harness. Do not add replacements merely to preserve
the count or apparent coverage of deleted tests. Do not copy business logic into
fixtures or build fake applications, elaborate setup, or new reporting systems
merely to comply with a testing preference.

Temporary verification code also needs a concrete question and proportional
cost. Prefer direct tools and existing commands; use a small task-local script
only when it materially simplifies the proof. Leaving a harness uncommitted
does not justify building it. Stop once the requested outcomes are proven.

## Explain why automation is necessary

For a new regression, cite its pre-existing public issue or offending commit
beside the test. For an explicit human request, record the requested test seam
before implementation. A current PR, invented incident, or private customer
identifier is not evidence. Provenance does not establish necessity.

Review must check additions inside existing declarations, fixtures and parameter
tables as well as new test files. A passing suite does not establish value or
test-first order.

This is a public library, which makes some stable contract checks valuable but
is not a blanket exemption. Preserve narrow compile-time checks for costly
silent error/requirement inference regressions and independent adapter proof
where workflow verification cannot expose the failure. Do not execute runtime
tests merely to repeat type declarations, schema definitions or export maps.

## Exceptions for isolation

Explain the specific failure E2E cannot adequately exercise, such as a storage
crash window or a nondeterministic race. Convenience, coverage targets, package
size, or being a library do not establish that need. Choose the real integration
boundary before a unit seam. The selection rules above still apply.

Write the failure inventory and necessary failing tests before implementation
or the fix, following [failure-first development](tdd.md). Use existing proof
for already-covered cases; an inventory does not require a new test per item.

## Audit existing tests

Judge what a test can actually catch, not its length, file name, or citation.
Delete tests that only restate schemas, constants, configuration, or types;
assert internal call order or mock setup; duplicate the implementation's
algorithm; or test private graphs and incidental rendering details. Remove
duplicate coverage when a stronger retained scenario catches the same failure.

Retention also needs a concrete answer: which costly production failure would
escape the remaining checks, and why is repeatable workflow verification
insufficient? A citation, real database, browser, or authorization/recovery
label does not answer that question. Apply this bar to the whole suite,
including the fake application or infrastructure needed to run it.

Retain independent expected results at stable boundaries for failures that E2E
cannot reliably force, such as data loss, authorization races, or crash recovery.
Keep only the cases needed to expose those failures; useful protection does not
justify the surrounding feature matrix. Do not delete such protection solely
because it predates this policy or invent a historical test-first claim.

Keep one regression per incident at the strongest seam, with only the cases
needed to distinguish its failure modes. Weigh suite runtime and maintenance
against the risk it protects. Delete orphaned fixtures, mocks, and runner
entries with the tests that required them; preserve shared live acceptance
infrastructure. Do not write replacement unit tests as part of cleanup.
