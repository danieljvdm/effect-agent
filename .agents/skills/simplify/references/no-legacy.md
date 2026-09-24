# Pre-production no-legacy pass

Use only for an explicit request to remove legacy compatibility in pre-production
code. The request sets the goal; verify that the affected code has no production
consumer or persisted shape that requires the old path. Preserve a load-bearing
path when that premise does not hold, and explain its constraint.

Inspect the branch changes, including relevant untracked files. Look for adapters,
deprecated aliases, old/new branches, dual reads or writes, and migration helpers
that exist only to support a superseded path. Check callers, tests, and stored-data
contracts before removing them.

Collapse supported behavior onto the intended implementation. Remove obsolete
flags, documentation, and tests for the retired behavior together. Keep names
that describe the domain; rename transitional labels only where the distinction
has actually disappeared.

Verify the remaining behavior with the relevant checks. Judge the result by
whether the unnecessary path is gone and required behavior still works, not a
quota of deletions or a smaller diff. Report any compatibility retained and the
consumer, contract, or explicit user requirement that makes it necessary.
