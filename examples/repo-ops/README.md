# Repo-ops: the evidence auditor

A deterministic integration example that checks quoted test titles in fixture
documents against a repository tree and writes an audit report after approval.
Run it with `vp run -F @effect-agent/example-repo-ops test`.

The test supplies a scripted model, a temporary fixture tree, and a SQLite-backed
`NodeDurableAgentRuntime`. Reads use structured `SandboxRequest` values for
`/bin/ls`, `/bin/cat`, and `/usr/bin/grep` through `@effect-agent/sandbox-local`.
Model-supplied paths are validated, output and execution time are bounded, and
the local sandbox explicitly reports that it is unisolated.

Each document is audited inside a named Durable Step, `audit:{document}`. A
resumed attempt reuses committed rows; an uncommitted read-only step may execute
again. The report Tool declares an idempotent write and suspends until an
approval decision is recorded. The test verifies the durable records, absence
of the report before approval, and the final file and settlement after approval.

The suite requires no credentials or network access.
