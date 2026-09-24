# Test design

Observe behavior through the public boundary that owns it. Assert an expected
result derived from the requirement, a worked example, or independent fixture,
rather than repeating the implementation's calculation.

Use the real workflow by default. If isolation is justified, choose the boundary
that owns the failure and follow [failure-first development](tdd.md) before
implementation. Do not narrow to a private helper merely because it is easier
to mock. Avoid mocks of internal collaborators that only prove current wiring.

Control nondeterministic inputs such as time or an external service when the
failure requires it. A substitute must faithfully provide the behavior its
interface promises. Keep its use local unless several justified tests need it.

Use assertions that distinguish the failure from success and survive unrelated
refactors. Do not add companion tests merely because a new source file exists.

Use the actual runtime required by the failure: the existing SQLite, Node
process-loss, or workerd entrypoint for that boundary. Control time through
existing Effect test services rather than wall-clock sleeps. Preserve published
storage-upgrade contracts; this repository has external consumers.

Keep tests under the owning package's `test/` directory and reuse its current
runner. Keep only helpers needed by retained cases. Public `@effect-agent/testing`
APIs and live release acceptance infrastructure are library/workflow code;
audit their callers and contracts before removing them with a test.
