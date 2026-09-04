# Test design

Observe behavior through the public boundary that owns it. Assert an expected
result derived from the requirement, a worked example, or independent fixture,
rather than repeating the implementation's calculation.

Choose the boundary that exposes the failure with the fewest irrelevant moving
parts. Persistence requirements may need real storage and migrations; UI
interaction failures may need a browser. Follow the repository's substitutes
and avoid mocks of internal collaborators that only prove the current wiring.

Control nondeterministic inputs such as time or an external service when the
failure requires it. A substitute must faithfully provide the behavior its
interface promises. Keep its use local unless several justified tests need it.

Use assertions that distinguish the failure from success and survive unrelated
refactors. Do not add companion tests merely because a new source file exists.
