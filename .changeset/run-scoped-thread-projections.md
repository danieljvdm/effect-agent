---
"@effect-agent/thread": minor
---

Keep completed Steps and projected tool calls distinct when component IDs contain separators or are reused across runs.

BEHAVIOR CHANGE: Include `schemaVersion: 2` on manually created `ThreadProjection` values and `runId` on `SubagentInvocationState`; discard and rebuild checkpoints whose projection state fails Schema decoding.
