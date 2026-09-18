---
"effect-agent": minor
---

Add `SelectiveCompactor.layer` to prune old tool-result bodies through a supplied DecisionModel and optional effectful retention question before falling back to the existing summary or rollover strategy. Support exact durable selections and metered auxiliary compaction inference with independent usage accounting.

BEHAVIOR CHANGE: Direct `ContextCompactor.compact` harnesses must provide `CompactionEvaluator`; AgentRuntime supplies it automatically.
