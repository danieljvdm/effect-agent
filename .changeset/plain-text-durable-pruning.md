---
"@effect-agent/engine": patch
"@effect-agent/thread": patch
---

Support schema-validated plain-text final replies with `Output.text(schema)`. Honor prompt targets when pruning older tool results and durably replay pruning of fully settled current-run batches while preserving the newest result and incomplete work.
