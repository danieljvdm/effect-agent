---
"effect-agent": patch
---

Add `ToolDiscovery.fromDecisionModel` to discover tools by semantic relevance through any decision provider, with bounded evaluation and typed provider and observer failures. Construct discovery as an Effect and override its evaluation settings through the shared `ToolSelector.DecisionConfig` service.
