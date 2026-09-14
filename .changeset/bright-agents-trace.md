---
"effect-agent": patch
---

Label agent and model spans with GenAI operation names and agent, conversation, and model identity for agent dashboards. Update span-name filters from `AgentRuntime.run` to `invoke_agent <agent ID>` and from `LanguageModel.streamText` to `chat <model>`.
