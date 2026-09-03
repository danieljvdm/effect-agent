---
"@effect-agent/workflow": minor
"@effect-agent/thread": minor
"@effect-agent/platform-node": minor
---

Run registered agents inside native Effect Workflow handlers with `AgentWorkflow.execute` and suspend until their typed results are available. **BEHAVIOR CHANGE:** supply `principal` to `WorkflowAgentHost.layer`; custom dispatch stores must return the retained intent from `put`, preserve and atomically attach its completion token, and compare the full intent before removal.
