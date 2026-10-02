---
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-pr-review": minor
"@yielded/agent-sandbox-local": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-testing": minor
"@yielded/agent-workflow": minor
"@yielded/agent": minor
---

Consolidate agent definitions, execution, capabilities, and sandbox contracts into `@yielded/agent`, and use kebab-case public module paths across framework packages.

BEHAVIOR CHANGE: Replace `@effect-agent/core`, `@effect-agent/engine`, `@effect-agent/capabilities`, and `@effect-agent/sandbox` dependencies with `@yielded/agent`; migrate direct imports such as `@yielded/agent/AgentRuntime` to `@yielded/agent/agent-runtime` and upgrade framework packages together.
