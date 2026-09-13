---
"@effect-agent/platform-cloudflare": minor
"@effect-agent/platform-node": minor
"@effect-agent/pr-review": minor
"@effect-agent/sandbox-local": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/testing": minor
"@effect-agent/thread": minor
"@effect-agent/workflow": minor
"effect-agent": minor
---

Consolidate agent definitions, execution, capabilities, and sandbox contracts into `effect-agent`, and use kebab-case public module paths across framework packages.

BEHAVIOR CHANGE: Replace `@effect-agent/core`, `@effect-agent/engine`, `@effect-agent/capabilities`, and `@effect-agent/sandbox` dependencies with `effect-agent`; migrate direct imports such as `effect-agent/AgentRuntime` to `effect-agent/agent-runtime` and upgrade framework packages together.
