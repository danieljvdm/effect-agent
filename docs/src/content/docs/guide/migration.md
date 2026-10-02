---
title: Migrate from Effect Agent
description: Move dependencies and imports from Effect Agent to Yielded Agent.
---

Yielded Agent is the new name for Effect Agent. The repository is
[`yielded-dev/agent`](https://github.com/yielded-dev/agent), and documentation lives
at [yielded.dev/agent/](https://yielded.dev/agent/). Existing repository and
`effect-agent.com` page links redirect to their corresponding new locations.

## Packages

Install `@yielded/agent@beta` and replace each adapter you use with its new name.
Keep all framework packages on the same release.

```sh
bun remove effect-agent
bun add @yielded/agent@beta
```

| Previous package                    | New package                          |
| ----------------------------------- | ------------------------------------ |
| `effect-agent`                      | `@yielded/agent`                     |
| `@effect-agent/ai-decision`         | `@yielded/agent-ai-decision`         |
| `@effect-agent/platform-cloudflare` | `@yielded/agent-platform-cloudflare` |
| `@effect-agent/platform-node`       | `@yielded/agent-platform-node`       |
| `@effect-agent/pr-review`           | `@yielded/agent-pr-review`           |
| `@effect-agent/sandbox-local`       | `@yielded/agent-sandbox-local`       |
| `@effect-agent/storage-cloudflare`  | `@yielded/agent-storage-cloudflare`  |
| `@effect-agent/storage-memory`      | `@yielded/agent-storage-memory`      |
| `@effect-agent/storage-postgres`    | `@yielded/agent-storage-postgres`    |
| `@effect-agent/storage-sql`         | `@yielded/agent-storage-sql`         |
| `@effect-agent/storage-sqlite`      | `@yielded/agent-storage-sqlite`      |
| `@effect-agent/testing`             | `@yielded/agent-testing`             |
| `@effect-agent/workflow`            | `@yielded/agent-workflow`            |

Previously published packages remain on npm. npm does not redirect dependencies
to a new package name; update your dependency declarations and lockfile.

## Imports

Replace the package prefix, keeping the module subpath:

```ts
import { Agent, AgentRuntime } from "@yielded/agent";
import * as InMemory from "@yielded/agent/in-memory";
import { NodeDurableHost } from "@yielded/agent-platform-node";
```

The rename preserves APIs, service identities, schema brands, and stored formats.
It does not require a data migration. Update all framework dependencies together;
avoid installing both names in the same application.

## GitHub Action

Change `uses: danieljvdm/effect-agent/action@action-v1` to
`uses: yielded-dev/agent/action@action-v1`. GitHub does not redirect Action references.
The existing `@effect-agent` review commands and bot identity remain unchanged.
