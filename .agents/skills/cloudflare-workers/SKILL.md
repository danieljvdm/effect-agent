---
name: cloudflare-workers
description: Build, review, or operate Cloudflare Workers, Durable Objects, and Sandbox SDK applications with Wrangler.
license: Apache-2.0
---

# Cloudflare Workers

Use the repository's architecture, commands, installed types, and runtime
compatibility settings. Current documentation explains APIs; a maintenance task
does not by itself call for a dependency or compatibility-date upgrade.

Read for the changed boundary:

- Worker handlers, streams, bindings, and verification: [Workers](references/workers.md).
- Durable Object identity, storage, alarms, and WebSockets: [Durable Objects](references/durable-objects.md).
- Wrangler commands, environments, deployment, and secrets: [Wrangler](references/wrangler.md).
- An application using `@cloudflare/sandbox`: [Sandbox SDK](references/sandbox.md).
  Ordinary commands inside an agent sandbox do not require the SDK.

Resolve the account, environment, and local or remote target before mutations.
Preserve deployed storage migrations, existing authorization, and host ownership
of task containers, previews, authentication, and capability URLs. Follow the
repository's testing policy and verify the behavior at its actual runtime boundary.

Attribution is in [NOTICE](NOTICE); terms are in [LICENSE](LICENSE).
