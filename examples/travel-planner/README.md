# Travel planner

An Effect Agent example for planning trips through chat or voice, researching destinations
with background agents, saving itineraries, and building standalone trip websites.

It uses Cloudflare:

- **Workers** to serve the TanStack Start UI and APIs.
- **Durable Objects with SQLite** to persist accounts, conversations, trips, and agent execution.
- **Browser Run** to research travel websites.
- **Artifacts** to version generated trip-site source code.
- **Workflows and Sandbox containers** to build sites, **R2** to store builds, and
  **Dynamic Workers** to serve them.
- **Email Sending** for email sign-in, alongside GitHub OAuth.

The app consumes published Effect Agent packages and uses Effect Atom for client state.
[alchemy.run.ts](alchemy.run.ts) defines the Cloudflare resources and required configuration.

The production deployment workflow enables Cloudflare traces after verifying request URL
query-string redaction, keeping authentication callback parameters out of platform telemetry.
Direct Alchemy deployments leave traces disabled because its SDK does not yet expose that setting.

From the repository root:

```sh
vp install
vp run -F @effect-agent/example-travel-planner dev
```
