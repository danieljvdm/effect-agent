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

Authentication uses matching versions of `@yielded/auth` and `@yielded/auth-persistence`.
The persistence package supplies the Drizzle adapters for the app-owned SQLite tables.
GitHub sign-in creates flow IDs on the server; the browser retains the returned ID for the
callback. Email verification keys are supplied through `ProofKeys` using `AUTH_PROOF_KEY`.

When upgrading from Auth beta.5 to beta.7, keep the existing database, namespaces, and key IDs.
Users with an email code requested before the upgrade should start a fresh email flow because
the proof template identifier changed. Existing accounts and sessions are retained.

The production deployment workflow enables Cloudflare traces after verifying request URL
query-string redaction, keeping authentication callback parameters out of platform telemetry.
Direct Alchemy deployments leave traces disabled because its SDK does not yet expose that setting.

From the repository root:

```sh
vp install
vp run -F @effect-agent/example-travel-planner dev
```
