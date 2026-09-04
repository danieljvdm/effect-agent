# Cloudflare Worker API

Use the `effect-development` and `cloudflare-workers` skills when
they are available. Read current installed or primary Cloudflare documentation
before choosing Wrangler configuration, compatibility dates, bindings, or local
development commands.

## Boundary

Model the Worker as a deployable package with an explicit host boundary. Keep
contracts and application services independent of Worker globals. Translate
bindings, requests, execution context, and platform failures at the entry point,
then assemble the application layer once.

When the repository uses Effect, define shared HTTP contracts before handlers,
keep failures typed through the application, and connect the final handler to
the Worker adapter. Generate clients or OpenAPI from the same contract when the
product needs them.

## Configuration

Derive binding types from Wrangler configuration and include the generated file
in the package's typecheck without hand-maintained ambient duplicates. Separate
secret names from secret values. Scope environment-specific resources in
Wrangler rather than branching through application code.

Integrate Worker development, type generation, deployment, and dry-run commands
into the repository's existing command authority. Add the narrow Cloudflare
skills that match the selected products; a Worker alone does not justify every
Cloudflare skill.

## Completion

Generate bindings, run repository validation, build or dry-run the Worker, and
exercise one request through the actual Worker entry point. Account for streaming,
request cancellation, background work, observability, and binding failures before
declaring the API ready.
