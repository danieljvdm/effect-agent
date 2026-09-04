# Wrangler

Use the declared Wrangler version and command entrypoint. Inspect the relevant script, config, and installed help before composing arguments. With Vite+, use the repository task or `vp exec wrangler`. Install a missing tool only when the requested task needs it.

## Configuration and local development

Locate the owning Worker config and environment before changing a binding.
Prefer JSONC for new configurations when it fits repository conventions;
preserve an existing format for unrelated edits. Check fields against the
installed Wrangler config schema and regenerate binding types through the
repository's normal command after changing them.

Compatibility dates and flags change runtime behavior. Preserve the selected
date during routine edits. When an upgrade is requested or required, read the
relevant compatibility changes and verify affected behavior rather than setting
today's date automatically.

Inspect local and remote binding settings before starting development. Local
development can reach real services when remote bindings are enabled. Keep
private temporary servers separate from any preview managed by the host.
Reuse an existing dev process when it exercises the change.

Use the installed CLI's help for flags and subcommands. The repository's scripts
own ports, environment names, credential loading, and generated file locations.
Do not create a second workflow merely to reproduce an example from this skill.

References:

- [Configuration](https://developers.cloudflare.com/workers/wrangler/configuration/)
- [Compatibility dates](https://developers.cloudflare.com/workers/configuration/compatibility-dates/)
- [Local development](https://developers.cloudflare.com/workers/development-testing/)

## Operations

Identify the selected account, Worker or resource, environment, and local or
remote target. An environment flag does not guarantee every resource setting
is inherited; inspect the effective config and binding before acting.

For a deployment, use the repository's release procedure and required checks.
Use a dry run when it materially checks packaging or bindings. A dry run does
not prove runtime behavior or authorize deployment. Reuse any deployment
authorization already present in the task.

Preview the target and impact of deletion, migrations, or resource replacement.
Use existing backup or recovery procedures where the operation calls for them.
Inspect versions before a rollback; code rollback may not undo data changes.

After a timeout or partial failure, inspect the resulting resource or deployment
before retrying a mutation. Retry only the missing work. Stop and report the
remaining state when access is unavailable, the target is ambiguous, or further
retries could duplicate consequential work.

Use protected input or the repository's secret integration for credentials.
Preserve existing authentication instead of starting an interactive login
unnecessarily. Report deployment and resource identifiers without exposing
secrets or private preview capabilities.
