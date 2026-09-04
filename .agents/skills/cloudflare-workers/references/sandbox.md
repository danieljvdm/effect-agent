# Cloudflare Sandbox SDK

Inspect the installed SDK, Worker bindings, image, and lifecycle owner. Docker is relevant to local container execution, not every remote SDK or documentation task. Preserve host ownership of containers and previews.

## Sandbox setup

Inspect the existing Worker entrypoint, Durable Object binding and migrations,
container image, and instance settings. Export the Sandbox class or the project's
subclass in the way required by the installed SDK. Match the binding to that
export; preserve deployed class names and migration history.

Select the image and SDK versions together using the project's lock and image
policy. Check compatibility before upgrading. Add only dependencies needed by
the workload; avoid pinning all projects to an example image tag or capacity.

Use Docker when exercising local container behavior. Prefer the repository's
existing local or deployed test route and report a missing capability when it
prevents verification. Do not install infrastructure just to inspect SDK code.

For new setup or a missing platform detail, use [the SDK getting-started guide](https://developers.cloudflare.com/sandbox/get-started/)
and [examples](https://github.com/cloudflare/sandbox-sdk/tree/main/examples).
Treat them as examples to adapt to the target repository.

## Execution and resource ownership

Choose a sandbox identity that matches the intended user or task isolation.
Reusing an ID can reuse state; a container boundary does not authorize sharing
one user's files or interpreter context with another.

Use command execution for shell workloads and interpreter contexts for stateful
language execution with structured results. Inspect the installed API for
timeouts, streaming, cancellation, exit codes, and file path semantics. Handle
process failure explicitly instead of treating successful transport as a
successful command.

Track the lifetime of processes, contexts, and exposed ports. The owner of a
temporary resource cleans it up, including on cancellation or partial failure.
Keep a user-requested persistent session alive. Do not destroy a host-managed or
shared sandbox merely because one command finished.

Expose a port only when the task calls for it and the host permits it. Treat
preview URLs as capabilities and publish them only through the host's approved
path. Diagnose domain and exposure settings from the current
[service exposure guide](https://developers.cloudflare.com/sandbox/guides/expose-services/).

Before retrying a command that may have written data, inspect its outcome or
use an idempotent operation. Stop when cleanup or authorization cannot be
established and report the exact resource still outstanding.
