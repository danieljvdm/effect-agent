# Workers runtime

Follow installed generated types and the repository command authority.

## Worker runtime boundaries

Keep request-specific state and I/O resources within the request or an explicit
application lifetime. Immutable constants can be shared; mutable globals must
not leak one request's identity or data into another.

Stream large or unbounded bodies rather than buffering them. Preserve stream
cancellation and backpressure, and avoid consuming a body twice. Bound work
that depends on client-controlled sizes or upstream responses.

Await work required for the response. Use the execution context for permitted
post-response work that fits its lifetime. `void` alone does not keep a Promise
alive or handle its failure. Use durable execution such as a queue or workflow
when the work must outlive that request lifetime.

Keep authentication, decoding, and error mapping at the appropriate boundary.
In Effect applications, preserve the existing service and Layer ownership.
Do not force a plain async example into an established Effect handler design.

Use the installed types and relevant [Workers runtime documentation](https://developers.cloudflare.com/workers/runtime-apis/)
to check limits and API behavior that affect the task.

## Worker configuration

Use generated binding types and the effective configuration for the target
environment. Prefer service and resource bindings when available; use external
APIs where the integration requires them. Preserve existing database connection
ownership and evaluate Hyperdrive only for the relevant connection needs.

Add compatibility flags when required by the dependencies actually in use.
Treat compatibility-date changes as runtime changes with their own verification.
Keep secrets in the repository's configured secret store or binding mechanism.

For error capture, structured logs, tracing, sampling, and sensitive fields,
follow the repository's reporting policy. A narrow Worker edit is not a reason
to install another telemetry system or change global sampling.

Retrieve the relevant [Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/)
when a platform-specific question remains. Validate signatures against the
installed toolchain instead of downloading newer types to judge older code.

## Review and verification

Trace the changed request or event path, the capabilities it uses, and its
failure and cancellation behavior. Check configuration only where it affects
that path. Cite the concrete failure or risk in each finding; unrelated
hardening is not a prerequisite for completing the requested change.

Inspect surrounding code when needed to establish ownership. For uncertain API
behavior, consult installed declarations and the relevant official documentation
before reporting a violation.

Run the repository's required commands. Exercise the affected handler with its
actual runtime or the project's existing integration setup when needed.
Use the repository's testing policy to decide whether to add a regression test;
a new Worker does not imply a new test suite.

Report what ran, the observable result, and any runtime behavior left unverified.
Keep an audit read-only unless fixes are also requested.
