---
name: effect-development
description: Set up Effect, implement Atom React state, HTTP APIs or CLIs, and review Effect architecture.
---

# Effect development

Read the repository's `node_modules/effect/AGENTS.md` completely before writing
or evaluating Effect code, then follow its relevant links. Installed declarations
and repository policy own version-sensitive APIs and verification commands.

Read only the guide for the requested boundary:

- React queries, mutations, shared state, workflows, or SSR: [Effect Atom](references/atom/index.md).
- HTTP contracts, handlers, middleware, or server runtimes: [HTTP APIs](references/api/index.md).
- Scripts, CI automation, or command-line applications: [CLIs](references/cli/index.md).
- A requested architecture review or service-boundary refactor: [architecture](references/architecture/index.md).
- Adding Effect dependencies or its agent guidance: [setup](references/setup.md).

For Effect React applications, Effect Atom owns shared state and server data;
React renders and dispatches. View-local state can stay in React. Keep business
logic in Effect workflows and declare cross-query invalidation as mutation
reactivity keys. Read the Atom guide for runtime and registry ownership.

Preserve the requested scope. A component edit does not require a server rewrite
or architecture audit. Follow repository policy for committed tests; changes to
an atom, endpoint, or script do not by themselves justify a new test.
