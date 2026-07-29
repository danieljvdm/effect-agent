# ADR-0006: Use a phase-gated, package-only Vite+ monorepo

Status: **Accepted; repository-shape details superseded by ADR-0007**

## Context

The implementation will be carried out by many agents over several phases. It needs one
reproducible toolchain, explicit package boundaries, exact Effect versions, and a local checkout of
the matching Effect v4 source. The referenced Vite+ Effect/Cloudflare template already provides
most of that machinery, but its browser and Worker applications are not part of this product.

Creating every future package now would make speculative boundaries look settled and encourage
implementation to spread ahead of the roadmap. Keeping application shells would imply product
surfaces that have not been chosen.

## Decision

Use a private Bun workspace with Vite+ as the shared formatter, linter, test runner, library
builder, staged-check runner, and task orchestrator.

- The workspace contains `packages/*` and no `apps/*`.
- Phase 0 contains `config`, `core`, `engine`, and `testing`.
- Later packages are created only when their roadmap phase begins.
- Shared dependency versions live in the root Bun catalog.
- Effect v4 is pinned exactly.
- A shallow `effect-smol` submodule is checked out at the `effect@<catalog version>` tag.
- `@danieljvdm/agent-skills` supplies checked-in contributor instructions.
- Contributor agent skills are not runtime framework Skills.
- All packages remain private and use working `@effect-agent/*` names.

Cloudflare remains a required later platform, but it arrives through
`@effect-agent/storage-cloudflare` and `@effect-agent/platform-cloudflare` library packages. No
application or deployment entrypoint is scaffolded now.

## Consequences

- one command, `bun run ready`, checks the current repository;
- Vite+ derives task order from real package dependencies;
- all workspace packages resolve one Effect version;
- implementation agents can inspect the exact matching Effect source;
- the repository does not carry unused React, Wrangler, D1, or Worker code;
- package creation becomes a visible roadmap decision;
- Bun and Vite+ beta upgrades are deliberate repository-wide changes;
- publication metadata and `dist` export maps remain future work.

## Rejected alternatives

### Keep the template applications

Rejected because neither a browser application nor a deployable Worker is part of the current
product surface.

### Create all planned packages immediately

Rejected because empty speculative packages create false architectural commitments. The
architecture documents the future graph; the filesystem reflects the current phase.

### Use contributor skills as runtime dependencies

Rejected because agent instructions for working on the repository are unrelated to the
framework's model-visible, versioned runtime Skill capability.
