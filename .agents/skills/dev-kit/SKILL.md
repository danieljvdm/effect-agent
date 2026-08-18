---
name: dev-kit
description: Set up or reshape TypeScript repositories through agent-led, repo-owned changes. Use when scaffolding a new repository, establishing or modernizing project tooling, adding an application or package, composing a Cloudflare Worker API, ejecting legacy dev-kit.jsonc and dev-kit.lock.json management, or copying and updating curated agent skills with the transient Dev Kit CLI.
---

# Dev Kit

Build repositories from intent, then leave every output under normal repository
ownership. Use the transient CLI for catalog and migration mechanics; use agent
judgment for architecture and configuration.

## Route

Read every reference whose branch applies before editing:

- Read [repository-setup.md](references/repository-setup.md) for every new,
  existing, or expanded repository.
- Read [default-typescript-repository.md](references/default-typescript-repository.md)
  when the user wants the Dev Kit default or a Vite+ TypeScript foundation.
- Read [cloudflare-worker-api.md](references/cloudflare-worker-api.md) when adding
  a Cloudflare Worker, Worker API, bindings, Durable Objects, or Wrangler.
- Read [legacy-eject.md](references/legacy-eject.md) when `dev-kit.jsonc`,
  `dev-kit.lock.json`, `.dev-kit/state.json`, managed markers, or Dev Kit config
  imports exist.
- Read [skills.md](references/skills.md) when discovering, adding, refreshing,
  merging, or detaching repository skills.

## Workflow

1. Establish the Git root and read repository instructions. Inventory package
   manifests, workspaces, tool configuration, source boundaries, CI, and current
   validation commands. Finish with every existing convention that constrains
   the change accounted for.
2. Translate the request into outcomes and invariants. Resolve consequential
   choices with the user; infer naming and file placement from repository
   evidence. Finish with a coherent target architecture rather than a list of
   templates.
3. Present the material file, dependency, and command changes before broad or
   destructive work. Treat existing files as repository-owned and reconcile
   them in place.
4. Implement against current installed APIs and local documentation. When the
   repository uses Effect, read `node_modules/effect/AGENTS.md` completely before
   writing Effect code and follow its relevant references.
5. Run the repository's command authority and exercise the changed behavior.
   Finish when validation passes and the generated setup works from the same
   entry points future contributors will use.
6. Hand off the architecture and any deliberate choices. The finished repository
   has no Dev Kit dependency, manifest, lock, lifecycle hook, or managed output.
