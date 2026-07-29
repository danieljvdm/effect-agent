# ADR-0007: Slim the toolchain and use the canonical Effect source

Status: **Accepted**

## Context

The first repository scaffold intentionally followed the Vite+ Effect/Cloudflare template closely.
After installation and validation, several template-derived pieces had no job in this
package-only project:

- an empty `@effect-agent/config` workspace wrapped static TypeScript JSON;
- a standalone patch script only called `effect-tsgo patch`;
- every package repeated the root-only `@effect/tsgo` tool;
- direct Vitest and Bun type declarations duplicated capabilities already supplied by Vite+ or
  were unused;
- the source submodule still referenced the now-archived `Effect-TS/effect-smol` repository.

The dependency graph also mixed Effect beta.98 with newer transitive Effect packages and with the
skills CLI's beta.90 runtime.

## Decision

- Keep only `@effect-agent/core`, `@effect-agent/engine`, and `@effect-agent/testing` in Phase 0.
- Put shared compiler options in root `tsconfig.base.json`.
- Install `@effect/tsgo` once at the root and invoke it directly from post-install.
- Use Vite+'s bundled test runner rather than declaring Vitest separately. Pin its bundled Vitest
  version through the root override when a test integration such as `@effect/vitest` peers on
  Vitest, so every test API resolves the same runner instance.
- Keep the Vite core alias because Bun needs a direct peer provider for the Vite+ toolchain.
- Pin the current Effect v4 beta and `@effect/platform-node` to one exact aligned release.
- Override the contributor skills CLI's older Effect pins so repository tooling uses the same
  runtime.
- Pin the GitHub skills dependency to its inspected commit.
- Use `Effect-TS/effect` at `repos/effect`, synchronized to the root catalog's
  `effect@<version>` tag.

## Consequences

- the workspace graph contains three product packages instead of four;
- there is one shared TypeScript config and one Effect-aware compiler installation;
- framework code and repository tooling resolve one Effect runtime;
- the source checkout follows the active canonical repository;
- Vite+ remains the source for formatting, linting, testing, building, and task orchestration;
- upgrades remain explicit catalog, lockfile, skills commit, and submodule changes.

## Superseded details

ADR-0006 remains the record choosing a package-only Vite+ monorepo. This ADR supersedes only its
`config` package and `effect-smol` source-checkout details.
