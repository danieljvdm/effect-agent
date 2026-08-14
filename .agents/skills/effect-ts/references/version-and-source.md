# Effect Version And Source Strategy

Use this guide before installing Effect packages or relying on exact API names,
signatures, exports, or source paths.

## Review Baseline

This skill was last reviewed on 2026-07-29 against:

- `effect@4.0.0-beta.102`
- matching `4.0.0-beta.102` releases of the v4 `@effect/*` packages
- canonical source from `Effect-TS/effect`
- official Effect skill commit
  `a8b6bb40d1d4d550b49c0ff7a624b5e6da500a24`

The beta is intentionally volatile. Review metadata records what was checked;
it does not override the target repository's installed version.

## Determine The Target Version

Before editing:

1. Read the target repository's package manifest and lockfile.
2. Identify the resolved `effect` version and every installed `@effect/*`
   version.
3. Determine whether the repository is on Effect v3 or the v4 beta.
4. Keep guidance and source research on that same release line.

Do not silently upgrade an existing repository. If its Effect packages are
misaligned, report the mismatch and treat version alignment as a separate
change.

## Installing Effect v4

For a new Effect v4 installation:

1. Resolve the current `effect@beta` version from the package registry.
2. Install `effect` at that exact version.
3. Install only the runtime and integration packages the task requires.
4. Pin every v4 `@effect/*` package to the same exact beta version.

`effect@latest` is the stable v3 release line until Effect v4 leaves beta.
Do not use generic v3 packages such as `@effect/platform` with Effect v4.
Effect v4 places many capabilities under `effect/unstable/*`; install separate
packages only for runtime-, provider-, driver-, or tool-specific integrations.

## Source Research

Use this order:

1. Search the installed package declarations and source in the target
   repository.
2. Check the target repository for established usage of the same API.
3. Inspect the canonical `Effect-TS/effect` source at the matching tag or
   commit.
4. Use current `main` only when intentionally researching a future upgrade.

A project-local checkout at canonical `Effect-TS/effect` source may be used when the repository
already provides one, but it is optional. Never pause ordinary Effect work to
force the user to add a subtree, submodule, or clone.

## V4 Migration Checks

When adapting older examples, verify them against the installed v4 release.
Common v4 changes include:

- define services with `Context.Service`, not the removed `Context.Tag`,
  `Context.GenericTag`, `Effect.Tag`, or `Effect.Service`
- prefer yielding the service key directly
- use `Effect.catch`, `Effect.catchCause`, and `Effect.catchFilter` rather than
  the renamed v3 `catchAll`, `catchAllCause`, and `catchSome`
- use `Context` plus `Effect.runForkWith` rather than the removed generic
  `Runtime<R>` and `Effect.runtime`
- import consolidated capabilities from `effect/unstable/*` rather than
  obsolete standalone packages
- verify Schema class, union, decoding, and transformation signatures because
  they changed throughout the v4 beta

## Completion Check

Before completing version-sensitive work:

- every code example uses APIs present in the target's resolved packages
- every installed Effect v4 package resolves to the same beta
- no source path is assumed solely because it appears in this skill
- current-main guidance is not presented as installed-version behavior
