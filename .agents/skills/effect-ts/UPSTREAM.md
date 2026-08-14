# Upstream Baseline

The general Effect guides in this skill were adapted from
[`Effect-TS/skills`](https://github.com/Effect-TS/skills) at commit
`a8b6bb40d1d4d550b49c0ff7a624b5e6da500a24`.

They have been modified to:

- remove the mandatory `.repos/effect` checkout
- target the current canonical `Effect-TS/effect` v4 source
- update version-sensitive guidance from the upstream beta.66 snapshot to
  beta.102
- preserve this repository's service ownership, layer construction,
  schema-first modeling, type-boundary, HTTP, testing, logging, audit, and CLI
  conventions

## Local Authoring Checkout

This repository keeps an ignored, version-matched Effect checkout for
validating source paths and beta-sensitive APIs. `vp i` runs the equivalent
dev-kit setup automatically; it can also be invoked directly:

```bash
./bin/dev-kit.mjs effect sync
```

Update the checkout, package dependencies, review baseline, feature index, and
stale-API tests together when moving to a newer Effect beta.
