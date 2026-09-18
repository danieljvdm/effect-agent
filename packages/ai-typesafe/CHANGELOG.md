# @effect-agent/ai-typesafe

## 0.1.0-beta.107

### Patch Changes

- Updated dependencies []:
  - @effect-agent/ai-decision@0.1.0-beta.107

## 0.1.0-beta.106

### Patch Changes

- Updated dependencies []:
  - @effect-agent/ai-decision@0.1.0-beta.106

## 0.1.0-beta.105

### Patch Changes

- [#532](https://github.com/danieljvdm/effect-agent/pull/532) [`8e09da8`](https://github.com/danieljvdm/effect-agent/commit/8e09da806f0609bc23fa6fc15b1bed5ae0a0cb1d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require `TypeSafeClient.Config` and `HttpClient` when constructing the TypeSafe client.

  BEHAVIOR CHANGE: Replace `make(options)`, `layer(options)`, and `layerConfig(options)` with the `make` Effect or `layer` Layer; provide `TypeSafeClient.Config` (or `TypeSafeClient.Config.layer` for environment configuration) and apply HTTP policies to the supplied `HttpClient` instead of `transformClient`.

- Updated dependencies []:
  - @effect-agent/ai-decision@0.1.0-beta.105

## 0.1.0-beta.104

### Patch Changes

- Updated dependencies []:
  - @effect-agent/ai-decision@0.1.0-beta.104

## 0.1.0-beta.103

### Minor Changes

- [#527](https://github.com/danieljvdm/effect-agent/pull/527) [`f030933`](https://github.com/danieljvdm/effect-agent/commit/f030933c16926171436b4f92e7b920a8fbc89184) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add provider-neutral Decision Models with reusable, schema-encoded decision sets and typed choice, score, and probability queries. Supply Jev evaluations through the TypeSafe adapter with separate provider statistics and bounded support for rounded Choice probability totals in both the client and shared model.

### Patch Changes

- Updated dependencies [[`f030933`](https://github.com/danieljvdm/effect-agent/commit/f030933c16926171436b4f92e7b920a8fbc89184)]:
  - @effect-agent/ai-decision@0.1.0-beta.103

## 0.1.0-beta.102

## 0.1.0-beta.101

## 0.1.0-beta.100

### Minor Changes

- [#514](https://github.com/danieljvdm/effect-agent/pull/514) [`61a0a73`](https://github.com/danieljvdm/effect-agent/commit/61a0a73a2db6214073bf9472b83fe9466f7abf84) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add TypeSafe AI evaluations with inferred question keys and choice literals, validated probability distributions, and typed Effect failures. Compose the client with native Effect AI tools and explicit HTTP, retry, and timeout policies.
