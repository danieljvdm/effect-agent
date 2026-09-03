# effect-agent

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2)]:
  - @effect-agent/capabilities@0.1.0-beta.46
  - @effect-agent/core@0.1.0-beta.46
  - @effect-agent/engine@0.1.0-beta.46

## 0.1.0-beta.45

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.45
  - @effect-agent/core@0.1.0-beta.45
  - @effect-agent/engine@0.1.0-beta.45

## 0.1.0-beta.44

### Patch Changes

- [#307](https://github.com/danieljvdm/effect-agent/pull/307) [`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

  BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.

- Updated dependencies [[`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41)]:
  - @effect-agent/core@0.1.0-beta.44
  - @effect-agent/engine@0.1.0-beta.44
  - @effect-agent/capabilities@0.1.0-beta.44

## 0.1.0-beta.43

### Patch Changes

- Updated dependencies [[`4532f8d`](https://github.com/danieljvdm/effect-agent/commit/4532f8d5c65b0d41532e9ebe0212c64a0a63f678)]:
  - @effect-agent/capabilities@0.1.0-beta.43
  - @effect-agent/core@0.1.0-beta.43
  - @effect-agent/engine@0.1.0-beta.43

## 0.1.0-beta.42

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.42
  - @effect-agent/core@0.1.0-beta.42
  - @effect-agent/engine@0.1.0-beta.42

## 0.1.0-beta.41

### Patch Changes

- Updated dependencies [[`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1), [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587)]:
  - @effect-agent/core@0.1.0-beta.41
  - @effect-agent/capabilities@0.1.0-beta.41
  - @effect-agent/engine@0.1.0-beta.41

## 0.1.0-beta.40

### Patch Changes

- Updated dependencies [[`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`0fbcbbf`](https://github.com/danieljvdm/effect-agent/commit/0fbcbbf3c8c2ca7595543e545baddb0c6f965436)]:
  - @effect-agent/capabilities@0.1.0-beta.40
  - @effect-agent/core@0.1.0-beta.40
  - @effect-agent/engine@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

### Patch Changes

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- Updated dependencies [[`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268), [`0d88d90`](https://github.com/danieljvdm/effect-agent/commit/0d88d90443e7d35e34799f4458d274fde99e0859), [`79fbd8b`](https://github.com/danieljvdm/effect-agent/commit/79fbd8b755434a162629a534478e188636d186fe), [`4c458e4`](https://github.com/danieljvdm/effect-agent/commit/4c458e43738bb243d1e343c97ecfd49e3b41ca9f), [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`655bf5f`](https://github.com/danieljvdm/effect-agent/commit/655bf5f217dce1865c97ce613246c27846bfaf8a), [`d004a36`](https://github.com/danieljvdm/effect-agent/commit/d004a361518c23cdc81f1768e5ab31560e014935), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e), [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748)]:
  - @effect-agent/capabilities@0.1.0-beta.39
  - @effect-agent/engine@0.1.0-beta.39
  - @effect-agent/core@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.38
  - @effect-agent/core@0.1.0-beta.38
  - @effect-agent/engine@0.1.0-beta.38

## 0.1.0-beta.37

### Patch Changes

- Updated dependencies [[`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea)]:
  - @effect-agent/engine@0.1.0-beta.37
  - @effect-agent/core@0.1.0-beta.37
  - @effect-agent/capabilities@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.36
  - @effect-agent/core@0.1.0-beta.36
  - @effect-agent/engine@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.35
  - @effect-agent/core@0.1.0-beta.35
  - @effect-agent/engine@0.1.0-beta.35

## 0.1.0-beta.34

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2)]:
  - @effect-agent/engine@0.1.0-beta.34
  - @effect-agent/core@0.1.0-beta.34
  - @effect-agent/capabilities@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.33
  - @effect-agent/core@0.1.0-beta.33
  - @effect-agent/engine@0.1.0-beta.33

## 0.1.0-beta.32

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.32
  - @effect-agent/core@0.1.0-beta.32
  - @effect-agent/engine@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- Updated dependencies [[`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188)]:
  - @effect-agent/capabilities@0.1.0-beta.31
  - @effect-agent/core@0.1.0-beta.31
  - @effect-agent/engine@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- Updated dependencies [[`34d05cd`](https://github.com/danieljvdm/effect-agent/commit/34d05cd1ce06f57f890b18b5ba1bce8af85db3e3)]:
  - @effect-agent/capabilities@0.1.0-beta.30
  - @effect-agent/core@0.1.0-beta.30
  - @effect-agent/engine@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.29
  - @effect-agent/core@0.1.0-beta.29
  - @effect-agent/engine@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies [[`374771d`](https://github.com/danieljvdm/effect-agent/commit/374771d90afa26ce7e1832f76715aa7b9eea3741)]:
  - @effect-agent/engine@0.1.0-beta.28
  - @effect-agent/capabilities@0.1.0-beta.28
  - @effect-agent/core@0.1.0-beta.28

## 0.1.0-beta.27

### Patch Changes

- Updated dependencies [[`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b), [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1)]:
  - @effect-agent/capabilities@0.1.0-beta.27
  - @effect-agent/core@0.1.0-beta.27
  - @effect-agent/engine@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.26
  - @effect-agent/core@0.1.0-beta.26
  - @effect-agent/engine@0.1.0-beta.26

## 0.1.0-beta.25

### Patch Changes

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/engine@0.1.0-beta.25
  - @effect-agent/capabilities@0.1.0-beta.25
  - @effect-agent/core@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.24
  - @effect-agent/core@0.1.0-beta.24
  - @effect-agent/engine@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.23
  - @effect-agent/core@0.1.0-beta.23
  - @effect-agent/engine@0.1.0-beta.23

## 0.1.0-beta.22

### Patch Changes

- Updated dependencies [[`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588)]:
  - @effect-agent/capabilities@0.1.0-beta.22
  - @effect-agent/core@0.1.0-beta.22
  - @effect-agent/engine@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies [[`27618dc`](https://github.com/danieljvdm/effect-agent/commit/27618dc03b0703fc784dc7abc4280fc74bb95045)]:
  - @effect-agent/capabilities@0.1.0-beta.21
  - @effect-agent/core@0.1.0-beta.21
  - @effect-agent/engine@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/core@0.1.0-beta.20
  - @effect-agent/engine@0.1.0-beta.20
  - @effect-agent/capabilities@0.1.0-beta.20

## 0.1.0-beta.19

### Patch Changes

- Updated dependencies [[`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26)]:
  - @effect-agent/engine@0.1.0-beta.19
  - @effect-agent/capabilities@0.1.0-beta.19
  - @effect-agent/core@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies [[`f36fd40`](https://github.com/danieljvdm/effect-agent/commit/f36fd409f8a34e13c87646fd857a4060ac89e89d)]:
  - @effect-agent/engine@0.1.0-beta.18
  - @effect-agent/capabilities@0.1.0-beta.18
  - @effect-agent/core@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies [[`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf)]:
  - @effect-agent/core@0.1.0-beta.17
  - @effect-agent/engine@0.1.0-beta.17
  - @effect-agent/capabilities@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.16
  - @effect-agent/core@0.1.0-beta.16
  - @effect-agent/engine@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.15
  - @effect-agent/core@0.1.0-beta.15
  - @effect-agent/engine@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.14
  - @effect-agent/core@0.1.0-beta.14
  - @effect-agent/engine@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13
  - @effect-agent/capabilities@0.1.0-beta.13
  - @effect-agent/engine@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.12
  - @effect-agent/core@0.1.0-beta.12
  - @effect-agent/engine@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.11
  - @effect-agent/core@0.1.0-beta.11
  - @effect-agent/engine@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.10
  - @effect-agent/core@0.1.0-beta.10
  - @effect-agent/engine@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9
  - @effect-agent/engine@0.1.0-beta.9
  - @effect-agent/capabilities@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.8
  - @effect-agent/core@0.1.0-beta.8
  - @effect-agent/engine@0.1.0-beta.8

## 0.1.0-beta.7

### Patch Changes

- Updated dependencies [[`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7), [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a), [`b44ed77`](https://github.com/danieljvdm/effect-agent/commit/b44ed7771c3e1ace2516507b0b54d11e662f036c), [`3a44b5f`](https://github.com/danieljvdm/effect-agent/commit/3a44b5f6595f4070abb61c79d5b756a9f7ed20af)]:
  - @effect-agent/engine@0.1.0-beta.7
  - @effect-agent/capabilities@0.1.0-beta.7
  - @effect-agent/core@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- [#30](https://github.com/danieljvdm/effect-agent/pull/30) [`94c169a`](https://github.com/danieljvdm/effect-agent/commit/94c169a44a248972158ca955e33fb02dd5e55463) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Export privacy-safe canonical Tool spans and bounded terminal logs from the engine, including
  model-declared and programmatic broker calls, value-level failures, and delayed terminal event/
  trace commit, while isolating complete span-lifecycle defects through Effect's error reporter.
  Build Cloudflare Conversation Objects on `effect-cf`'s native `DurableObject.make` boundary so it
  owns the cached runtime, event-scoped Layers, native RPC methods, `waitUntil`, and post-RPC OTLP
  flush isolation. Upgrade to `effect-cf` 0.25.3 so the same upstream boundary flushes alarm
  telemetry. Remove Effect Agent's duplicate telemetry service, flush coordinator, timeout
  configuration, and lifecycle fixture matrix.
- Updated dependencies [[`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8), [`94c169a`](https://github.com/danieljvdm/effect-agent/commit/94c169a44a248972158ca955e33fb02dd5e55463)]:
  - @effect-agent/core@0.1.0-beta.6
  - @effect-agent/engine@0.1.0-beta.6
  - @effect-agent/capabilities@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5
  - @effect-agent/engine@0.0.1-beta.5
  - @effect-agent/capabilities@0.0.1-beta.5

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.
- Updated dependencies [[`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b)]:
  - @effect-agent/core@0.0.1-beta.4
  - @effect-agent/engine@0.0.1-beta.4
  - @effect-agent/capabilities@0.0.1-beta.4
