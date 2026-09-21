# @effect-agent/sandbox-local

## 0.1.0-beta.130

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.130

## 0.1.0-beta.129

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.129

## 0.1.0-beta.128

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.128

## 0.1.0-beta.127

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.127

## 0.1.0-beta.126

### Patch Changes

- Updated dependencies [[`bf955bb`](https://github.com/danieljvdm/effect-agent/commit/bf955bbf275901e560d93cf0a054cfbf51aa9420), [`81a78cd`](https://github.com/danieljvdm/effect-agent/commit/81a78cd2bbd932b939b942eedea53c8e2894480e)]:
  - effect-agent@0.1.0-beta.126

## 0.1.0-beta.125

### Patch Changes

- Updated dependencies [[`34d7c5f`](https://github.com/danieljvdm/effect-agent/commit/34d7c5ff9392fd6fb1db348257fd22dea58a337c)]:
  - effect-agent@0.1.0-beta.125

## 0.1.0-beta.124

### Patch Changes

- Updated dependencies [[`d8bd6db`](https://github.com/danieljvdm/effect-agent/commit/d8bd6db4d21dbb0ae53132d52db7fa3fa6ef9f76)]:
  - effect-agent@0.1.0-beta.124

## 0.1.0-beta.123

### Patch Changes

- Updated dependencies [[`72e07a3`](https://github.com/danieljvdm/effect-agent/commit/72e07a35010564acb411845e250fa5d552edef0d)]:
  - effect-agent@0.1.0-beta.123

## 0.1.0-beta.122

### Patch Changes

- Updated dependencies [[`83fb830`](https://github.com/danieljvdm/effect-agent/commit/83fb83078a95a5fb60fffa0ea818dca98d4e88bd)]:
  - effect-agent@0.1.0-beta.122

## 0.1.0-beta.121

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.121

## 0.1.0-beta.120

### Patch Changes

- Updated dependencies [[`037d29a`](https://github.com/danieljvdm/effect-agent/commit/037d29a754034551520c8df9cb41bfb7660cde40)]:
  - effect-agent@0.1.0-beta.120

## 0.1.0-beta.119

### Patch Changes

- Updated dependencies [[`5c11bea`](https://github.com/danieljvdm/effect-agent/commit/5c11bea7ec185136b3453d317a0fea20f015a3a8)]:
  - effect-agent@0.1.0-beta.119

## 0.1.0-beta.118

### Patch Changes

- Updated dependencies [[`ff29420`](https://github.com/danieljvdm/effect-agent/commit/ff2942050eae59ad3ccc9731caaf809e13d957f1)]:
  - effect-agent@0.1.0-beta.118

## 0.1.0-beta.117

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.117

## 0.1.0-beta.116

### Patch Changes

- Updated dependencies [[`c29c38c`](https://github.com/danieljvdm/effect-agent/commit/c29c38cc4ebaf81c700911b83a57073005c6bdfa)]:
  - effect-agent@0.1.0-beta.116

## 0.1.0-beta.115

### Patch Changes

- Updated dependencies [[`1c33f81`](https://github.com/danieljvdm/effect-agent/commit/1c33f812e4339f1b5757d2721aa8318c8119aa51), [`d1313aa`](https://github.com/danieljvdm/effect-agent/commit/d1313aaf2a1be18b34e5ebfa680ed12f4cef31bc), [`432036c`](https://github.com/danieljvdm/effect-agent/commit/432036cedbe59e8ecbdcd4c71417b730d5b781df)]:
  - effect-agent@0.1.0-beta.115

## 0.1.0-beta.114

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.114

## 0.1.0-beta.113

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.113

## 0.1.0-beta.112

### Patch Changes

- [#558](https://github.com/danieljvdm/effect-agent/pull/558) [`6716f8c`](https://github.com/danieljvdm/effect-agent/commit/6716f8c5915fee466c89d9d82159fd8f2b67ece4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect rc.116 and replace the local decision and TypeSafe APIs with native `Decision`, `DecisionModel`, and `@effect/ai-typesafe`, retaining `AutoModel` for thread selection.

  BEHAVIOR CHANGE: Import decisions from `effect/unstable/ai` and configure TypeSafe with `TypeSafeClient.layerConfig()`; AutoModel requires at least two profiles, writes version 2 selection records, and rejects version 1 records without reselection or mutation. Retain the previous runtime for active version 1 threads or explicitly upgrade their records in your storage adapter; native probability sums must be within `1e-6` of 1.

- Updated dependencies [[`ab5030d`](https://github.com/danieljvdm/effect-agent/commit/ab5030d9814a5c47f6facfdf89fe5799bdba6b00), [`6716f8c`](https://github.com/danieljvdm/effect-agent/commit/6716f8c5915fee466c89d9d82159fd8f2b67ece4)]:
  - effect-agent@0.1.0-beta.112

## 0.1.0-beta.111

### Patch Changes

- Updated dependencies [[`b2cf08c`](https://github.com/danieljvdm/effect-agent/commit/b2cf08c14d3c455990724fb30062bdd5544dcabb)]:
  - effect-agent@0.1.0-beta.111

## 0.1.0-beta.110

### Patch Changes

- Updated dependencies [[`c2ae9e7`](https://github.com/danieljvdm/effect-agent/commit/c2ae9e777766fba0e14e8a472bc833d2122c2b10), [`a1957c4`](https://github.com/danieljvdm/effect-agent/commit/a1957c457777e7f8eeb7b51ab8833f41593c3ecf), [`2582969`](https://github.com/danieljvdm/effect-agent/commit/25829699c09a4cc862b650e4e30e5edc0fbb4fc0)]:
  - effect-agent@0.1.0-beta.110

## 0.1.0-beta.109

### Patch Changes

- Updated dependencies [[`cdbe786`](https://github.com/danieljvdm/effect-agent/commit/cdbe786861e9ba10ecb1dccf3b26f47170a8245e)]:
  - effect-agent@0.1.0-beta.109

## 0.1.0-beta.108

### Patch Changes

- Updated dependencies [[`92bd9e2`](https://github.com/danieljvdm/effect-agent/commit/92bd9e26c181c07f84371a372d8885cd4db4667a)]:
  - effect-agent@0.1.0-beta.108

## 0.1.0-beta.107

### Patch Changes

- Updated dependencies [[`cfb6e1e`](https://github.com/danieljvdm/effect-agent/commit/cfb6e1e04b9e80d276f918f29c369cddec5b917c)]:
  - effect-agent@0.1.0-beta.107

## 0.1.0-beta.106

### Patch Changes

- Updated dependencies [[`992d062`](https://github.com/danieljvdm/effect-agent/commit/992d062a095995bd8f328a01cc784b6a9a7ffc72)]:
  - effect-agent@0.1.0-beta.106

## 0.1.0-beta.105

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.105

## 0.1.0-beta.104

### Patch Changes

- Updated dependencies [[`caf7e7e`](https://github.com/danieljvdm/effect-agent/commit/caf7e7ea69448fb820f9e95cffe480cbb458d500)]:
  - effect-agent@0.1.0-beta.104

## 0.1.0-beta.103

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.103

## 0.1.0-beta.102

### Patch Changes

- Updated dependencies [[`be0dcaf`](https://github.com/danieljvdm/effect-agent/commit/be0dcafb69e0641d8b82ff174fee53a53e367f18)]:
  - effect-agent@0.1.0-beta.102

## 0.1.0-beta.101

### Patch Changes

- Updated dependencies [[`6a4f4f8`](https://github.com/danieljvdm/effect-agent/commit/6a4f4f870fe87ebb0d3cc76905dcadd77c9a29ef)]:
  - effect-agent@0.1.0-beta.101

## 0.1.0-beta.100

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.100

## 0.1.0-beta.99

### Patch Changes

- Updated dependencies [[`e1f06bb`](https://github.com/danieljvdm/effect-agent/commit/e1f06bbd3f66478c9223c5888696cd8c6e75fc37)]:
  - effect-agent@0.1.0-beta.99

## 0.1.0-beta.98

### Patch Changes

- Updated dependencies [[`95c962f`](https://github.com/danieljvdm/effect-agent/commit/95c962f8ee45c35f877d0bb21f82d4f6bac6759c)]:
  - effect-agent@0.1.0-beta.98

## 0.1.0-beta.97

### Patch Changes

- Updated dependencies [[`385f119`](https://github.com/danieljvdm/effect-agent/commit/385f1197eb41e8114c5daf5b6763824450095cf5)]:
  - effect-agent@0.1.0-beta.97

## 0.1.0-beta.96

### Patch Changes

- Updated dependencies [[`771498b`](https://github.com/danieljvdm/effect-agent/commit/771498b1952794b8f2f19d1e35b604937bffcc3c)]:
  - effect-agent@0.1.0-beta.96

## 0.1.0-beta.95

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.95

## 0.1.0-beta.94

### Patch Changes

- Updated dependencies [[`373d188`](https://github.com/danieljvdm/effect-agent/commit/373d18828f2fc2851614cf2612c5e71e91075c88), [`bbb709c`](https://github.com/danieljvdm/effect-agent/commit/bbb709c9beff0b8f2e6b67d05e0f8223a7cb6f93)]:
  - effect-agent@0.1.0-beta.94

## 0.1.0-beta.93

### Patch Changes

- Updated dependencies [[`319c156`](https://github.com/danieljvdm/effect-agent/commit/319c156be5a85a2d490cf79531f94591881436f8)]:
  - effect-agent@0.1.0-beta.93

## 0.1.0-beta.92

### Patch Changes

- [#487](https://github.com/danieljvdm/effect-agent/pull/487) [`054b1c3`](https://github.com/danieljvdm/effect-agent/commit/054b1c3a7e7a6571fc82caedc4ae8835c5aacfb4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect rc.115 across the packages and effect-cf 0.43.0 for Cloudflare hosts.

- Updated dependencies [[`054b1c3`](https://github.com/danieljvdm/effect-agent/commit/054b1c3a7e7a6571fc82caedc4ae8835c5aacfb4)]:
  - effect-agent@0.1.0-beta.92

## 0.1.0-beta.91

### Patch Changes

- Updated dependencies [[`b60b07e`](https://github.com/danieljvdm/effect-agent/commit/b60b07e307dc366637f5247fb788b24b17c554eb)]:
  - effect-agent@0.1.0-beta.91

## 0.1.0-beta.90

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.90

## 0.1.0-beta.89

### Patch Changes

- Updated dependencies [[`983a558`](https://github.com/danieljvdm/effect-agent/commit/983a558703a187285ff9c900792defc8f15984a1)]:
  - effect-agent@0.1.0-beta.89

## 0.1.0-beta.88

### Patch Changes

- Updated dependencies [[`5e24e87`](https://github.com/danieljvdm/effect-agent/commit/5e24e8782203aef836c8b4ba49e72468d7d510b1)]:
  - effect-agent@0.1.0-beta.88

## 0.1.0-beta.87

### Patch Changes

- Updated dependencies [[`0be6edf`](https://github.com/danieljvdm/effect-agent/commit/0be6edfa8c73822f59184e6177a265c56c3ac1cd)]:
  - effect-agent@0.1.0-beta.87

## 0.1.0-beta.86

### Minor Changes

- [#466](https://github.com/danieljvdm/effect-agent/pull/466) [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Consolidate agent definitions, execution, capabilities, and sandbox contracts into `effect-agent`, and use kebab-case public module paths across framework packages.

  BEHAVIOR CHANGE: Replace `@effect-agent/core`, `@effect-agent/engine`, `@effect-agent/capabilities`, and `@effect-agent/sandbox` dependencies with `effect-agent`; migrate direct imports such as `effect-agent/AgentRuntime` to `effect-agent/agent-runtime` and upgrade framework packages together.

### Patch Changes

- Updated dependencies [[`1112b1b`](https://github.com/danieljvdm/effect-agent/commit/1112b1bfb388be600c9326737d10608660698ef3), [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0), [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0), [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0)]:
  - effect-agent@0.1.0-beta.86

## 0.1.0-beta.85

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.85

## 0.1.0-beta.84

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.84

## 0.1.0-beta.83

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.83

## 0.1.0-beta.82

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.82

## 0.1.0-beta.81

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.81

## 0.1.0-beta.80

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.80

## 0.1.0-beta.79

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.79

## 0.1.0-beta.78

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.78

## 0.1.0-beta.77

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.77

## 0.1.0-beta.76

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.76

## 0.1.0-beta.75

### Patch Changes

- Updated dependencies [[`230c18a`](https://github.com/danieljvdm/effect-agent/commit/230c18a79fa3941615a6116f5678a1a3bd4b169c)]:
  - @effect-agent/sandbox@0.1.0-beta.75

## 0.1.0-beta.74

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.74

## 0.1.0-beta.73

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.73

## 0.1.0-beta.72

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.72

## 0.1.0-beta.71

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.71

## 0.1.0-beta.70

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.70

## 0.1.0-beta.69

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.69

## 0.1.0-beta.68

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.68

## 0.1.0-beta.67

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.67

## 0.1.0-beta.66

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.66

## 0.1.0-beta.65

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.65

## 0.1.0-beta.64

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.64

## 0.1.0-beta.63

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.63

## 0.1.0-beta.62

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.62

## 0.1.0-beta.61

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.61

## 0.1.0-beta.60

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.60

## 0.1.0-beta.59

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.59

## 0.1.0-beta.58

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.58

## 0.1.0-beta.57

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.57

## 0.1.0-beta.56

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.56

## 0.1.0-beta.55

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.55

## 0.1.0-beta.54

### Patch Changes

- [#353](https://github.com/danieljvdm/effect-agent/pull/353) [`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bound Cloudflare progress-wait cancellation and Quick Action response cleanup, and retain cancellation hints for late progress retries. Apply sandbox wall-time limits to configuration, process startup, and execution together.

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.54

## 0.1.0-beta.53

### Patch Changes

- Updated dependencies [[`1398ce5`](https://github.com/danieljvdm/effect-agent/commit/1398ce52ba6828a3ae17f1808c545c64b2fc566a)]:
  - @effect-agent/sandbox@0.1.0-beta.53

## 0.1.0-beta.52

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.52

## 0.1.0-beta.51

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.51

## 0.1.0-beta.50

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.50

## 0.1.0-beta.49

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.49

## 0.1.0-beta.48

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.48

## 0.1.0-beta.47

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.47

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2)]:
  - @effect-agent/sandbox@0.1.0-beta.46

## 0.1.0-beta.45

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.45

## 0.1.0-beta.44

### Patch Changes

- [#307](https://github.com/danieljvdm/effect-agent/pull/307) [`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

  BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.

- Updated dependencies [[`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41)]:
  - @effect-agent/sandbox@0.1.0-beta.44

## 0.1.0-beta.43

### Patch Changes

- Updated dependencies [[`361c643`](https://github.com/danieljvdm/effect-agent/commit/361c643bfd1ac40095bc1d63d4d84c5a0afbf3d0)]:
  - @effect-agent/sandbox@0.1.0-beta.43

## 0.1.0-beta.42

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.42

## 0.1.0-beta.41

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.41

## 0.1.0-beta.40

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

### Patch Changes

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- Updated dependencies [[`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e)]:
  - @effect-agent/sandbox@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.38

## 0.1.0-beta.37

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- Updated dependencies [[`065c455`](https://github.com/danieljvdm/effect-agent/commit/065c455d1277f73157f610429de283f41ec83d9c), [`06d4f88`](https://github.com/danieljvdm/effect-agent/commit/06d4f88c78ad175bb7e4106d53e01a2c6076ebdc)]:
  - @effect-agent/sandbox@0.1.0-beta.35

## 0.1.0-beta.34

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee)]:
  - @effect-agent/sandbox@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.33

## 0.1.0-beta.32

### Patch Changes

- Updated dependencies [[`7592ded`](https://github.com/danieljvdm/effect-agent/commit/7592deda757e0eeb0243f86bae9c2b15623e3c76)]:
  - @effect-agent/sandbox@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- Updated dependencies [[`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188)]:
  - @effect-agent/sandbox@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- Updated dependencies [[`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590)]:
  - @effect-agent/sandbox@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.28

## 0.1.0-beta.27

### Patch Changes

- Updated dependencies [[`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b)]:
  - @effect-agent/sandbox@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.26

## 0.1.0-beta.25

### Patch Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bound sandbox diagnostics and terminal artifact metadata. Reject mismatched local runtime identities and report post-start transport failures as exit failures.

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/sandbox@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.23

## 0.1.0-beta.22

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/sandbox@0.1.0-beta.20

## 0.1.0-beta.19

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.8

## 0.1.0-beta.7

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/sandbox@0.0.1-beta.5

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.
- Updated dependencies [[`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b)]:
  - @effect-agent/sandbox@0.0.1-beta.4

## 0.0.1-beta.3

### Patch Changes

- Adopt the MIT license across every published package, and ship the Cloudflare
  packages with type declarations for the first time: their Durable Object
  class factory now carries an explicit `ConversationObjectClass` return type,
  which unblocks TypeScript declaration emit (TS4094). Supersedes the
  0.0.1-beta.2 round (and the Cloudflare pair's 0.0.1-beta.0), which was
  published out of band from an uncommitted tree, still UNLICENSED, and without
  `.d.mts` for the Cloudflare packages.
- Updated dependencies []:
  - @effect-agent/sandbox@0.0.1-beta.3

## 0.0.1-beta.1

### Patch Changes

- Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
  artifacts depended on internal `@effect-agent/*` versions that were never
  published (`workspace:*` ranges were resolved from a stale lockfile at
  publish time); the release script now pins internal ranges to the exact
  workspace versions itself.
- Updated dependencies []:
  - @effect-agent/sandbox@0.0.1-beta.1

## 0.0.1-beta.0

### Patch Changes

- Initial beta-channel release of the Effect Agent framework packages for live
  integration testing: the schema-first authoring core, the ephemeral
  interpreter, operational capabilities, sandbox contracts and the local
  adapter, canonical session records with the durable coordinator, the memory
  and SQLite storage adapters, the Node platform assembly, and the
  deterministic testing kit. The Cloudflare packages stay private until their
  declaration-emit blocker (TS4094) is resolved.
- Updated dependencies []:
  - @effect-agent/sandbox@0.0.1-beta.0
