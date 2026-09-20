# @effect-agent/workflow

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

- Updated dependencies [[`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d), [`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d)]:
  - @effect-agent/core@0.1.0-beta.85
  - @effect-agent/thread@0.1.0-beta.85

## 0.1.0-beta.84

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.84
  - @effect-agent/thread@0.1.0-beta.84

## 0.1.0-beta.83

### Patch Changes

- Updated dependencies [[`d349fa1`](https://github.com/danieljvdm/effect-agent/commit/d349fa181bb3ecc88823aeef4ae9075a12d21f1f)]:
  - @effect-agent/thread@0.1.0-beta.83
  - @effect-agent/core@0.1.0-beta.83

## 0.1.0-beta.82

### Patch Changes

- Updated dependencies [[`c7fb67c`](https://github.com/danieljvdm/effect-agent/commit/c7fb67c0607f9a7e7a31e4f90ba3b20c7e6079aa)]:
  - @effect-agent/thread@0.1.0-beta.82
  - @effect-agent/core@0.1.0-beta.82

## 0.1.0-beta.81

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.81
  - @effect-agent/thread@0.1.0-beta.81

## 0.1.0-beta.80

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.80
  - @effect-agent/thread@0.1.0-beta.80

## 0.1.0-beta.79

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.79
  - @effect-agent/thread@0.1.0-beta.79

## 0.1.0-beta.78

### Patch Changes

- Updated dependencies [[`301ead3`](https://github.com/danieljvdm/effect-agent/commit/301ead3c88dbd5b6fc40e31e53f11745235d3977), [`84fe655`](https://github.com/danieljvdm/effect-agent/commit/84fe65580c35a91707eda809b7e47d90402179a9), [`c8163c1`](https://github.com/danieljvdm/effect-agent/commit/c8163c14194b256653db98821c58c493bdebe21a)]:
  - @effect-agent/thread@0.1.0-beta.78
  - @effect-agent/core@0.1.0-beta.78

## 0.1.0-beta.77

### Patch Changes

- Updated dependencies [[`84d6684`](https://github.com/danieljvdm/effect-agent/commit/84d66844e24e7fbdc5dc3f54a5d3a7a6127cdd99)]:
  - @effect-agent/core@0.1.0-beta.77
  - @effect-agent/thread@0.1.0-beta.77

## 0.1.0-beta.76

### Patch Changes

- Updated dependencies [[`3eef297`](https://github.com/danieljvdm/effect-agent/commit/3eef297d2343989a830d5d2b88e0b863b54c91fd)]:
  - @effect-agent/core@0.1.0-beta.76
  - @effect-agent/thread@0.1.0-beta.76

## 0.1.0-beta.75

### Patch Changes

- Updated dependencies [[`71afa3d`](https://github.com/danieljvdm/effect-agent/commit/71afa3d64f1cef889b46bea6a352d4e6f8446e32)]:
  - @effect-agent/core@0.1.0-beta.75
  - @effect-agent/thread@0.1.0-beta.75

## 0.1.0-beta.74

### Patch Changes

- Updated dependencies [[`cf10ec3`](https://github.com/danieljvdm/effect-agent/commit/cf10ec32e2d94402d417b05358bf96715e8c5401)]:
  - @effect-agent/core@0.1.0-beta.74
  - @effect-agent/thread@0.1.0-beta.74

## 0.1.0-beta.73

### Patch Changes

- Updated dependencies [[`da6971d`](https://github.com/danieljvdm/effect-agent/commit/da6971d450c7ed73b88c8ae74ac8376aee6c1254)]:
  - @effect-agent/core@0.1.0-beta.73
  - @effect-agent/thread@0.1.0-beta.73

## 0.1.0-beta.72

### Patch Changes

- Updated dependencies [[`08571ea`](https://github.com/danieljvdm/effect-agent/commit/08571eacf1483fbc0008106e6753138ad75eb011)]:
  - @effect-agent/core@0.1.0-beta.72
  - @effect-agent/thread@0.1.0-beta.72

## 0.1.0-beta.71

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.71
  - @effect-agent/thread@0.1.0-beta.71

## 0.1.0-beta.70

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.70
  - @effect-agent/thread@0.1.0-beta.70

## 0.1.0-beta.69

### Patch Changes

- Updated dependencies [[`e37a126`](https://github.com/danieljvdm/effect-agent/commit/e37a12613f25225c3ae8544dc384f4f7da4adc03), [`f497de2`](https://github.com/danieljvdm/effect-agent/commit/f497de24ad24ba12b2eebf29f4473b4b89f90158), [`9c98161`](https://github.com/danieljvdm/effect-agent/commit/9c98161d5a1026f2dc3d0fb395ea4a0bf6323fdd)]:
  - @effect-agent/core@0.1.0-beta.69
  - @effect-agent/thread@0.1.0-beta.69

## 0.1.0-beta.68

### Patch Changes

- Updated dependencies [[`4ca6361`](https://github.com/danieljvdm/effect-agent/commit/4ca6361c2085b5b77d1835c2b61ca1e67d2f8e6c)]:
  - @effect-agent/thread@0.1.0-beta.68
  - @effect-agent/core@0.1.0-beta.68

## 0.1.0-beta.67

### Patch Changes

- Updated dependencies [[`8c0fe3b`](https://github.com/danieljvdm/effect-agent/commit/8c0fe3bf4f5a2ff84bd3ae6a44abd18b89f6bc1f)]:
  - @effect-agent/thread@0.1.0-beta.67
  - @effect-agent/core@0.1.0-beta.67

## 0.1.0-beta.66

### Patch Changes

- Updated dependencies [[`05f105d`](https://github.com/danieljvdm/effect-agent/commit/05f105dba7ee6ea5d605ef41bb39db913dc08254)]:
  - @effect-agent/thread@0.1.0-beta.66
  - @effect-agent/core@0.1.0-beta.66

## 0.1.0-beta.65

### Patch Changes

- Updated dependencies [[`4d8a33a`](https://github.com/danieljvdm/effect-agent/commit/4d8a33a82ebae221bcb62e9cec4d53a0e76f6bd8)]:
  - @effect-agent/thread@0.1.0-beta.65
  - @effect-agent/core@0.1.0-beta.65

## 0.1.0-beta.64

### Patch Changes

- Updated dependencies [[`620d7d3`](https://github.com/danieljvdm/effect-agent/commit/620d7d38dd94b95c29d2e07a79b445a6fbccd648), [`a5bcce2`](https://github.com/danieljvdm/effect-agent/commit/a5bcce2bcb8683735284b24dd026391237cf70d9)]:
  - @effect-agent/core@0.1.0-beta.64
  - @effect-agent/thread@0.1.0-beta.64

## 0.1.0-beta.63

### Patch Changes

- Updated dependencies [[`d0f36bf`](https://github.com/danieljvdm/effect-agent/commit/d0f36bfc21e821fcc34caacf3c39f1e904a5d1c9)]:
  - @effect-agent/thread@0.1.0-beta.63
  - @effect-agent/core@0.1.0-beta.63

## 0.1.0-beta.62

### Patch Changes

- Updated dependencies [[`46e8ad2`](https://github.com/danieljvdm/effect-agent/commit/46e8ad22fa9f436ce155a6696ddf8e11cec2931c)]:
  - @effect-agent/thread@0.1.0-beta.62
  - @effect-agent/core@0.1.0-beta.62

## 0.1.0-beta.61

### Patch Changes

- Updated dependencies [[`21431ae`](https://github.com/danieljvdm/effect-agent/commit/21431ae6cacd78e6330b1017c2768f4f9c347b7a)]:
  - @effect-agent/core@0.1.0-beta.61
  - @effect-agent/thread@0.1.0-beta.61

## 0.1.0-beta.60

### Patch Changes

- Updated dependencies [[`bed4e71`](https://github.com/danieljvdm/effect-agent/commit/bed4e7170ab2d9b2ef3fbf3c7c3a8fa16e0d803d)]:
  - @effect-agent/thread@0.1.0-beta.60
  - @effect-agent/core@0.1.0-beta.60

## 0.1.0-beta.59

### Patch Changes

- Updated dependencies [[`cb1d297`](https://github.com/danieljvdm/effect-agent/commit/cb1d297d3464850b5e4645a0d3b3a5062a1ba71b)]:
  - @effect-agent/core@0.1.0-beta.59
  - @effect-agent/thread@0.1.0-beta.59

## 0.1.0-beta.58

### Patch Changes

- Updated dependencies [[`daca525`](https://github.com/danieljvdm/effect-agent/commit/daca52585983bb90b6c43a29e4a44a28c8de1743)]:
  - @effect-agent/thread@0.1.0-beta.58
  - @effect-agent/core@0.1.0-beta.58

## 0.1.0-beta.57

### Patch Changes

- Updated dependencies [[`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81), [`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81)]:
  - @effect-agent/core@0.1.0-beta.57
  - @effect-agent/thread@0.1.0-beta.57

## 0.1.0-beta.56

### Patch Changes

- Updated dependencies []:
  - @effect-agent/thread@0.1.0-beta.56
  - @effect-agent/core@0.1.0-beta.56

## 0.1.0-beta.55

### Patch Changes

- Updated dependencies [[`2259fc0`](https://github.com/danieljvdm/effect-agent/commit/2259fc05eec3bfac2a92a8d055953f3482e54735)]:
  - @effect-agent/thread@0.1.0-beta.55
  - @effect-agent/core@0.1.0-beta.55

## 0.1.0-beta.54

### Patch Changes

- Updated dependencies [[`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e), [`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e)]:
  - @effect-agent/core@0.1.0-beta.54
  - @effect-agent/thread@0.1.0-beta.54

## 0.1.0-beta.53

### Patch Changes

- Updated dependencies [[`d93903e`](https://github.com/danieljvdm/effect-agent/commit/d93903ec923da7a9841b5ab1a72bba5c0a0fb34b), [`6b4839f`](https://github.com/danieljvdm/effect-agent/commit/6b4839f6ab14adcf82c72159152ab5fe2a946f97)]:
  - @effect-agent/thread@0.1.0-beta.53
  - @effect-agent/core@0.1.0-beta.53

## 0.1.0-beta.52

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.52
  - @effect-agent/thread@0.1.0-beta.52

## 0.1.0-beta.51

### Patch Changes

- [#341](https://github.com/danieljvdm/effect-agent/pull/341) [`75898ae`](https://github.com/danieljvdm/effect-agent/commit/75898aef60b09945d90bfe5674b5153edb0717eb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add revisioned subscription management, bounded event retention, and explicit recovery of parked admissions. Fence fresh destination admission by host policy and retain one unsettled submission per optional admission group until canonical settlement.

  BEHAVIOR CHANGE: Reset incompatible development storage and update custom stores for required configuration revisions and retry generations.

- Updated dependencies [[`75898ae`](https://github.com/danieljvdm/effect-agent/commit/75898aef60b09945d90bfe5674b5153edb0717eb)]:
  - @effect-agent/thread@0.1.0-beta.51
  - @effect-agent/core@0.1.0-beta.51

## 0.1.0-beta.50

### Patch Changes

- Updated dependencies [[`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf), [`0438a7b`](https://github.com/danieljvdm/effect-agent/commit/0438a7b9c58869a91870d3df44dc163ec790a929), [`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf)]:
  - @effect-agent/thread@0.1.0-beta.50
  - @effect-agent/core@0.1.0-beta.50

## 0.1.0-beta.49

### Patch Changes

- Updated dependencies [[`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f)]:
  - @effect-agent/thread@0.1.0-beta.49
  - @effect-agent/core@0.1.0-beta.49

## 0.1.0-beta.48

### Patch Changes

- Updated dependencies [[`e640747`](https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22), [`8899bdb`](https://github.com/danieljvdm/effect-agent/commit/8899bdbcbbd16c5b7f9981564939f64729b73015)]:
  - @effect-agent/thread@0.1.0-beta.48
  - @effect-agent/core@0.1.0-beta.48

## 0.1.0-beta.47

### Patch Changes

- Updated dependencies [[`e6ff3bc`](https://github.com/danieljvdm/effect-agent/commit/e6ff3bcd1b5ce0f2348de668853482ba9d5e126b)]:
  - @effect-agent/core@0.1.0-beta.47
  - @effect-agent/thread@0.1.0-beta.47

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

- [#315](https://github.com/danieljvdm/effect-agent/pull/315) [`cebe728`](https://github.com/danieljvdm/effect-agent/commit/cebe728685cf9f45c1d9579273222a865bb8109d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run registered agents inside native Effect Workflow handlers with `AgentWorkflow.execute` and suspend until their typed results are available. **BEHAVIOR CHANGE:** supply `principal` to `WorkflowAgentHost.layer`; custom dispatch stores must return the retained intent from `put`, preserve and atomically attach its completion token, and compare the full intent before removal.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2), [`cebe728`](https://github.com/danieljvdm/effect-agent/commit/cebe728685cf9f45c1d9579273222a865bb8109d)]:
  - @effect-agent/core@0.1.0-beta.46
  - @effect-agent/thread@0.1.0-beta.46

## 0.1.0-beta.45

### Minor Changes

- [#309](https://github.com/danieljvdm/effect-agent/pull/309) [`c8812c2`](https://github.com/danieljvdm/effect-agent/commit/c8812c221004bfbeded7a56a03f13102e282f4e0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run existing registered agents through an application-supplied Effect Workflow engine with durable dispatch repair. Add SQLite dispatch storage, a scoped Node repair trigger, and bounded durable processing with attempt-scoped ownership release.

  BEHAVIOR CHANGE: Replace `NodeDurableRuntime` and its `Options`, `Config`, `ConfigValue`, `Services`, and `InitializationError` exports with the corresponding `NodeDurableAgentRuntime` names.

  BEHAVIOR CHANGE: Register agents with `DurableAgentRuntime.layerRegistered` or `NodeDurableAgentRuntime.layerRegistered`, and use `layerWithBindings` for precompiled bindings. Call `processThreadResolved(threadId)` and run the `runResolvedWorker` Effect without binding arguments; use `NodeDurableHost.layer` and `ThreadMaintenance.layer` as Layer values over an already-assembled runtime.

  BEHAVIOR CHANGE: Supply `WakeScheduler`, `ToolReconciler`, and `DurableRuntimeFailpoint` when calling `runChaosPlan`, which constructs registered runtimes for its delegation fixtures.

### Patch Changes

- Updated dependencies [[`c8812c2`](https://github.com/danieljvdm/effect-agent/commit/c8812c221004bfbeded7a56a03f13102e282f4e0)]:
  - @effect-agent/thread@0.1.0-beta.45
  - @effect-agent/core@0.1.0-beta.45
