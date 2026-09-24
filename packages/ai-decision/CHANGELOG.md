# @effect-agent/ai-decision

## 0.1.0-beta.141

## 0.1.0-beta.140

## 0.1.0-beta.139

## 0.1.0-beta.138

## 0.1.0-beta.137

### Minor Changes

- [#642](https://github.com/danieljvdm/effect-agent/pull/642) [`bc7eee7`](https://github.com/danieljvdm/effect-agent/commit/bc7eee7713e84c185fd953f540af2be7bfd52a44) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add `LanguageModelDecisionModel.layer` to answer native Effect decisions through any structured-output language model.

## 0.1.0-beta.136

## 0.1.0-beta.135

## 0.1.0-beta.134

### Patch Changes

- [#631](https://github.com/danieljvdm/effect-agent/pull/631) [`d210027`](https://github.com/danieljvdm/effect-agent/commit/d210027cd1103cb5a13da03e7054e504c0159e2d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect 4.0.0-rc.117 and update the model examples to GPT-6.

## 0.1.0-beta.133

## 0.1.0-beta.132

## 0.1.0-beta.131

## 0.1.0-beta.130

## 0.1.0-beta.129

## 0.1.0-beta.128

## 0.1.0-beta.127

## 0.1.0-beta.126

## 0.1.0-beta.125

## 0.1.0-beta.124

## 0.1.0-beta.123

## 0.1.0-beta.122

## 0.1.0-beta.121

## 0.1.0-beta.120

## 0.1.0-beta.119

## 0.1.0-beta.118

## 0.1.0-beta.117

## 0.1.0-beta.116

## 0.1.0-beta.115

## 0.1.0-beta.114

## 0.1.0-beta.113

## 0.1.0-beta.112

### Minor Changes

- [#558](https://github.com/danieljvdm/effect-agent/pull/558) [`6716f8c`](https://github.com/danieljvdm/effect-agent/commit/6716f8c5915fee466c89d9d82159fd8f2b67ece4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect rc.116 and replace the local decision and TypeSafe APIs with native `Decision`, `DecisionModel`, and `@effect/ai-typesafe`, retaining `AutoModel` for thread selection.

  BEHAVIOR CHANGE: Import decisions from `effect/unstable/ai` and configure TypeSafe with `TypeSafeClient.layerConfig()`; AutoModel requires at least two profiles, writes version 2 selection records, and rejects version 1 records without reselection or mutation. Retain the previous runtime for active version 1 threads or explicitly upgrade their records in your storage adapter; native probability sums must be within `1e-6` of 1.

## 0.1.0-beta.111

## 0.1.0-beta.110

## 0.1.0-beta.109

## 0.1.0-beta.108

### Minor Changes

- [#539](https://github.com/danieljvdm/effect-agent/pull/539) [`92bd9e2`](https://github.com/danieljvdm/effect-agent/commit/92bd9e26c181c07f84371a372d8885cd4db4667a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Provide AutoModel as a native Effect model Layer to select through DecisionModel automatically on each parent or subagent thread's first turn. Retain selections across follow-ups through a shared SelectionStore, with an in-memory Layer and schema-backed records for host-owned persistence.

## 0.1.0-beta.107

## 0.1.0-beta.106

## 0.1.0-beta.105

## 0.1.0-beta.104

## 0.1.0-beta.103

### Minor Changes

- [#527](https://github.com/danieljvdm/effect-agent/pull/527) [`f030933`](https://github.com/danieljvdm/effect-agent/commit/f030933c16926171436b4f92e7b920a8fbc89184) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add provider-neutral Decision Models with reusable, schema-encoded decision sets and typed choice, score, and probability queries. Supply Jev evaluations through the TypeSafe adapter with separate provider statistics and bounded support for rounded Choice probability totals in both the client and shared model.
