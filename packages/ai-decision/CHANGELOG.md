# @effect-agent/ai-decision

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
