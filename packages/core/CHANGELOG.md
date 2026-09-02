# @effect-agent/core

## 0.1.0-beta.41

### Minor Changes

- [#284](https://github.com/danieljvdm/effect-agent/pull/284) [`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add optional namespace-owned Cloudflare memory with bounded batch recall, authoritative semantic-candidate validation, and durable conditional writes shared across Threads. Limit semantic recall output with `maxOutputBytes`, counting repeated attribution and metadata.

  BEHAVIOR CHANGE: Construct access and document scopes with `MemoryScope.make` or decode them with its Schema; Cloudflare memory clients require the existing branded `Principal`, capped at 256 characters.

  BEHAVIOR CHANGE: Replace `recallMemory` with `Memory.recall` for multi-source composition, or use `client.recall(candidates, limits)` for a bound Cloudflare memory client. The old function is removed without an alias.

- [#281](https://github.com/danieljvdm/effect-agent/pull/281) [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Derive delegation schemas and mappings from child definitions, inherit omitted policy defaults within shared reservation limits, and accept model Layers directly for subagent execution and durable registration.

  BEHAVIOR CHANGE: Durable delegations enforce shared reservation caps; configure identical `parentCaps` when multiple delegation policies share a parent Run.

## 0.1.0-beta.40

### Minor Changes

- [#270](https://github.com/danieljvdm/effect-agent/pull/270) [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add optional conditional memory updates, idempotent write receipts, and terminal withdrawal with SQLite persistence. Revalidate cached passages against the current source revision and host-selected access scope before recall.

- [#273](https://github.com/danieljvdm/effect-agent/pull/273) [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add optional native Effect embedding retrieval with bounded deterministic chunks and authoritative source validation. Provide a disposable exact cosine index with fenced replacement and withdrawal.

- [#269](https://github.com/danieljvdm/effect-agent/pull/269) [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose bounded, attributed Markdown or external corpus passages as optional transient model context. Keep recalled text outside Thread history and compaction coverage, and reload it when durable execution resumes.

### Patch Changes

- [#269](https://github.com/danieljvdm/effect-agent/pull/269) [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Qualify recalled source identities by private authority and render only result-local authority labels. BEHAVIOR CHANGE: direct readers must set the same `MemoryPassage.authority` to deduplicate across reader declarations.

- [#278](https://github.com/danieljvdm/effect-agent/pull/278) [`0fbcbbf`](https://github.com/danieljvdm/effect-agent/commit/0fbcbbf3c8c2ca7595543e545baddb0c6f965436) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Define memory namespaces with branded Schema identities and retain their types through reads, writes, and semantic indexing. Use one canonical address for document, receipt, and index isolation.

  BEHAVIOR CHANGE: Replace raw namespace strings with `MemoryNamespace.define(...).make(...)`, use `.Wire` Schemas at heterogeneous transport boundaries, and reset incompatible development memory and prepared processor data.

## 0.1.0-beta.39

### Minor Changes

- [#251](https://github.com/danieljvdm/effect-agent/pull/251) [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add Effectful `inputPrompt` projections to Agent Definitions so hosts can omit input fields from model requests while retaining canonical input for authorization and recovery.

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

- [#257](https://github.com/danieljvdm/effect-agent/pull/257) [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Construct agents with `Agent.make` and execute Definitions with native model Layers through `Effect.provide`. Accept Schema-encoded inputs in `run`, `stream`, and `start`, and decode external data through `runUnknown`, `streamUnknown`, and `startUnknown`.

  BEHAVIOR CHANGE: Replace `Agent.define` with `Agent.make`; move inputs typed as `unknown` to the explicit unknown-input operations.

### Patch Changes

- [#243](https://github.com/danieljvdm/effect-agent/pull/243) [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve Run limits across durable recovery, require explicit delegation replay authority, and reject unusable compaction summaries. Authorize settlement waits and aborts through the runtime authorizer and reject settlement Receipts whose Submission belongs to another Thread.

  BEHAVIOR CHANGE: Reset private-development histories whose RunStarted records predate policy accounting version 1 before resuming them.

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

## 0.1.0-beta.38

## 0.1.0-beta.37

### Patch Changes

- [#199](https://github.com/danieljvdm/effect-agent/pull/199) [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow agents to require a native completion Tool, rejecting ordinary final text instead of parsing it as structured output. Permit a sole completion Tool on the final allowed turn when no further model call is needed.

## 0.1.0-beta.36

## 0.1.0-beta.35

## 0.1.0-beta.34

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

## 0.1.0-beta.33

## 0.1.0-beta.32

## 0.1.0-beta.31

## 0.1.0-beta.30

## 0.1.0-beta.29

## 0.1.0-beta.28

## 0.1.0-beta.27

### Minor Changes

- [#155](https://github.com/danieljvdm/effect-agent/pull/155) [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add crash-safe terminal delivery Tools and final model responses, completion-capacity reservation, Run-scoped prompt provenance, and target-aware compaction.
  Persist priced per-call model usage in the DN and DC assemblies and expose aggregate usage on Run settlements.

## 0.1.0-beta.26

## 0.1.0-beta.25

## 0.1.0-beta.24

## 0.1.0-beta.23

## 0.1.0-beta.22

### Minor Changes

- [#124](https://github.com/danieljvdm/effect-agent/pull/124) [`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Authorize every still-executable model-declared call in a fresh or resumed application Tool batch
  through a host-supplied Run option before durable preparation or Handler execution. Settle denied
  accepted work with a typed failure while preserving canonical Run, Turn, input, and Tool Call
  identity across recovery.

## 0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

## 0.1.0-beta.19

## 0.1.0-beta.18

## 0.1.0-beta.17

### Minor Changes

- [#101](https://github.com/danieljvdm/effect-agent/pull/101) [`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a Definition-owned Schema boundary for typed application run dispositions and persist valid
  ordinary-completion values on canonical `SubmissionSettled` records across crash recovery and
  replay, including materialization through the public durable Settlement API.

## 0.1.0-beta.16

## 0.1.0-beta.15

## 0.1.0-beta.14

## 0.1.0-beta.13

### Minor Changes

- [#86](https://github.com/danieljvdm/effect-agent/pull/86) [`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Typed budget dimension on durable settlements (RUN-011, [#83](https://github.com/danieljvdm/effect-agent/issues/83)): the canonical `SubmissionSettled`
  record additively persists `exhausted` (`"tokens" | "tool-calls" | "turns"`) beside
  `finishReason: "budget-exhausted"` for a completed soft landing, and `policyLimit` (the typed
  `AgentPolicyError.limit`) beside the bounded `{errorTag, message}` failure projection for a
  `failed` hard-rail settlement — consumers read the dimension typed instead of parsing message
  text. Decode is family-bound fail-closed (`exhausted` only with the budget-exhausted
  finishReason, `policyLimit` only on a failed outcome) and histories persisted before the
  metadata existed keep decoding with it absent (schemaVersion 1 unchanged).
  `@effect-agent/core` now exports the `ExhaustedLimit` and `PolicyLimit` literal schemas backing
  the fields.

## 0.1.0-beta.12

## 0.1.0-beta.11

## 0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- [#66](https://github.com/danieljvdm/effect-agent/pull/66) [`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Harden context economics per the reviewer's second pass: the cost budget is
  enforced even when a token breach soft-lands the same response; tool results
  that cannot serialize become a bounded `UnserializableToolResult` sentinel
  instead of passing through unbounded; recovery re-seeds spend and derives
  token-exhaustion state (fail mode rejects an already-over-budget resume before
  any model call); compaction summarizer usage is staged into the canonical
  turn record; staged usage is validated as non-negative finite integers; and a
  provider-only breaching stop settles honestly as budget-exhausted.

## 0.1.0-beta.8

## 0.1.0-beta.7

### Minor Changes

- [#54](https://github.com/danieljvdm/effect-agent/pull/54) [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Context economics ([#54](https://github.com/danieljvdm/effect-agent/issues/54), RUN-022–027/CAP-017): application tool results are bounded by default (50 KiB
  `TruncatedToolResult` envelopes), budget accounting becomes cache-aware with last-call
  live-context tracking, every request can carry a derived run-status message, the token
  dimension joins the `onExhaustion` soft landing (RUN-018) with the `exhausted` dimension marker,
  and the engine compacts natively at the pre-Turn seam (prune, then one metered summarize)
  with a canonical `CompactionCreated` record that projections fold across Runs; provider
  context-length rejections compact-and-retry once, then fail typed (`ContextOverflowError`).

## 0.1.0-beta.6

### Minor Changes

- [#39](https://github.com/danieljvdm/effect-agent/pull/39) [`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Budget soft landing (D-037, ADR-0019, RUN-018/019/020): `AgentPolicy` gains
  `onExhaustion: "final-answer" | "fail"`, defaulting to `"final-answer"` — Turn and Tool Call
  exhaustion now settle the Run through one constrained final-answer opportunity instead of failing
  it. An over-budget Tool batch settles synthetically as model-visible failed results (no handler
  starts, no durable batch declaration, exempt from repeated-failure folding), subsequent model
  requests carry `toolChoice: "none"`, Turn exhaustion admits exactly one grace Turn, and the Run
  completes with the honest `finishReason: "budget-exhausted"` on the live event, the reduced
  `AgentResult`, and (additively) the durable `SubmissionSettled` record. Duration, token, cost, and
  repeated-failure bounds stay hard rails; `onExhaustion: "fail"` preserves the prior run-fatal
  behavior exactly. BEHAVIOR CHANGE ON UPGRADE: Turn/Tool-Call budget deaths become honest
  completions unless a policy pins `"fail"` — `@effect-agent/pr-review` pins `"fail"` pending its
  containment rework.

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.

## 0.0.1-beta.3

### Patch Changes

- Adopt the MIT license across every published package, and ship the Cloudflare
  packages with type declarations for the first time: their Durable Object
  class factory now carries an explicit `ConversationObjectClass` return type,
  which unblocks TypeScript declaration emit (TS4094). Supersedes the
  0.0.1-beta.2 round (and the Cloudflare pair's 0.0.1-beta.0), which was
  published out of band from an uncommitted tree, still UNLICENSED, and without
  `.d.mts` for the Cloudflare packages.

## 0.0.1-beta.1

### Patch Changes

- Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
  artifacts depended on internal `@effect-agent/*` versions that were never
  published (`workspace:*` ranges were resolved from a stale lockfile at
  publish time); the release script now pins internal ranges to the exact
  workspace versions itself.

## 0.0.1-beta.0

### Patch Changes

- Initial beta-channel release of the Effect Agent framework packages for live
  integration testing: the schema-first authoring core, the ephemeral
  interpreter, operational capabilities, sandbox contracts and the local
  adapter, canonical session records with the durable coordinator, the memory
  and SQLite storage adapters, the Node platform assembly, and the
  deterministic testing kit. The Cloudflare packages stay private until their
  declaration-emit blocker (TS4094) is resolved.
