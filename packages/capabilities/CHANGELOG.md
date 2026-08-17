# @effect-agent/capabilities

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.16
  - @effect-agent/engine@0.1.0-beta.16
  - @effect-agent/sandbox@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.15
  - @effect-agent/engine@0.1.0-beta.15
  - @effect-agent/sandbox@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.14
  - @effect-agent/engine@0.1.0-beta.14
  - @effect-agent/sandbox@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13
  - @effect-agent/engine@0.1.0-beta.13
  - @effect-agent/sandbox@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.12
  - @effect-agent/engine@0.1.0-beta.12
  - @effect-agent/sandbox@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.11
  - @effect-agent/engine@0.1.0-beta.11
  - @effect-agent/sandbox@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.10
  - @effect-agent/engine@0.1.0-beta.10
  - @effect-agent/sandbox@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9
  - @effect-agent/engine@0.1.0-beta.9
  - @effect-agent/sandbox@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.8
  - @effect-agent/engine@0.1.0-beta.8
  - @effect-agent/sandbox@0.1.0-beta.8

## 0.1.0-beta.7

### Minor Changes

- [#56](https://github.com/danieljvdm/effect-agent/pull/56) [`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Budget extension (D-037, ADR-0019 S3, RUN-021/SUB-034): `RunOptions` gains tightening-only
  `toolCallAllowance` and `turnAllowance` — the effective limit is
  `min(policy bound, max(1, floor(allowance)))`, never wider, and the `onExhaustion` soft landing
  keys off the effective limits. `Subagent.define` gains
  `toolCallAllowance: { default, fromParameters }`, clamped fail-closed to the delegation's
  per-invocation `SubagentPolicy.maxToolCalls` slice and threaded into ephemeral child runs, so an
  orchestrator model grants a scout more budget by re-delegating with a raised allowance (fresh
  child; never a mid-flight top-up). `projectResult` now receives a bounded
  `SubagentResultContext` whose `budgetExhausted` marker is honest on both paths — from the
  ephemeral child result's `finishReason`, or from the child Settlement's durable marker carried
  through the new optional `ChildEstablishSettled.finishReason` (threaded by the session
  coordinator shared by the DN and DC assemblies; exercised in the DN-profile durable-subagent
  suites) — so a budget-truncated partial can be surfaced in the declared success Schema. Existing one-argument `projectResult` functions keep
  compiling unchanged. Also hardens S2 containment per its autoreviewer findings: `Subagent.define`
  is overloaded so the Tool channels follow the `failureMode` value; genuine engine signals are
  classified by unspoofable provenance instead of `instanceof` on exported classes; each delegation
  exposes its canonical `containedFailure` schema (pr-review's coverage decoder now derives from
  it); and the pr-review child reviewer deliberately returns to typed exhaustion — a review is a
  coverage claim, so a budget-exhausted unit stays honestly unreviewed (contained as result data,
  never run-fatal).

- [#54](https://github.com/danieljvdm/effect-agent/pull/54) [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Context economics ([#54](https://github.com/danieljvdm/effect-agent/issues/54), RUN-022–027/CAP-017): application tool results are bounded by default (50 KiB
  `TruncatedToolResult` envelopes), budget accounting becomes cache-aware with last-call
  live-context tracking, every request can carry a derived run-status message, the token
  dimension joins the `onExhaustion` soft landing (RUN-018) with the `exhausted` dimension marker,
  and the engine compacts natively at the pre-Turn seam (prune, then one metered summarize)
  with a canonical `CompactionCreated` record that projections fold across Runs; provider
  context-length rejections compact-and-retry once, then fail typed (`ContextOverflowError`).

- [#50](https://github.com/danieljvdm/effect-agent/pull/50) [`b44ed77`](https://github.com/danieljvdm/effect-agent/commit/b44ed7771c3e1ace2516507b0b54d11e662f036c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Delegation containment (D-037, ADR-0019 S2, SUB-033): `Subagent.define` gains
  `failureMode: "error" | "return"` (default `"error"`, today's semantics). Under `"return"` every
  expected delegation failure — the declared child failure plus `SubagentPrestartDenied`,
  `SubagentBudgetExhausted`, `SubagentProjectionFailure`, and `SubagentExecutionFailure` — becomes
  model-visible result data in the Tool success union instead of failing the parent Run, so one
  dead child cannot detonate a fan-out. The engine signals (`ToolCallWaiting`,
  `SubagentDurabilityError`) always stay in the error channel, preserving durable suspension by
  construction, and the durable settlement join records the contained failure with the same
  non-failure polarity the live batch continues with. pr-review retires its same-name shadow-Tool
  workaround for the first-party option, adopts the S1 `final-answer` soft landing in all three
  default reviewer policies (an exhausted child or coordinator now returns a partial review instead
  of "unit unreviewed: AgentPolicyError"), and reverts the fan-out `repeatedFailureLimit` sizing
  hack. Contained unit failures reach coverage classification with richer tags
  (`FileReviewUnitFailed:<childErrorTag>`).

### Patch Changes

- Updated dependencies [[`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7), [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a), [`3a44b5f`](https://github.com/danieljvdm/effect-agent/commit/3a44b5f6595f4070abb61c79d5b756a9f7ed20af)]:
  - @effect-agent/engine@0.1.0-beta.7
  - @effect-agent/core@0.1.0-beta.7
  - @effect-agent/sandbox@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- Updated dependencies [[`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8), [`94c169a`](https://github.com/danieljvdm/effect-agent/commit/94c169a44a248972158ca955e33fb02dd5e55463)]:
  - @effect-agent/core@0.1.0-beta.6
  - @effect-agent/engine@0.1.0-beta.6
  - @effect-agent/sandbox@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5
  - @effect-agent/engine@0.0.1-beta.5

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
  - @effect-agent/core@0.0.1-beta.3
  - @effect-agent/engine@0.0.1-beta.3

## 0.0.1-beta.1

### Patch Changes

- Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
  artifacts depended on internal `@effect-agent/*` versions that were never
  published (`workspace:*` ranges were resolved from a stale lockfile at
  publish time); the release script now pins internal ranges to the exact
  workspace versions itself.
- Updated dependencies []:
  - @effect-agent/core@0.0.1-beta.1
  - @effect-agent/engine@0.0.1-beta.1

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
  - @effect-agent/core@0.0.1-beta.0
  - @effect-agent/engine@0.0.1-beta.0
