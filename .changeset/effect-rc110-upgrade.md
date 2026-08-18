---
"effect-agent": patch
"@effect-agent/core": patch
"@effect-agent/engine": patch
"@effect-agent/capabilities": patch
"@effect-agent/sandbox": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/session": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/pr-review": patch
"@effect-agent/testing": patch
---

Align every public package with the Effect 4.0.0-rc.110 family (`effect`,
`@effect/ai-anthropic`, `@effect/ai-openai`, `@effect/atom-react`,
`@effect/platform-browser`, `@effect/platform-node`, `@effect/sql-sqlite-do`,
`@effect/sql-sqlite-node`, `@effect/vitest`).

Two upstream changes in this range needed a matching fix here:

- `effect/unstable/cli` now fails a boolean `Flag` with `MissingOption` when
  it is omitted and has no default, instead of silently resolving to `false`.
  `pr-review`'s CLI gains explicit `Flag.withDefault(false)` on `--post`,
  `--apply-verdict`, `--fan-out`, and `--skip-unchanged` so omitting them keeps
  working exactly as before.
- `@effect/ai-anthropic`'s Claude model-capability table now defaults unknown
  models to modern capabilities (128K output, native structured outputs)
  instead of the legacy 4096-token/no-structured-output fallback, so newer
  Claude models no longer need a capability-table update to get correct
  defaults.
