# Example: customized pull-request reviewer

A consumer of [`@effect-agent/pr-review`](../../packages/pr-review) showing
the adaptation path: this example was the original home of the reviewer
(including the subagent fan-out variant) before it was promoted into that
package, and now demonstrates what a downstream repository writes —
configuration, not framework code.

## Shape

- **Customization** (`src/reviewer.ts`): `PrReview.make` with three
  adaptations — mission-aware `guidance`, one extra `readonly` Effect AI Tool
  (`read_review_conventions`) whose handler Layer the caller provides, and
  lockfiles removed from the review surface with `ignore` globs. None of them
  touch the package's fail-closed publication path: anchor validation, the
  findings bound, and publication-after-settlement are unconditional.
- **Host script** (`src/cli.ts`): the "rung 2" consumer shape — target
  resolution, GitHub adapters, and the OpenAI client all come from the
  package; only the customization is local.
- **Offline test** (`test/example-reviewer.test.ts`): the customized reviewer
  runs end-to-end against the package's fixture source, collecting publisher,
  and a prompt-keyed scripted model — no network, no credentials, every
  ordinary gate.

## CLI

```sh
bun src/cli.ts --repo owner/name --pr 123            # dry run: prints the plan
bun src/cli.ts --repo owner/name --pr 123 --post     # posts the review
```

Environment: `OPENAI_API_KEY` (required for the model), `GITHUB_TOKEN`
(required to post; optional for public-repository reads),
`GITHUB_REPOSITORY` / `GITHUB_EVENT_PATH` / `GITHUB_API_URL` (provided by
GitHub Actions).

## Honest limits

Deployment class `E` (ephemeral): one `AgentRuntime.run` per invocation, no
durability claim, no exactly-once anything. The repository's own PR-review
workflow uses the prebuilt [`action/`](../../action) bundle, not this
example.
