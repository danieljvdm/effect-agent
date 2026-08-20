# Example: customized pull-request reviewer

This example shows how a repository configures [`@effect-agent/pr-review`](../../packages/pr-review).
The reviewer, including its Subagent fan-out variant, started here before moving into the
package. Only consumer configuration remains.

## Shape

- **Customization.** `src/reviewer.ts` calls `PrReview.make` with mission-aware `guidance`, one
  extra `readonly` Effect AI Tool
  (`read_review_conventions`) whose handler Layer the caller provides, and
  lockfiles removed from review with `ignore` globs. None of these changes
  affect the package's fail-closed publication path. Anchor validation, the
  findings bound, and publication-after-settlement are unconditional.
- **Host script.** `src/cli.ts` demonstrates the "rung 2" consumer shape. Target
  resolution, GitHub adapters, and the OpenAI client all come from the
  package; only the customization is local.
- **Offline test.** `test/example-reviewer.test.ts` runs the customized reviewer end-to-end
  against the package's fixture source, collecting publisher,
  and a prompt-keyed scripted model. It needs no network or credentials and exercises every
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
