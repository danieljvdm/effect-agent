# @effect-agent/pr-review

A bounded, fail-closed GitHub pull-request reviewer built on the effect-agent
public surface. One read-only Agent reviews a pull request through typed
ports; the host validates every finding anchor against the real diff and
posts one review after the run settles.

**Deployment class E (ephemeral).** One `AgentRuntime.run` per invocation, no
durability claim, and review posting is never exactly-once: a failed or
truncated run posts nothing.

## Use

```ts
import { Effect, Layer } from "effect";
import {
  PrReview,
  gitHubReviewLayers,
  resolveReviewTarget,
  makeOpenAiReviewModel,
  openAiClientLayer,
} from "@effect-agent/pr-review";

const reviewer = PrReview.make({ model: makeOpenAiReviewModel() });

const program = Effect.gen(function* () {
  const target = yield* resolveReviewTarget({ repository: "acme/api", number: 123 });
  return yield* reviewer
    .run({ post: true })
    .pipe(Effect.provide(Layer.merge(gitHubReviewLayers(target), openAiClientLayer)));
});
```

The run's requirement channel keeps every real dependency visible: the
`PullRequestSource` and `ReviewPublisher` ports, the provider client, and the
handler Layer of any extra tool you add. Anthropic is equally supported
(`makeAnthropicReviewModel`, `anthropicClientLayer`), and the factory accepts
any Effect AI Model.

## Adapt

```ts
const reviewer = PrReview.make({
  model: makeAnthropicReviewModel("claude-sonnet-5"),
  guidance: (mission) => [
    "This is an Effect codebase. Flag naked Promises in public APIs.",
    mission.changedFileCount > 50 ? "Large PR: prioritize breadth over nit depth." : "",
  ],
  ignore: ["**/*.lock", "dist/**"],
  maxFindings: 10,
  policy: { maxTurns: 20, maxToolCalls: 40, maxDuration: "10 minutes", toolConcurrency: 2 },
  extraTools: [MyReadonlyTool], // must be annotated ToolExecutionClass "readonly"
});
```

Every knob widens what goes INTO the review. What leaves it is not
configurable: model output is untrusted input, so finding anchors are
re-validated against the parsed unified diff (invalid ones are demoted into
the review body, never trusted), the findings bound is enforced host-side,
and publication happens only through the `ReviewPublisher` port after the run
settles. `PrReview.makeFanOut` builds the delegating variant — bounded
per-unit child reviewers (S1 attached ephemeral delegation) merged under the
same output contract and the same publication path.

## Swap a port

Tools observe the pull request only through `PullRequestSource`; publication
happens only through `ReviewPublisher`. Provide your own Layers to review
anything diff-shaped or publish anywhere else — the GitHub REST adapters are
one implementation, not the contract.

## Test what you adapted

```ts
import {
  fixturePullRequestSourceLayer,
  collectingReviewPublisherLayer,
  makeOfflineReviewerModel,
  makePromptKeyedModel,
} from "@effect-agent/pr-review/testing";
```

Deterministic in-memory adapters for both ports plus prompt-keyed scripted
models that walk the real tool surface — no network, no credentials, every
ordinary gate.

## Skip unchanged changesets

Repositories that auto-merge the base branch into open pull requests fire
`synchronize` on every base update — but the effective diff usually hasn't
changed. Every posted review embeds an invisible changeset fingerprint
(SHA-256 over the ignore-filtered changeset plus the prompt signature), and
the action skips typed when the current fingerprint matches the last posted
review (`skip-unchanged` input, default `true`; `--skip-unchanged` on the
CLI). Real changes, conflict-resolution merges, and configuration changes
(guidance, ignore globs, bounds) produce a new fingerprint and review again.
The check reads prior reviews through the `PriorReviews` port and fails open:
a lookup fault reviews instead of skipping.

## Hosts

- **GitHub Actions**: the repository ships a prebuilt node-runtime action
  (`action/` at the repo root) — `uses` it with an API-key secret and nothing
  else. For custom reviewers in CI, `@effect-agent/pr-review/action` exports
  `runReviewAction` (event resolution, typed draft/non-PR skips, step
  outputs, verdict gate) to harness your own `reviewer.run`.
- **CLI**: `bun src/cli.ts --repo owner/name --pr 123 [--post] [--provider anthropic] [--fan-out]`
  (also exported as the `./cli` entry).

Environment: `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` for the model,
`GITHUB_TOKEN` to post (optional for public-repository reads), and the
standard `GITHUB_REPOSITORY` / `GITHUB_EVENT_PATH` / `GITHUB_API_URL`
variables inside Actions.

## Bounds, spelled out

Finite `AgentPolicy` on every definition plus run-level `UsageBudgetLimits`
(tokens, tool calls, cost, duration). Pull requests beyond 300 changed files
or 200k-character files are refused typed, never silently truncated. Fan-out
capacity overflow is reported in the review summary, never dropped.
