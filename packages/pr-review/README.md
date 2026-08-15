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
the review body with the reason named, never trusted), the findings bound is
enforced host-side, and publication happens only through the
`ReviewPublisher` port after the run settles. `PrReview.makeFanOut` builds
the delegating variant — bounded per-unit child reviewers (S1 attached
ephemeral delegation) merged under the same output contract and the same
publication path; the shared `guidance` and `maxFindings` shape the
coordinator's merge as well as the children.

## What a posted review looks like

The body opens with a host-derived callout tier — `[!CAUTION]` when any
finding is blocking, `[!IMPORTANT]` for important findings, an ℹ️ blockquote
for nits, `✅` for a clean approval — computed from the validated severities,
never from model prose. Below the summary, non-anchored `concerns` (deletion
plans, rollout sequencing, coverage gaps, scope questions — things with no
diff line to point at) render as severity-tagged sections. The footer names
the model binding, the observed token usage, and links to the workflow run;
an invisible metadata comment pins the reviewed head commit so later readers
know when line callouts have gone stale.

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

## Incremental Action reviews

A completely covered posted review carries bounded, versioned review state:
the exact PR/base/head lineage, profile and accepted-scope fingerprints, and
the still-unresolved findings and concerns. A later Action run validates the
state and reviews the GitHub comparison from that reviewed head to the
current head, not the complete base...HEAD diff. Unchanged accepted scope is
not sent back to the model; unchanged unresolved findings remain active;
changed or reverted paths invalidate their prior findings. Non-anchored
concerns are carried conservatively until a full audit because they cannot be
mapped safely to one path.

State lookup, schema, identity, ancestry, profile, and comparison checks are
fail-closed for scope selection: missing, stale, incompatible, or truncated
state/comparisons produce a visible full-diff fallback. An ancestor base
advance remains incremental and adds overlapping PR paths as affected
context; a materially changed base lineage falls back to full. Re-running the
same covered head skips model execution by default while preserving its
stored blocking/success conclusion.

`review-mode: final` is the explicit bounded merge-readiness audit. It reviews
the full current PR diff and resets the incremental baseline; normal
`synchronize` events use `incremental` and do not perform this audit.

## Hosts

- **GitHub Actions**: the repository ships a prebuilt node-runtime action
  supporting a committed review-profile document via `guidance-file` (this
  repository's own profile lives at `.github/review-guidance.md`)
  (`action/` at the repo root) — `uses` it with an API-key secret and nothing
  else. For custom reviewers in CI, `@effect-agent/pr-review/action` exports
  `runReviewAction` (event resolution, typed draft/non-PR skips, durable range
  selection, step outputs, and conservative check gate) to harness your own
  `reviewer.run`.
- **CLI**: `bun src/cli.ts --repo owner/name --pr 123 [--post] [--provider anthropic] [--fan-out]`
  (also exported as the `./cli` entry).

Environment: `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` for the model,
`GITHUB_TOKEN` to post (optional for public-repository reads), and the
standard `GITHUB_REPOSITORY` / `GITHUB_EVENT_PATH` / `GITHUB_API_URL`
variables inside Actions.

## Bounds, spelled out

Finite `AgentPolicy` on every definition plus run-level `UsageBudgetLimits`
(tokens, tool calls, cost, duration). Reading a file head version beyond 200k
characters is refused typed. The changeset surface is bounded at 300 files:
files beyond the bound are not fetched, and the review body reports
`Reviewed N of M changed files` instead of claiming completeness. Fan-out
capacity overflow is reported in the review summary, never dropped. Any
blocking active finding fails the Action check. Any required-file coverage
gap — undiffable/unassigned paths, failed units (including policy exhaustion),
truncation, or coordinator/run failure — is non-success rather than green.
