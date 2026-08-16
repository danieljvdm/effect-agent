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

GitHub may omit the `patch` field for large textual files as well as binary
files. The GitHub source recovers a missing patch by reading bounded, strict
UTF-8 base/head content: additions require the head, deletions require the
base, and other changes require both. Reviewers receive that content through
the ordinary diff-read tool with non-anchorable `B`/`H` line labels and must
report defects as review-body concerns. Invalid UTF-8, binary NUL content,
missing sides, files beyond the per-side read bound, and complete B/H evidence
beyond the model-facing render bound remain explicit coverage gaps.

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

A completely covered posted Action review carries bounded, versioned,
HMAC-authenticated continuity state:
the exact PR/base/head lineage, profile and accepted-scope fingerprints, and
the still-unresolved findings and concerns. A later Action run validates the
state and reviews the GitHub comparison from that reviewed head to the
current head, not the complete base...HEAD diff. Unchanged accepted scope is
not sent back to the model; unchanged unresolved findings remain active;
changed or reverted paths invalidate their prior findings. Non-anchored
concerns are carried conservatively until a full audit because they cannot be
mapped safely to one path.

The state marker must be terminal, signed with the configured stable
`PR_REVIEW_STATE_SECRET`, authored by the configured review-posting bot, and
pinned to the reviewed commit. The expected author defaults to
`github-actions[bot]`; set `PR_REVIEW_AUTHOR_LOGIN` (or the Action's
`review-author` input) to `<app-slug>[bot]` when posting with a custom GitHub
App token. State lookup, authentication, schema, identity, ancestry, profile,
and comparison checks are fail-closed for scope selection: missing, stale,
incompatible, or truncated state/comparisons produce a visible full-diff
fallback. An ancestor base advance remains incremental and adds overlapping
PR paths as affected context; a materially changed base lineage falls back to
full. Re-running the same covered head skips model execution by default while
preserving its stored blocking/success conclusion.

After a new state-bearing Action review posts, prior marker-bearing bot reviews
are retired by default: their bodies become collapsed, superseded history,
resolved findings are struck through, and matching inline comments are
minimized as outdated. Only strictly older reviews from the same GitHub actor
are eligible, so copied markers and newer concurrent reviews are untouched.
The machine-state comments remain byte-identical and terminal, so an edited
body still participates in incremental state recovery.
This cosmetic pass is fail-open and can be disabled with the Action input
`retire-stale-reviews: "false"`.

Authentication is an explicit Effect service supplied by the Action host;
WebCrypto import/sign/verify failures stay typed. The terminal marker is
schema-branded and capped at 24,000 characters. If signing fails or state
exceeds that bound, the completed review is posted without continuity state
and with a bounded warning, so the next run safely performs a full review.

`review-mode: final` is the explicit bounded merge-readiness audit. It reviews
the full current PR diff and resets the incremental baseline; normal
`synchronize` events use `incremental` and do not perform this audit.

## Hosts

- **GitHub Actions**: the repository ships a prebuilt node-runtime action
  supporting a committed review-profile document via `guidance-file` (this
  repository's own profile lives at `.github/review-guidance.md`)
  (`action/` at the repo root) — `uses` it with an API-key secret and nothing
  else. While a run executes it maintains one sticky, fail-open "review in
  progress" comment updated in place with the settled outcome
  (`progress-comment` input, default on; at-least-once with generation-fenced
  writes and best-effort duplicate cleanup — strict single-comment behavior
  comes from a per-PR workflow concurrency group), and its logs render one
  compact line per event (`log-level` input, default `Info`). For custom reviewers in
  CI, `@effect-agent/pr-review/action` exports `runReviewAction` (event
  resolution, typed draft/non-PR skips, bounded range selection, step
  outputs, and conservative check gate) to harness your own `reviewer.run`;
  pass `progressComment: true` to opt a custom harness into the sticky
  progress comment.
- **CLI**: `bun src/cli.ts --repo owner/name --pr 123 [--post] [--provider anthropic] [--fan-out]`
  (also exported as the `./cli` entry).

Environment: `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` for the model,
`PR_REVIEW_STATE_SECRET` to authenticate incremental state,
`PR_REVIEW_AUTHOR_LOGIN` to match a custom review-posting bot (defaults to
`github-actions[bot]`),
`GITHUB_TOKEN` to post (optional for public-repository reads), and the
standard `GITHUB_REPOSITORY` / `GITHUB_EVENT_PATH` / `GITHUB_API_URL`
variables inside Actions.

## Bounds, spelled out

Finite `AgentPolicy` on every definition plus run-level `UsageBudgetLimits`
(tokens, tool calls, cost, duration). Reading either file version beyond 200k
bytes or characters is refused typed; patchless content is reviewable only
when its complete B/H rendering fits 220k characters. The changeset surface is
bounded at 300 files:
files beyond the bound are not fetched, and the review body reports
`Reviewed N of M changed files` instead of claiming completeness. Fan-out
capacity overflow is reported in the review summary, never dropped. Any
blocking active finding fails the Action check. Any required-file coverage
gap — paths with neither a patch nor bounded textual fallback, unassigned
paths, failed units (including policy exhaustion), truncation, or
coordinator/run failure — is non-success rather than green.
